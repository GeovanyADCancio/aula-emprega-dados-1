"""
transactions_generation.py
Gerador de transações bancárias para ambiente de treinamento.

Domínio : banco digital com clientes pessoa física (PF) e jurídica (PJ).
Destino : Amazon S3 — Parquet particionado por ano/mês (~2 GB total).
Volume  : 36 meses x 500.000 linhas = 18 milhões de transações.

Uso:
    python3 transactions_generation.py
"""

import io
import random
import uuid
from datetime import date, timedelta

import boto3
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Credenciais AWS
# ─────────────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION            = "sa-east-1"

# ─────────────────────────────────────────────────────────────────────────────
# Parâmetros de geração
# ─────────────────────────────────────────────────────────────────────────────
BUCKET         = "aula-spark-emprega-dados1"
S3_PREFIX      = "raw/transactions"   # Hive-partitioned: ano= / mes=
# ANOS           = [2024, 2025, 2026]
ANOS           = [2026]
LINHAS_POR_MES = 500_000              # 36 meses × 500k = 18 M linhas ≈ 2 GB
SEED           = 42

# ─────────────────────────────────────────────────────────────────────────────
# Domínio — tipos de transação com faixas de valor realistas
# ─────────────────────────────────────────────────────────────────────────────

# (valor_minimo, valor_maximo) em R$
TIPOS_TRANSACAO: dict[str, tuple[float, float]] = {
    "PIX":            (1.00,    20_000.00),
    "TED":            (500.00, 500_000.00),
    "DOC":            (100.00, 100_000.00),
    "Pagamento":      (10.00,   50_000.00),
    "Compra Débito":  (5.00,    5_000.00),
    "Compra Crédito": (5.00,   15_000.00),
    "Saque":          (20.00,   3_000.00),
    "Depósito":       (50.00,  50_000.00),
    "Tarifa":         (5.00,     150.00),
    "Estorno":        (5.00,   15_000.00),
}

CANAIS = [
    "App Mobile",
    "Internet Banking",
    "Caixa Eletrônico",
    "Agência",
    "API Open Finance",
]

# Distribuição: ~70% Aprovada, ~10% cada Recusada / Pendente / Estornada
STATUS_POOL = (
    ["Aprovada"] * 7
    + ["Recusada"]
    + ["Pendente"]
    + ["Estornada"]
)

MOTIVOS_RECUSA = [
    "Saldo insuficiente",
    "Limite excedido",
    "Conta bloqueada",
    "Suspeita de fraude",
    "Dados inválidos",
]

SEGMENTOS_PF = ["Varejo", "Universitário", "Premium", "Private"]
SEGMENTOS_PJ = ["MEI", "Pequena Empresa", "Média Empresa", "Corporate"]

# Distribuição regional aproximada à população brasileira
ESTADOS_PESO: list[tuple[str, int]] = [
    ("SP", 22), ("MG", 10), ("RJ", 9),  ("BA", 7),  ("PR", 6),
    ("RS", 5),  ("PE", 5),  ("CE", 5),  ("PA", 4),  ("SC", 4),
    ("GO", 3),  ("MA", 3),  ("AM", 2),  ("ES", 2),  ("MT", 2),
    ("MS", 2),  ("DF", 2),  ("PI", 1),  ("RO", 1),  ("TO", 1),
]
ESTADOS     = [e for e, _ in ESTADOS_PESO]
PESOS_EST   = [p for _, p in ESTADOS_PESO]


# ─────────────────────────────────────────────────────────────────────────────
# Geração de entidades
# ─────────────────────────────────────────────────────────────────────────────

def gerar_pool_contas(n: int) -> list[dict]:
    """
    Cria um pool fixo de n contas para reutilização em todos os meses.
    Simula a base de clientes real do banco.
    """
    random.seed(SEED)
    contas = []
    for _ in range(n):
        tipo = random.choices(["PF", "PJ"], weights=[75, 25])[0]
        segmento = (
            random.choice(SEGMENTOS_PF) if tipo == "PF"
            else random.choice(SEGMENTOS_PJ)
        )
        estado = random.choices(ESTADOS, weights=PESOS_EST)[0]
        contas.append({
            "conta_id":  str(uuid.uuid4())[:12].upper(),
            "tipo":      tipo,
            "segmento":  segmento,
            "estado":    estado,
            "agencia":   f"{random.randint(1, 9999):04d}",
            "ativo":     random.choices([True, False], weights=[95, 5])[0],
        })
    return contas


def _data_aleatoria(ano: int, mes: int) -> str:
    """Retorna uma data aleatória dentro do mês (ISO 8601)."""
    inicio = date(ano, mes, 1)
    if mes == 12:
        fim = date(ano, 12, 31)
    else:
        fim = date(ano, mes + 1, 1) - timedelta(days=1)
    return (inicio + timedelta(days=random.randint(0, (fim - inicio).days))).isoformat()


def gerar_batch(
    contas: list[dict],
    ano: int,
    mes: int,
    n: int,
) -> pd.DataFrame:
    """
    Gera n transações para um determinado ano/mês.
    Retorna um DataFrame pronto para serialização em Parquet.
    """
    tipos = list(TIPOS_TRANSACAO.keys())
    registros = []

    for _ in range(n):
        tipo        = random.choice(tipos)
        vmin, vmax  = TIPOS_TRANSACAO[tipo]
        valor       = round(random.uniform(vmin, vmax), 2)

        origem  = random.choice(contas)
        destino = random.choice(contas)

        status = random.choice(STATUS_POOL)
        # Motivo de recusa só existe quando a transação foi recusada
        motivo_recusa = (
            random.choice(MOTIVOS_RECUSA) if status == "Recusada" else None
        )
        # Flag de alerta de fraude: ~2% das transações
        alerta_fraude = random.random() < 0.02

        registros.append({
            # ── Identificação ──────────────────────────────────────────────
            "transacao_id":    str(uuid.uuid4()),
            "data":            _data_aleatoria(ano, mes),
            "ano":             ano,
            "mes":             mes,
            # ── Detalhes da operação ───────────────────────────────────────
            "tipo":            tipo,
            "canal":           random.choice(CANAIS),
            "status":          status,
            "motivo_recusa":   motivo_recusa,
            "valor":           valor,
            "alerta_fraude":   alerta_fraude,
            # ── Conta de origem ────────────────────────────────────────────
            "conta_origem":    origem["conta_id"],
            "tipo_origem":     origem["tipo"],
            "segmento_origem": origem["segmento"],
            "estado_origem":   origem["estado"],
            "agencia_origem":  origem["agencia"],
            # ── Conta de destino ───────────────────────────────────────────
            "conta_destino":   destino["conta_id"],
            "tipo_destino":    destino["tipo"],
            "estado_destino":  destino["estado"],
        })

    return pd.DataFrame(registros)


# ─────────────────────────────────────────────────────────────────────────────
# Upload para o S3
# ─────────────────────────────────────────────────────────────────────────────

def upload_parquet(df: pd.DataFrame, bucket: str, s3_key: str) -> None:
    """Serializa o DataFrame em Parquet (Snappy) e faz upload para o S3."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket, s3_key)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    random.seed(SEED)

    print("Criando pool de 100.000 contas …")
    contas = gerar_pool_contas(100_000)

    total_meses  = len(ANOS) * 12
    meses_feitos = 0
    total_linhas = 0

    total_esperado = len(ANOS) * 12 * LINHAS_POR_MES
    print(
        f"Gerando {total_esperado:,} transações "
        f"({len(ANOS)} anos x 12 meses x {LINHAS_POR_MES:,} linhas/mês)\n"
    )

    for ano in ANOS:
        for mes in range(1, 13):
            meses_feitos += 1

            df = gerar_batch(contas, ano, mes, LINHAS_POR_MES)

            # Convenção Hive → Spark/Athena detecta as partições automaticamente
            s3_key = f"{S3_PREFIX}/ano={ano}/mes={mes:02d}/transacoes.parquet"

            upload_parquet(df, BUCKET, s3_key)

            mb = df.memory_usage(deep=True).sum() / 1_048_576
            total_linhas += len(df)
            print(
                f"  [{meses_feitos:02d}/{total_meses}] "
                f"ano={ano}/mes={mes:02d}  "
                f"{len(df):,} linhas  ({mb:.0f} MB em memória)  "
                f"→ s3://{BUCKET}/{s3_key}"
            )

    print(
        f"\nConcluído: {total_linhas:,} transações enviadas "
        f"para s3://{BUCKET}/{S3_PREFIX}/"
    )


if __name__ == "__main__":
    main()