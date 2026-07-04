"""
transactions_generation.py
Gerador de transações bancárias.

Domínio: banco digital com clientes pessoa física e jurídica.
Volume:  ~2 GB de Parquet particionado por ano/mes no S3.

Uso:
    python3 transactions_generation.py
"""

import random
import uuid
from datetime import date, timedelta
from pathlib import Path

import boto3
import pandas as pd

# ── Credenciais AWS ────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION            = "sa-east-1"

# ── Configuração da geração ────────────────────────────────────────────────
BUCKET         = "aula-spark-emprega-dados1"
S3_PREFIX      = "raw/transactions"       # particionado por ano= / mes=
OUTPUT_DIR     = Path("./data/transactions")
ANOS           = [2024, 2025, 2026]
LINHAS_POR_MES = 500_000                # 36 meses × 500k = 18M linhas ≈ 2 GB Parquet
SEED           = 42

# ── Domínio bancário ───────────────────────────────────────────────────────

# Tipos de transação e faixas de valor realistas para cada um
TIPOS_TRANSACAO = {
    "PIX":            (1.00,    20_000.00),
    "TED":            (500.00,  500_000.00),
    "DOC":            (100.00,  100_000.00),
    "Pagamento":      (10.00,   50_000.00),
    "Compra Débito":  (5.00,    5_000.00),
    "Compra Crédito": (5.00,    15_000.00),
    "Saque":          (20.00,   3_000.00),
    "Depósito":       (50.00,   50_000.00),
    "Tarifa":         (5.00,    150.00),
    "Estorno":        (5.00,    15_000.00),
}

CANAIS = ["App Mobile", "Internet Banking", "Caixa Eletrônico", "Agência", "API Open Finance"]

STATUS = [
    "Aprovada", "Aprovada", "Aprovada", "Aprovada", "Aprovada",  # 70% aprovadas
    "Aprovada", "Aprovada",
    "Recusada",                                                    # ~10% recusadas
    "Pendente",                                                    # ~10% pendentes
    "Estornada",                                                   # ~10% estornadas
]

MOTIVOS_RECUSA = [
    "Saldo insuficiente", "Limite excedido", "Conta bloqueada",
    "Suspeita de fraude", "Dados inválidos", None,  # None = não recusada
]

SEGMENTOS_PF = ["Varejo", "Universitário", "Premium", "Private"]
SEGMENTOS_PJ = ["MEI", "Pequena Empresa", "Média Empresa", "Corporate"]

ESTADOS = ["SP", "RJ", "MG", "RS", "PR", "BA", "SC", "GO", "PE", "CE",
           "AM", "PA", "MT", "MS", "ES", "DF", "RO", "TO", "MA", "PI"]

# ── Geração de entidades ───────────────────────────────────────────────────

def gerar_pool_contas(n: int) -> list[dict]:
    """Cria um pool fixo de contas para reuso — simula clientes reais."""
    random.seed(SEED)
    contas = []
    for _ in range(n):
        tipo = random.choice(["PF", "PF", "PF", "PJ"])  # 75% PF
        segmento = (
            random.choice(SEGMENTOS_PF) if tipo == "PF"
            else random.choice(SEGMENTOS_PJ)
        )
        contas.append({
            "conta_id":  str(uuid.uuid4())[:12].upper(),
            "tipo":      tipo,
            "segmento":  segmento,
            "estado":    random.choice(ESTADOS),
            "agencia":   f"{random.randint(1, 9999):04d}",
        })
    return contas


def gerar_data(ano: int, mes: int) -> str:
    inicio = date(ano, mes, 1)
    fim_dia = (date(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    fim = date(ano, mes, fim_dia)
    return (inicio + timedelta(days=random.randint(0, (fim - inicio).days))).isoformat()


def gerar_batch(
    contas: list[dict],
    ano: int,
    mes: int,
    n: int,
) -> pd.DataFrame:
    """Gera n transações para um determinado ano/mês como DataFrame."""
    tipos   = list(TIPOS_TRANSACAO.keys())
    registros = []

    for _ in range(n):
        tipo  = random.choice(tipos)
        vmin, vmax = TIPOS_TRANSACAO[tipo]
        valor = round(random.uniform(vmin, vmax), 2)

        origem  = random.choice(contas)
        destino = random.choice(contas)

        status = random.choice(STATUS)
        # Motivo de recusa só faz sentido quando a transação foi recusada
        motivo = random.choice(MOTIVOS_RECUSA[:-1]) if status == "Recusada" else None

        registros.append({
            "transacao_id":    str(uuid.uuid4()),
            "data":            gerar_data(ano, mes),
            "ano":             ano,
            "mes":             mes,
            "tipo":            tipo,
            "canal":           random.choice(CANAIS),
            "status":          status,
            "motivo_recusa":   motivo,
            "valor":           valor,
            # Conta origem
            "conta_origem":    origem["conta_id"],
            "tipo_origem":     origem["tipo"],
            "segmento_origem": origem["segmento"],
            "estado_origem":   origem["estado"],
            "agencia_origem":  origem["agencia"],
            # Conta destino
            "conta_destino":   destino["conta_id"],
            "tipo_destino":    destino["tipo"],
            "estado_destino":  destino["estado"],
        })

    return pd.DataFrame(registros)


# ── Upload ─────────────────────────────────────────────────────────────────

def upload_parquet(df: pd.DataFrame, bucket: str, s3_key: str) -> None:
    """Salva DataFrame como Parquet em memória e faz upload direto para o S3."""
    import io
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


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    random.seed(SEED)

    # Pool fixo de 100k contas — reutilizado em todos os meses
    print("Criando pool de contas …")
    contas = gerar_pool_contas(100_000)

    total_meses = len(ANOS) * 12
    gerado      = 0
    total_linhas = 0

    print(f"Gerando {len(ANOS) * 12 * LINHAS_POR_MES:,} transações "
          f"({len(ANOS)} anos × 12 meses × {LINHAS_POR_MES:,} linhas)\n")

    for ano in ANOS:
        for mes in range(1, 13):
            gerado += 1

            df = gerar_batch(contas, ano, mes, LINHAS_POR_MES)

            # Chave S3 seguindo convenção Hive — Spark lê a partição automaticamente
            s3_key = f"{S3_PREFIX}/ano={ano}/mes={mes:02d}/transacoes.parquet"

            upload_parquet(df, BUCKET, s3_key)

            mb = df.memory_usage(deep=True).sum() / 1_048_576
            total_linhas += len(df)
            print(f"  [{gerado:02d}/{total_meses}] ano={ano}/mes={mes:02d}  "
                  f"{len(df):,} linhas  ({mb:.0f} MB em memória)  → s3://{BUCKET}/{s3_key}")

    print(f"\nConcluído: {total_linhas:,} transações enviadas para s3://{BUCKET}/{S3_PREFIX}/")


if __name__ == "__main__":
    main()