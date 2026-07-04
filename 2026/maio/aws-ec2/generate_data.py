"""
generate_data.py
Gerador de dados de vendas para a aula de PySpark na AWS.

Uso:
    python3 generate_data.py --bucket SEU-BUCKET
    python3 generate_data.py --bucket SEU-BUCKET --linhas-por-mes 50000
"""

import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

# ── Credenciais AWS ────────────────────────────────────────────────────────
# Preencha com as chaves geradas em IAM → Users → Security credentials
# ⚠️ Não suba este arquivo para o GitHub com as chaves preenchidas

AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION            = "sa-east-1"  # ajuste para a região do seu bucket

# ── Domínio ────────────────────────────────────────────────────────────────

REGIOES = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]

CATEGORIAS = {
    "Eletrônicos":      ["Smartphone", "Notebook", "Tablet", "Fone Bluetooth", "Smartwatch"],
    "Eletrodomésticos": ["Geladeira", "Micro-ondas", "Lavadora", "Ar-condicionado", "Liquidificador"],
    "Moda":             ["Tênis", "Camiseta", "Calça Jeans", "Jaqueta", "Bolsa"],
    "Livros":           ["Romance", "Técnico", "Infantil", "Biografias", "HQ"],
    "Alimentos":        ["Café", "Chocolate", "Whey Protein", "Azeite", "Granola"],
}

CANAIS     = ["E-commerce", "Loja Física", "Marketplace", "Televendas", "App Mobile"]
PAGAMENTOS = ["Cartão Crédito", "Cartão Débito", "PIX", "Boleto", "Carteira Digital"]
STATUS     = ["Concluído", "Concluído", "Concluído", "Cancelado", "Devolvido"]

PRECO_BASE = {
    "Eletrônicos":      (199.90, 4999.90),
    "Eletrodomésticos": (149.90, 3499.90),
    "Moda":             (49.90,  599.90),
    "Livros":           (19.90,  149.90),
    "Alimentos":        (9.90,   199.90),
}

VENDEDORES_POR_REGIAO = {r: [str(uuid.uuid4())[:8].upper() for _ in range(20)] for r in REGIOES}

CAMPOS = [
    "order_id", "data_venda", "ano", "mes", "regiao", "categoria",
    "produto", "canal", "pagamento", "status", "vendedor_id",
    "cliente_id", "quantidade", "preco_unitario", "desconto_pct",
    "desconto_valor", "valor_total",
]


# ── Geração de linhas ──────────────────────────────────────────────────────

def gerar_data(ano: int, mes: int) -> str:
    primeiro = date(ano, mes, 1)
    ultimo_dia = (date(ano, mes % 12 + 1, 1) - timedelta(days=1)).day if mes < 12 else 31
    ultimo = date(ano, mes, ultimo_dia)
    d = primeiro + timedelta(days=random.randint(0, (ultimo - primeiro).days))
    return d.isoformat()


def gerar_linha(ano: int, mes: int) -> dict:
    categoria  = random.choice(list(CATEGORIAS))
    regiao     = random.choice(REGIOES)
    preco_unit = round(random.uniform(*PRECO_BASE[categoria]), 2)
    quantidade = random.randint(1, 10)
    desconto_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
    desconto_val = round(preco_unit * quantidade * desconto_pct / 100, 2)

    return {
        "order_id":       str(uuid.uuid4()),
        "data_venda":     gerar_data(ano, mes),
        "ano":            ano,
        "mes":            mes,
        "regiao":         regiao,
        "categoria":      categoria,
        "produto":        random.choice(CATEGORIAS[categoria]),
        "canal":          random.choice(CANAIS),
        "pagamento":      random.choice(PAGAMENTOS),
        "status":         random.choice(STATUS),
        "vendedor_id":    random.choice(VENDEDORES_POR_REGIAO[regiao]),
        "cliente_id":     f"CLI{random.randint(1, 500_000):07d}",
        "quantidade":     quantidade,
        "preco_unitario": preco_unit,
        "desconto_pct":   desconto_pct,
        "desconto_valor": desconto_val,
        "valor_total":    round(preco_unit * quantidade - desconto_val, 2),
    }


# ── Geração de arquivos ────────────────────────────────────────────────────

def gerar_periodo(output_dir: Path, anos: list, linhas_por_mes: int) -> list:
    output_dir.mkdir(parents=True, exist_ok=True)
    arquivos = []
    total = len(anos) * 12

    for i, ano in enumerate(anos):
        for mes in range(1, 13):
            nome = output_dir / f"vendas_{ano}_{mes:02d}.csv"
            with open(nome, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CAMPOS)
                writer.writeheader()
                for _ in range(linhas_por_mes):
                    writer.writerow(gerar_linha(ano, mes))
            gerado = i * 12 + mes
            mb = nome.stat().st_size / 1_048_576
            print(f"  [{gerado}/{total}] {nome.name}  ({mb:.1f} MB)")
            arquivos.append(nome)

    return arquivos


# ── Upload S3 via boto3 ────────────────────────────────────────────────────

def upload_s3(arquivos: list, bucket: str, prefix: str = "raw") -> None:
    import boto3

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    print(f"\nEnviando {len(arquivos)} arquivo(s) para s3://{bucket}/{prefix}/ …")
    for arq in arquivos:
        destino = f"{prefix}/{arq.name}"
        s3.upload_file(str(arq), bucket, destino)
        print(f"  ✓ s3://{bucket}/{destino}")


# ── Configuração ────────────────────────────────────────────────────────────────────

ANOS           = [2022, 2023, 2024]
LINHAS_POR_MES = 10_000
OUTPUT_DIR     = "./data/raw"
BUCKET         = "aula-spark-emprega-dados1"
S3_PREFIX      = "raw/sales"


def main() -> None:
    random.seed(42)

    total = len(ANOS) * 12 * LINHAS_POR_MES
    print(f"Gerando {total:,} linhas  |  anos: {ANOS}  |  {LINHAS_POR_MES:,} linhas/mês")
    print(f"Saída local: {OUTPUT_DIR}\n")

    arquivos = gerar_periodo(
        output_dir=Path(OUTPUT_DIR),
        anos=ANOS,
        linhas_por_mes=LINHAS_POR_MES,
    )

    total_mb = sum(a.stat().st_size for a in arquivos) / 1_048_576
    print(f"\nGeração concluída: {len(arquivos)} arquivo(s)  |  {total_mb:.0f} MB total")

    upload_s3(arquivos, bucket=BUCKET, prefix=S3_PREFIX)
    print("\nUpload concluído.")

if __name__ == "__main__":
    main()