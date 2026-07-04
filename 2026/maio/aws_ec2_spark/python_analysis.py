"""
analise_python.py
As mesmas análises do analise_spark.py implementadas em Python puro + pandas.
Roda na sua máquina local para comparar o tempo com o PySpark no EC2.

Uso:
    python3 analise_python.py --bucket SEU-BUCKET
"""

import argparse # lê argumentos passados no terminal (--bucket, --linhas, etc.)
import io
import time

import boto3 # SDK oficial da AWS para Python — acessa S3, EC2, IAM etc.
import pandas as pd

# ── Credenciais AWS ────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION            = "sa-east-1"


# ── Utilitário de tempo ────────────────────────────────────────────────────

class Timer:
    def __init__(self, label: str):
        self.label = label

    def __enter__(self): # roda quando entra no bloco with
        self.start = time.time()
        return self

    def __exit__(self, *_): # roda automaticamente quando o bloco with termina
        print(f"  ⏱  {self.label}: {time.time() - self.start:.2f}s\n")


# ── Leitura do S3 ──────────────────────────────────────────────────────────

def listar_arquivos_s3(s3, bucket: str, prefix: str) -> list[str]:
    """Lista todos os arquivos Parquet sob o prefixo dado."""
    paginator = s3.get_paginator("list_objects_v2")
    chaves = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix): # O S3 não retorna mais de 1000 arquivos por chamada. O paginator resolve isso automaticamente
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                chaves.append(obj["Key"]) # raw/transactions/ano=2024/mes=01/transactions.parquet
    return chaves


def ler_todos_parquets(bucket: str, prefix: str) -> pd.DataFrame:
    """Baixa e concatena todos os arquivos Parquet do prefixo."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    chaves = listar_arquivos_s3(s3, bucket, prefix)
    print(f"   {len(chaves)} arquivo(s) encontrado(s)")

    frames = []
    for i, chave in enumerate(chaves, 1):
        obj = s3.get_object(Bucket=bucket, Key=chave)
        df_part = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        frames.append(df_part)
        print(f"   [{i:02d}/{len(chaves)}] {chave.split('/')[-3]}/{chave.split('/')[-2]} "
              f"— {len(df_part):,} linhas")

    return pd.concat(frames, ignore_index=True)


# ── Análises ───────────────────────────────────────────────────────────────

def main(bucket: str) -> None:
    print("\n" + "=" * 65)
    print("  Aula 2 — Transações Bancárias com Python + Pandas")
    print("=" * 65 + "\n")

    # ── 1. Leitura ───────────────────────────────────────────────────────────
    print("1. Lendo Parquet do S3 …")
    with Timer("leitura total"):
        df = ler_todos_parquets(bucket, "raw/transactions/")
        print(f"   Total de transações: {len(df):,}")
        print(f"   Memória consumida: {df.memory_usage(deep=True).sum() / 1_048_576:.0f} MB")

    # ── 2. Volume e valor por tipo de transação ──────────────────────────────
    print("2. Volume e valor por tipo de transação:")
    with Timer("groupby tipo"):
        resultado = (
            df[df["status"] == "Aprovada"]
            .groupby("tipo")["valor"] # Agrupa por tipo e seleciona só a coluna valor
            .agg(
                quantidade="count",
                volume_total="sum",
                ticket_medio="mean",
                maior_transacao="max",
            )
            .round(2)
            .sort_values("volume_total", ascending=False)
        )

        # SELECT tipo,
        #     COUNT(valor)   AS quantidade,
        #     SUM(valor)     AS volume_total,
        #     AVG(valor)     AS ticket_medio,
        #     MAX(valor)     AS maior_transacao
        # FROM df
        # WHERE status = 'Aprovada'
        # GROUP BY tipo
        print(resultado.to_string())
        print() # sozinho no final imprime uma linha em branco — só para dar espaço visual

    # ── 3. Taxa de recusa por canal ──────────────────────────────────────────
    print("3. Taxa de recusa por canal:")
    with Timer("taxa recusa"):
        total_canal   = df.groupby("canal").size().rename("total") # conta quantas linhas existem por grupo — sem precisar especificar coluna
        recusadas     = df[df["status"] == "Recusada"].groupby("canal").size().rename("recusadas")
        taxa = pd.concat([total_canal, recusadas], axis=1).fillna(0)
        taxa["taxa_recusa_pct"] = (taxa["recusadas"] / taxa["total"] * 100).round(2)
        print(taxa.sort_values("taxa_recusa_pct", ascending=False).to_string())
        print()

    # ── 4. Evolução mensal do volume financeiro ──────────────────────────────
    print("4. Evolução mensal do volume financeiro (aprovadas):")
    with Timer("evolucao mensal"):
        resultado = (
            df[df["status"] == "Aprovada"]
            .groupby(["ano", "mes"])
            .agg(transacoes=("transacao_id", "count"), volume=("valor", "sum"))
            .round(2)
        )
        print(resultado.to_string())
        print()

    # ── 5. Ranking de estados por volume (equivale à Window Function) ────────
    print("5. Top 3 estados por volume em cada ano:")
    with Timer("ranking estados"):
        vol_estado = (
            df[df["status"] == "Aprovada"]
            .groupby(["ano", "estado_origem"])["valor"]
            .sum()
            .round(2)
            .rename("volume_estado")
            .reset_index()
        )

        # ano   estado_origem   volume_estado
        # 2024  SP              8.100.000,00
        # 2024  RJ              7.800.000,00

        # rank() dentro de cada ano — equivalente ao Window.partitionBy no Spark
        vol_estado["ranking"] = ( # O .groupby("ano") aqui não agrega — ele só cria grupos temporários para o .rank()
            vol_estado.groupby("ano")["volume_estado"]
            .rank(method="min", ascending=False) # quando há empate, os dois recebem o menor ranking (1, 1, 3) em vez de (1, 2, 3).
            .astype(int)
        )

        # ano   estado_origem   volume_estado   ranking
        # 2024  SP              8.100.000,00    2
        # 2024  RJ              7.800.000,00    4
        # 2024  PE              8.187.000,00    1

        resultado = (
            vol_estado[vol_estado["ranking"] <= 3]
            .sort_values(["ano", "ranking"])
        )
        print(resultado.to_string(index=False))
        print()

    # ── 6. Detecção de padrão suspeito ───────────────────────────────────────
    print("6. Contas com mais recusas por suspeita de fraude:")
    with Timer("fraude"):
        resultado = (
            df[df["motivo_recusa"] == "Suspeita de fraude"]
            .groupby(["conta_origem", "segmento_origem", "estado_origem"])
            .size()
            .rename("ocorrencias")
            .reset_index()
        )
        resultado = resultado[resultado["ocorrencias"] >= 5].sort_values(
            "ocorrencias", ascending=False
        ).head(10)
        print(resultado.to_string(index=False))
        print()

    print("✓ Análise concluída.\n")


if __name__ == "__main__": # Só roda ao executar o arquivo diretamente, não quando importa como módulo
    parser = argparse.ArgumentParser() # Lê o terminal e transforma em variáveis
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3")
    args = parser.parse_args()
    main(args.bucket)