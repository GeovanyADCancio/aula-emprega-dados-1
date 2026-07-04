"""
explore_vendas.py
Script de exploração dos dados de vendas com PySpark — Aula 1.
Roda no EC2 após o ambiente estar configurado.

Uso:
    python3 explore_vendas.py --bucket SEU-BUCKET
"""

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def criar_spark(app_name: str = "aula-spark") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")              # usa os 2 vCPUs do m7i-flex.large
        .config("spark.driver.memory", "6g")             # m7i-flex.large tem 8 GB; deixa 2g para o SO
        .config("spark.sql.shuffle.partitions", "8")     # 4x o número de vCPUs é um bom ponto de partida
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )


def cronometrar(label: str):
    """Context manager simples para medir tempo de operações."""
    class _Timer:
        def __enter__(self):
            self.start = time.time()
            return self
        def __exit__(self, *_):
            elapsed = time.time() - self.start
            print(f"  ⏱  {label}: {elapsed:.2f}s")
    return _Timer()


def main(bucket: str) -> None:
    print("\n" + "=" * 60)
    print("  Aula 1 — Explorando dados com PySpark + S3")
    print("=" * 60 + "\n")

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    s3_path = f"s3a://{bucket}/raw/sales/"

    # ── 1. Leitura ──────────────────────────────────────────────────────────
    print("1. Lendo CSVs do S3 …")
    with cronometrar("leitura"):
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(s3_path)
            .cache()   # mantém em memória para as queries seguintes
        )

    total = df.count()
    print(f"   Registros carregados: {total:,}")
    print()

    # ── 2. Schema ───────────────────────────────────────────────────────────
    print("2. Schema:")
    df.printSchema()

    # ── 3. Amostra ─────────────────────────────────────────────────────────
    print("3. Amostra (5 linhas):")
    df.show(5, truncate=False)

    # ── 4. Estatísticas básicas ─────────────────────────────────────────────
    print("4. Estatísticas numéricas:")
    with cronometrar("describe"):
        df.select("quantidade", "preco_unitario", "desconto_pct", "valor_total") \
          .describe() \
          .show()

    # ── 5. Faturamento por categoria ─────────────────────────────────────────
    print("5. Faturamento por categoria:")
    with cronometrar("groupBy categoria"):
        (
            df.filter(F.col("status") == "Concluído")
              .groupBy("categoria")
              .agg(
                  F.count("order_id").alias("pedidos"),
                  F.round(F.sum("valor_total"), 2).alias("faturamento_total"),
                  F.round(F.avg("valor_total"), 2).alias("ticket_medio"),
              )
              .orderBy(F.desc("faturamento_total"))
              .show()
        )

    # ── 6. Evolução mensal de vendas ─────────────────────────────────────────
    print("6. Evolução mensal de vendas (últimos 6 meses do dataset):")
    with cronometrar("groupBy ano/mês"):
        (
            df.filter(F.col("status") == "Concluído")
              .groupBy("ano", "mes")
              .agg(
                  F.count("order_id").alias("pedidos"),
                  F.round(F.sum("valor_total"), 2).alias("faturamento"),
              )
              .orderBy("ano", "mes")
              .show(6)
        )

    # ── 7. Top 5 regiões ────────────────────────────────────────────────────
    print("7. Top 5 regiões por faturamento:")
    with cronometrar("groupBy regiao"):
        (
            df.filter(F.col("status") == "Concluído")
              .groupBy("regiao")
              .agg(F.round(F.sum("valor_total"), 2).alias("faturamento"))
              .orderBy(F.desc("faturamento"))
              .show(5)
        )

    # ── 8. Taxa de cancelamento por canal ───────────────────────────────────
    print("8. Taxa de cancelamento por canal:")
    with cronometrar("cancelamentos"):
        total_canal = df.groupBy("canal").agg(F.count("*").alias("total"))
        cancel_canal = (
            df.filter(F.col("status") == "Cancelado")
              .groupBy("canal")
              .agg(F.count("*").alias("cancelados"))
        )
        (
            total_canal.join(cancel_canal, "canal", "left")
            .withColumn(
                "taxa_cancelamento",
                F.round(F.col("cancelados") / F.col("total") * 100, 2),
            )
            .orderBy(F.desc("taxa_cancelamento"))
            .show()
        )

    # ── 9. Escrita em Parquet ────────────────────────────────────────────────
    print("9. Escrevendo resultado em Parquet no S3 …")
    saida_path = f"s3a://{bucket}/processed/faturamento_categoria/"

    with cronometrar("write parquet"):
        (
            df.filter(F.col("status") == "Concluído")
              .groupBy("ano", "mes", "categoria", "canal", "regiao")
              .agg(
                  F.count("order_id").alias("pedidos"),
                  F.round(F.sum("valor_total"), 2).alias("faturamento"),
                  F.round(F.avg("valor_total"), 2).alias("ticket_medio"),
              )
              .write
              .mode("overwrite")
              .partitionBy("ano", "mes")
              .parquet(saida_path)
        )

    print(f"   Dados salvos em: {saida_path}")

    spark.stop()
    print("\n✓ Script finalizado.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3 (sem s3://)")
    args = parser.parse_args()
    main(args.bucket)