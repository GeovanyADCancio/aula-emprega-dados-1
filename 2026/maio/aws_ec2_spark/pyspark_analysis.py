"""
analise_spark.py
Análise de transações bancárias com PySpark — Aula 2.

Conceitos abordados:
  - Leitura de Parquet particionado com partition pruning
  - Cache e persistência
  - Transformações lazy vs ações
  - Window functions
  - Escrita eficiente em Parquet

Uso:
    python3 analise_spark.py --bucket SEU-BUCKET
"""

import argparse
import time
from pyspark.sql import SparkSession, Window # escrita em Python, mas o Spark executa em Scala/Java
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ── SparkSession ───────────────────────────────────────────────────────────

def criar_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("aula2-transacoes-bancarias") # nome que aparece na UI do Spark
        .master("local[*]") # spark na própria máquina. [*] use todos os cores disponíveis. Poderia ser 'spark://ip-do-master:7077'
        # Memória do driver: 6g dos 8g disponíveis no m7i-flex.large
        .config("spark.driver.memory", "6g")
        # Partições de shuffle: 2× os vCPUs disponíveis
        .config("spark.sql.shuffle.partitions", "4")
        # Evita spill to disk: eleva o limite antes de serializar para disco
        .config("spark.memory.fraction", "0.8") #  80% da memória do driver é gerenciada pelo Spark
        .config("spark.memory.storageFraction", "0.3") # 30% dessa região é para cache — o resto é para execução de operações
        # Conector S3A com autenticação via IAM Role da instância
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        # Melhora leitura de Parquet: lê só as colunas necessárias
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false") # operação cara e desnecessária quando você sabe que o schema é consistente
        .getOrCreate()
    )


# ── Utilitário de tempo ────────────────────────────────────────────────────

class Timer:
    """Mede o tempo de cada bloco de análise."""
    def __init__(self, label: str):
        self.label = label

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *_):
        print(f"  ⏱  {self.label}: {time.time() - self.start:.2f}s\n")


# ── Análises ───────────────────────────────────────────────────────────────

def main(bucket: str) -> None:
    print("\n" + "=" * 65)
    print("  Aula 2 — Transações Bancárias com PySpark")
    print("=" * 65 + "\n")

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    s3_path = f"s3a://{bucket}/raw/transactions/"

    # ── 1. Leitura com partition pruning (podar) ────────────────────────────────────
    # O Spark lê apenas as pastas ano=/mes= necessárias — não varre o bucket inteiro.
    # Isso é possível porque os dados foram gravados seguindo a convenção Hive.
    print("1. Lendo Parquet particionado do S3 …")
    with Timer("leitura total"):

        # Transformações — definem o que fazer, mas não fazem nada ainda: 
        # .filter(), .groupBy(), .select(), .join(). São lazy — só constroem um plano.

        # Ações — disparam a execução de tudo que foi acumulado: .count(), .show(), .collect(), 
        # .write. Só quando uma ação é chamada o Spark executa o plano inteiro de uma vez, otimizado.

        # df = spark.read.parquet(s3_path)   # transformação — nada executou ainda
        # df.filter(...)                     # transformação — ainda nada
        # df.groupBy(...)                    # transformação — ainda nada
        # df.count()                         # AÇÃO — agora o Spark executa tudo

        df = (
            spark.read
            .option("basePath", s3_path)
            .parquet(s3_path)
        )
        total = df.count()   # primeira ação — aqui o Spark realmente executa

        print(f"   Total de transações: {total:,}")

    df.printSchema()

    # ── 2. Cache ─────────────────────────────────────────────────────────────
    # Depois do cache, o Spark não relê o S3 nas próximas ações.
    # Usamos MEMORY_AND_DISK para não estourar a RAM em datasets grandes.
    print("2. Persistindo em memória …")
    from pyspark import StorageLevel
    with Timer("cache"):
        df.persist(StorageLevel.MEMORY_AND_DISK)
        df.count()  # força a materialização do cache

    # ── 3. Volume e valor por tipo de transação ──────────────────────────────
    print("3. Volume e valor por tipo de transação:")
    with Timer("groupBy tipo"):
        (
            df.filter(F.col("status") == "Aprovada") # -> df[df["status"] == "Aprovada"]
              .groupBy("tipo")
              .agg(
                  F.count("transacao_id").alias("quantidade"),
                  F.round(F.sum("valor"), 2).alias("volume_total"),
                  F.round(F.avg("valor"), 2).alias("ticket_medio"),
                  F.round(F.max("valor"), 2).alias("maior_transacao"),
              )
              .orderBy(F.desc("volume_total"))
              .show(truncate=False) # é a ação — aqui o Spark executa tudo. truncate=False evita que valores longos sejam cortados com ... na exibição.
        )

    # ── 4. Taxa de recusa por canal ──────────────────────────────────────────
    print("4. Taxa de recusa por canal:")
    with Timer("taxa recusa"):
        total_canal = (
            df.groupBy("canal")
              .agg(F.count("*").alias("total"))
        )
        recusadas_canal = (
            df.filter(F.col("status") == "Recusada")
              .groupBy("canal")
              .agg(F.count("*").alias("recusadas"))
        )
        (
            total_canal
            .join(recusadas_canal, "canal", "left") # df['teste'] = ...
            .withColumn( # cria ou substitui uma coluna. O primeiro argumento é o nome, o segundo é a expressão. Equivalente ao df["nova_coluna"] = ... do Pandas.
                "taxa_recusa_pct",
                F.round(F.col("recusadas") / F.col("total") * 100, 2),
            )
            .orderBy(F.desc("taxa_recusa_pct"))
            .show(truncate=False)
        )

    # ── 5. Evolução mensal do volume financeiro ──────────────────────────────
    print("5. Evolução mensal do volume financeiro (aprovadas):")
    with Timer("evolucao mensal"):
        (
            df.filter(F.col("status") == "Aprovada")
              .groupBy("ano", "mes")
              .agg(
                  F.count("transacao_id").alias("transacoes"),
                  F.round(F.sum("valor"), 2).alias("volume"),
              )
              .orderBy("ano", "mes")
              .show(36, truncate=False)
        )

    # ── 6. Window function: ranking de estados por volume ───────────────────
    # Window functions permitem calcular métricas por grupo sem perder linhas.
    # Aqui ranqueamos os estados dentro de cada ano pelo volume movimentado.
    print("6. Top 3 estados por volume em cada ano (Window Function):")
    with Timer("window ranking"):
        janela = Window.partitionBy("ano").orderBy(F.desc("volume_estado"))
        (
            df.filter(F.col("status") == "Aprovada")
              .groupBy("ano", "estado_origem")
              .agg(F.round(F.sum("valor"), 2).alias("volume_estado"))
              .withColumn("ranking", F.rank().over(janela)) # diferente do groupBy que colapsa 1, 2, 3, ...
              .filter(F.col("ranking") <= 3)
              .orderBy("ano", "ranking")
              .show(truncate=False)
        )

    # ── 7. Detecção de padrão suspeito ──────────────────────────────────────
    # Contas com alto volume de transações recusadas por suspeita de fraude.
    print("7. Contas com mais recusas por suspeita de fraude:")
    with Timer("fraude"):
        (
            df.filter(F.col("motivo_recusa") == "Suspeita de fraude")
              .groupBy("conta_origem", "segmento_origem", "estado_origem")
              .agg(F.count("*").alias("ocorrencias"))
              .filter(F.col("ocorrencias") >= 5)
              .orderBy(F.desc("ocorrencias"))
              .show(10, truncate=False)
        )

    # ── 8. Escrita do resultado agregado em Parquet ──────────────────────────
    # Particionamos por ano — padrão para séries temporais em data lakes.
    # coalesce(1) evita criar dezenas de arquivos pequenos por partição.
    print("8. Salvando agregado por tipo/canal/estado em Parquet …")
    saida = f"s3a://{bucket}/processed/transacoes_agregado/"
    with Timer("write parquet"):
        (
            df.filter(F.col("status") == "Aprovada")
              .groupBy("ano", "mes", "tipo", "canal", "estado_origem", "segmento_origem")
              .agg(
                  F.count("transacao_id").alias("quantidade"),
                  F.round(F.sum("valor"), 2).alias("volume"),
                  F.round(F.avg("valor"), 2).alias("ticket_medio"),
              )
              .coalesce(4)          # evita small files — 1 arquivo por partição de shuffle
              .write
              .mode("overwrite")
              .partitionBy("ano")   # partição simples por ano no output
              .parquet(saida)
        )
    print(f"   Salvo em: {saida}")

    df.unpersist()  # libera memória do cache
    spark.stop()
    print("\n✓ Análise concluída.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="Nome do bucket S3")
    args = parser.parse_args()
    main(args.bucket)