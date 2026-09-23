"""
AWS Glue Job — Bronze / Silver / Gold em Apache Iceberg
=========================================================
Aula: Introdução à Cloud com AWS — S3 + Glue + Iceberg.

Lê os CSVs de gestão de vulnerabilidades da camada Raw (S3), grava a camada
Bronze (ingestão crua, tipada), Silver (limpeza + padronização) e Gold
(métricas de negócio agregadas) como tabelas Iceberg no AWS Glue Data Catalog.

Este script roda DENTRO de um Glue Job (Glue 4.0+, Spark 3.3), nunca local —
ele depende de `awsglue`, que só existe no ambiente gerenciado do Glue.

Os parâmetros de catálogo Iceberg (spark.sql.catalog.glue_catalog.*) são
configurados nos "Job parameters" na criação do Job (ver README), não aqui —
assim o mesmo script funciona em qualquer bucket/ambiente sem editar código.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================================
# PARÂMETROS DO JOB (definidos na criação do Glue Job, não hardcoded aqui —
# isso é o que permite rodar o mesmo script em dev/homolog/prod só trocando
# os parâmetros, sem tocar no código)
# =========================================================================
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_PATH", "DB_BRONZE", "DB_SILVER", "DB_GOLD"],
)

CATALOGO = "glue_catalog"  # nome lógico do catálogo Iceberg, definido no job parameter --conf

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = args["RAW_PATH"].rstrip("/")
DB_BRONZE = args["DB_BRONZE"]
DB_SILVER = args["DB_SILVER"]
DB_GOLD = args["DB_GOLD"]

# Create databases on glue data catalog if not exists
for db in (DB_BRONZE, DB_SILVER, DB_GOLD):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOGO}.{db}")


def escrever_iceberg(df, catalogo, database, tabela, particoes=None):
    """CTAS simples: recria a tabela Iceberg a cada execução (didático — em
    produção, o normal seria MERGE INTO para carga incremental, comentado
    no fim do script)."""
    destino = f"{catalogo}.{database}.{tabela}"
    writer = df.writeTo(destino).using("iceberg")
    if particoes:
        writer = writer.partitionedBy(*particoes)
    writer.createOrReplace()
    print(f"[OK] {destino} — {df.count():,} linhas")


# =========================================================================
# CAMADA BRONZE — ingestão crua, só tipagem mínima + metadados de ingestão
# =========================================================================
print("=== BRONZE ===")

bronze_ativos = (
    spark.read.option("header", True).csv(f"{RAW_PATH}/ativos.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("ativos.csv"))
)
escrever_iceberg(bronze_ativos, CATALOGO, DB_BRONZE, "ativos")

bronze_cves = (
    spark.read.option("header", True).csv(f"{RAW_PATH}/vulnerabilidades_catalogo.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("vulnerabilidades_catalogo.csv"))
)
escrever_iceberg(bronze_cves, CATALOGO, DB_BRONZE, "vulnerabilidades_catalogo")

bronze_varreduras = (
    spark.read.option("header", True).csv(f"{RAW_PATH}/varreduras.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("varreduras.csv"))
)
escrever_iceberg(bronze_varreduras, CATALOGO, DB_BRONZE, "varreduras")

# =========================================================================
# CAMADA SILVER — cast, limpeza, padronização e deduplicação
# =========================================================================
print("=== SILVER ===")

silver_ativos = (
    bronze_ativos.withColumn("asset_id", F.col("asset_id").cast("long"))
    .withColumn("hostname", F.trim(F.lower("hostname")))
    .withColumn(
        "ip_address",
        F.when(F.col("ip_address").rlike(r"^\d{1,3}(\.\d{1,3}){3}$"), F.col("ip_address")).otherwise(None),
    )
    .withColumn("os_end_of_life", F.col("os_end_of_life").cast("boolean"))
    .withColumn("environment", F.lower(F.trim("environment")))
    .withColumn("criticality", F.lower(F.trim("criticality")))
    .withColumn("created_at", F.to_date("created_at", "yyyy-MM-dd"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
    .drop("_ingested_at", "_source_file")
)

# dedup: mantém o cadastro mais recente por hostname (ativo recadastrado/reinstalado)
w_host = Window.partitionBy("hostname").orderBy(F.col("created_at").desc())
silver_ativos = (
    silver_ativos.withColumn("rn", F.row_number().over(w_host))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
escrever_iceberg(silver_ativos, CATALOGO, DB_SILVER, "ativos", particoes=["environment"])

silver_cves = (
    bronze_cves.withColumn("cvss_score", F.col("cvss_score").cast("double"))
    .withColumn("published_date", F.to_date("published_date", "yyyy-MM-dd"))
    .withColumn("severity", F.upper(F.trim("severity")))
    .drop("_ingested_at", "_source_file")
)
escrever_iceberg(silver_cves, CATALOGO, DB_SILVER, "vulnerabilidades_catalogo")

silver_varreduras = (
    bronze_varreduras.withColumn("finding_id", F.col("finding_id").cast("long"))
    .withColumn("asset_id", F.col("asset_id").cast("long"))
    .withColumn("status", F.upper(F.trim(F.regexp_replace("status", "_", " "))))
    .withColumn(
        "status",
        F.when(F.col("status").rlike("ABERTO"), "OPEN")
        .when(F.col("status").rlike("CORRIGIDO"), "FIXED")
        .when(F.col("status").rlike("FALSO"), "FALSE POSITIVE")
        .when(F.col("status").rlike("RISCO"), "RISK ACCEPTED")
        .otherwise(F.col("status")),
    )
    .withColumn("scan_tool", F.initcap(F.trim("scan_tool")))
    .withColumn("first_detected", F.to_date("first_detected", "yyyy-MM-dd"))
    .withColumn("last_seen", F.to_date("last_seen", "yyyy-MM-dd"))
    .withColumn("scan_date", F.to_date("scan_date", "yyyy-MM-dd"))
    .withColumn("remediation_deadline", F.to_date("remediation_deadline", "yyyy-MM-dd"))
    .withColumn("port", F.col("port").cast("int"))
    .drop("_ingested_at", "_source_file")
)

# regra de negócio simples: flag de integridade referencial, sem descartar a linha
# (na Silver a gente sinaliza; quem decide filtrar ou não é a Gold/consumidor)
ids_ativos_validos = silver_ativos.select(F.col("asset_id").alias("_aid")).distinct()
cves_validas = silver_cves.select(F.col("cve_id").alias("_cid")).distinct()

silver_varreduras = (
    silver_varreduras.join(ids_ativos_validos, silver_varreduras.asset_id == ids_ativos_validos._aid, "left")
    .join(cves_validas, silver_varreduras.cve_id == cves_validas._cid, "left")
    .withColumn("ativo_valido", F.col("_aid").isNotNull())
    .withColumn("cve_valida", F.col("_cid").isNotNull())
    .drop("_aid", "_cid")
)

# dedup: mantém o registro mais recente por finding_id (reenvio de varredura)
w_finding = Window.partitionBy("finding_id").orderBy(F.col("last_seen").desc_nulls_last())
silver_varreduras = (
    silver_varreduras.withColumn("rn", F.row_number().over(w_finding))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
escrever_iceberg(silver_varreduras, CATALOGO, DB_SILVER, "varreduras", particoes=["status"])

# =========================================================================
# CAMADA GOLD — métricas de negócio, prontas para BI/Athena
# =========================================================================
print("=== GOLD ===")

# join broadcast: dimensões pequenas (ativos, catálogo) contra o fato (varreduras)
varreduras_enriquecidas = (
    silver_varreduras.filter(F.col("ativo_valido") & F.col("cve_valida"))
    .join(F.broadcast(silver_ativos), on="asset_id", how="inner")
    .join(F.broadcast(silver_cves), on="cve_id", how="inner")
)

# Gold 1 — exposição atual: vulnerabilidades ABERTAS por time responsável e severidade
gold_exposicao_por_time = (
    varreduras_enriquecidas.filter(F.col("status") == "OPEN")
    .groupBy("owner_team", "environment", "severity")
    .agg(
        F.count("*").alias("qtd_vulnerabilidades_abertas"),
        F.round(F.avg("cvss_score"), 2).alias("cvss_medio"),
        F.sum(F.col("os_end_of_life").cast("int")).alias("qtd_em_sistema_eol"),
    )
    .orderBy(F.desc("qtd_vulnerabilidades_abertas"))
)
escrever_iceberg(gold_exposicao_por_time, CATALOGO, DB_GOLD, "exposicao_por_time")

# Gold 2 — MTTR (mean time to remediate) por time, só para vulnerabilidades já corrigidas
gold_mttr_por_time = (
    varreduras_enriquecidas.filter(F.col("status") == "FIXED")
    .withColumn("dias_para_corrigir", F.datediff("last_seen", "first_detected"))
    .groupBy("owner_team", "severity")
    .agg(
        F.count("*").alias("qtd_corrigidas"),
        F.round(F.avg("dias_para_corrigir"), 1).alias("mttr_dias"),
    )
    .orderBy("owner_team", "severity")
)
escrever_iceberg(gold_mttr_por_time, CATALOGO, DB_GOLD, "mttr_por_time")

# Gold 3 — top ativos críticos: ranking de ativos com mais findings críticos/altos em aberto
w_rank = Window.orderBy(F.desc("qtd_alto_critico_aberto"))
gold_top_ativos_criticos = (
    varreduras_enriquecidas
    .filter((F.col("status") == "OPEN") & (F.col("severity").isin("CRITICAL", "HIGH")))
    .groupBy("asset_id", "hostname", "owner_team", "criticality")
    .agg(F.count("*").alias("qtd_alto_critico_aberto"))
    .withColumn("ranking", F.row_number().over(w_rank))
    .filter(F.col("ranking") <= 20)
)
escrever_iceberg(gold_top_ativos_criticos, CATALOGO, DB_GOLD, "top_ativos_criticos")

job.commit()

# =========================================================================
# Próximo passo sugerido para a turma (não executado aqui, fica como exercício):
# Em vez de `createOrReplace()` (que recria a tabela inteira a cada run), uma
# carga incremental de verdade usaria MERGE INTO do Iceberg, algo como:
#
# spark.sql(f"""
#   MERGE INTO {CATALOGO}.{DB_SILVER}.varreduras t
#   USING varreduras_novas s
#   ON t.finding_id = s.finding_id
#   WHEN MATCHED THEN UPDATE SET *
#   WHEN NOT MATCHED THEN INSERT *
# """)
#
# Isso evita reprocessar o histórico inteiro toda vez — só atualiza/insere o
# que mudou desde a última execução. Ótimo gancho para explicar por que
# Iceberg/Delta existem: MERGE INTO não é possível em Parquet puro sem reescrever
# arquivos inteiros.
# =========================================================================