# Databricks notebook source
# MAGIC %md
# MAGIC # 🏔️ Delta Lake — Update, Delete, Merge e Time Travel
# MAGIC
# MAGIC Lakehouse não é só "Parquet com nome bonito": tabelas Delta suportam operações
# MAGIC transacionais (ACID) que Parquet puro não suporta. Criamos uma cópia sandbox de
# MAGIC `silver_patients` e aplicamos update, delete, merge e viajamos entre versões.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

from pyspark.sql import functions as F
from delta.tables import DeltaTable

tabela = f"{catalog}.{schema}.delta_playground_patients"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sandbox — nunca faça isso direto numa tabela em produção
# MAGIC
# MAGIC Update/delete/merge em Delta reescrevem dados de verdade. Isolamos numa cópia.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {tabela} AS SELECT * FROM {catalog}.{schema}.silver_patients")

spark.sql(f"DESCRIBE HISTORY {tabela}").select("version", "timestamp", "operation").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UPDATE
# MAGIC
# MAGIC Cada UPDATE/DELETE/MERGE gera uma nova versão da tabela — os arquivos antigos
# MAGIC continuam existindo até um `VACUUM`.

# COMMAND ----------

spark.sql(f"UPDATE {tabela} SET income = income * 1.10 WHERE gender = 'F'")

spark.sql(f"DESCRIBE HISTORY {tabela}") \
    .selectExpr("version", "operation", "operationMetrics['numUpdatedRows'] as linhas_atualizadas") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DELETE
# MAGIC
# MAGIC Remove linhas sem apagar o histórico — dá pra recuperar depois (seção 5).

# COMMAND ----------

spark.sql(f"DELETE FROM {tabela} WHERE death = 'dead' AND income = 0")

spark.sql(f"DESCRIBE HISTORY {tabela}") \
    .selectExpr("version", "operation", "operationMetrics['numDeletedRows'] as linhas_deletadas") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE INTO — upsert, o coração do CDC em lakehouse
# MAGIC
# MAGIC Simulamos uma carga incremental: um paciente existente com renda atualizada e um
# MAGIC paciente novo. `MERGE` resolve update + insert numa operação atômica só.

# COMMAND ----------

algum_id = spark.table(tabela).select("id").limit(1).collect()[0]["id"]

carga_incremental = spark.createDataFrame(
    [(algum_id, 99999.0), ("paciente-novo-999", 50000.0)],
    ["id", "income"],
)

delta_tabela = DeltaTable.forName(spark, tabela)

(
    delta_tabela.alias("destino")
    .merge(carga_incremental.alias("origem"), "destino.id = origem.id")
    .whenMatchedUpdate(set={"income": "origem.income"})
    .whenNotMatchedInsert(values={"id": "origem.id", "income": "origem.income"})
    .execute()
)

spark.sql(f"DESCRIBE HISTORY {tabela}") \
    .selectExpr(
        "version", "operation",
        "operationMetrics['numTargetRowsUpdated'] as atualizadas",
        "operationMetrics['numTargetRowsInserted'] as inseridas",
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time travel — consultando versões anteriores
# MAGIC
# MAGIC A tabela guarda cada versão. Dá pra consultar por número de versão ou timestamp —
# MAGIC isso é só leitura, não muda nada.

# COMMAND ----------

spark.sql(f"SELECT count(*) AS total_v0 FROM {tabela} VERSION AS OF 0").show()
spark.sql(f"SELECT count(*) AS total_atual FROM {tabela}").show()

spark.read.format("delta").option("versionAsOf", 0).table(tabela) \
    .filter(F.col("id") == algum_id).select("id", "income").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RESTORE — voltar a tabela inteira para uma versão antiga
# MAGIC
# MAGIC Diferente do time travel de leitura, `RESTORE` sobrescreve a tabela atual — e
# MAGIC também vira uma nova versão no histórico (dá pra desfazer o restore também).

# COMMAND ----------

spark.sql(f"RESTORE TABLE {tabela} TO VERSION AS OF 0")

spark.sql(f"DESCRIBE HISTORY {tabela}").select("version", "operation").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. OPTIMIZE e VACUUM — manutenção do lakehouse
# MAGIC
# MAGIC `OPTIMIZE` compacta arquivos pequenos (`ZORDER BY` acelera filtros por essa coluna).
# MAGIC `VACUUM` apaga fisicamente arquivos órfãos além do período de retenção — depois
# MAGIC disso, time travel pra essas versões deixa de funcionar.

# COMMAND ----------

spark.sql(f"OPTIMIZE {tabela} ZORDER BY (id)")

# retenção padrão é 7 dias — só reduzir para demonstrar, nunca em produção
# spark.sql(f"VACUUM {tabela} RETAIN 0 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Limpeza (opcional)

# COMMAND ----------

# spark.sql(f"DROP TABLE {tabela}")
