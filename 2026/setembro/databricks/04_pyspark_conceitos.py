# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 PySpark por dentro — Lazy Evaluation, Explain, Cache e Broadcast
# MAGIC
# MAGIC Usamos `silver_patients` e `silver_encounters` para ver o que o Spark faz por baixo
# MAGIC dos panos: quando algo é só plano, quando algo de fato roda, e como otimizar joins
# MAGIC e reprocessamento.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ler as tabelas — ainda não aconteceu nada
# MAGIC
# MAGIC `spark.table()` só aponta para a origem. Não lê, não conta, não processa.

# COMMAND ----------

patients = spark.table(f"{catalog}.{schema}.silver_patients")
encounters = spark.table(f"{catalog}.{schema}.silver_encounters")

print(type(patients))  # DataFrame — zero dado carregado até aqui

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformações são preguiçosas (*lazy*)
# MAGIC
# MAGIC Pergunta: **qual o custo médio de encontro por classe de atendimento, só para
# MAGIC pacientes vivos?** Cada linha abaixo só empilha instrução num plano lógico.
# MAGIC Confira a Spark UI (aba *Jobs*) — nenhum job disparado ainda.

# COMMAND ----------

pacientes_vivos = patients.filter(F.col("death") == "alive").select("id", "gender", "race")

encontros_pacientes = (
    encounters
    .join(pacientes_vivos, encounters.patient == pacientes_vivos.id, "inner")
    .select("id", "encounterclass", "total_claim_cost", "gender", "race")
)

print(encontros_pacientes)  # ainda é só o plano, sem dado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `explain()` — o plano antes de rodar
# MAGIC
# MAGIC Mostra as 4 fases do Catalyst Optimizer: parsed → analyzed → optimized → physical.

# COMMAND ----------

encontros_pacientes.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `show()` / `count()` — as *actions* que disparam execução
# MAGIC
# MAGIC Só agora o Spark lê os arquivos Delta e roda o plano. Cada action = um job novo na
# MAGIC Spark UI.

# COMMAND ----------

encontros_pacientes.show(5)

# COMMAND ----------

encontros_pacientes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Broadcast join
# MAGIC
# MAGIC `patients` é bem menor que `encounters`. O Catalyst pode já ter escolhido sozinho um
# MAGIC `BroadcastHashJoin` no passo 3 (depende do tamanho e do limite
# MAGIC `spark.sql.autoBroadcastJoinThreshold`). Vamos forçar explicitamente e comparar o
# MAGIC plano.

# COMMAND ----------

print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))  # limite padrão: 10MB

join_forcado = encounters.join(
    F.broadcast(pacientes_vivos), encounters.patient == pacientes_vivos.id, "inner"
)
join_forcado.explain()  # a tabela pequena vai inteira pra cada executor — sem Exchange/shuffle nela

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `cache()` — evitando reprocessar o mesmo plano
# MAGIC
# MAGIC Vamos reaproveitar `encontros_pacientes` numa agregação. Sem cache, o Spark refaz
# MAGIC scan + join + filtro do zero a cada action.

# COMMAND ----------

resumo_classe = (
    encontros_pacientes.groupBy("encounterclass")
    .agg(F.avg("total_claim_cost").alias("custo_medio"), F.count("id").alias("total"))
)

resumo_classe.explain()  # plano completo de novo: scan + join + agregação

# COMMAND ----------

print(encontros_pacientes.storageLevel)  # StorageLevel(False, False, False, False, 1) — nada cacheado

encontros_pacientes.cache()
encontros_pacientes.count()  # primeira action materializa o cache

print(encontros_pacientes.storageLevel)  # agora memory/disk = True

# COMMAND ----------

resumo_classe_cached = (
    encontros_pacientes.groupBy("encounterclass")
    .agg(F.avg("total_claim_cost").alias("custo_medio"), F.count("id").alias("total"))
)

resumo_classe_cached.explain()  # repare no InMemoryTableScan no lugar do scan + join
resumo_classe_cached.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Limpando o cache
# MAGIC
# MAGIC Cache ocupa memória do cluster — sempre libere quando não precisar mais.

# COMMAND ----------

encontros_pacientes.unpersist()
print(encontros_pacientes.storageLevel)
