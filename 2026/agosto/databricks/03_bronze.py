# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Camada Gold — Tabelas analíticas
# MAGIC
# MAGIC Lê as tabelas silver e monta as três tabelas finais, iguais às do script original:
# MAGIC - `gold_obt_encounters` — One Big Table (encontros + dados do paciente);
# MAGIC - `gold_patient_summary` — resumo agregado por paciente;
# MAGIC - `gold_encounter_summary` — resumo agregado por tipo de encontro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ler tabelas silver

# COMMAND ----------

from pyspark.sql import functions as F

patients = spark.table(f"{catalog}.{schema}.silver_patients")
encounters = spark.table(f"{catalog}.{schema}.silver_encounters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One Big Table — encontros + dados do paciente
# MAGIC
# MAGIC Um `join` (equivalente ao `merge` do pandas) trazendo os dados do paciente para
# MAGIC dentro de cada linha de encontro.

# COMMAND ----------

obt = (
    encounters.alias("e")
    .join(patients.alias("p"), F.col("e.patient") == F.col("p.id"), "left")
    .select(
        F.col("e.id").alias("encounter_id"),
        F.col("e.patient").alias("patient_id"),
        F.col("e.start").alias("encounter_start_date"),
        F.col("e.stop").alias("encounter_end_date"),
        F.col("e.encounterclass"),
        F.col("e.description").alias("encounter_description"),
        F.col("e.duration_hours"),
        F.col("e.total_claim_cost"),
        F.col("e.payer_coverage"),
        F.col("p.gender"),
        F.col("p.race"),
        F.col("p.ethnicity"),
        F.col("p.full_name"),
    )
)

obt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_obt_encounters")
print(f"✅ gold_obt_encounters criada com {obt.count()} linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumo por paciente
# MAGIC
# MAGIC `groupBy` + `agg` é o equivalente direto do `.groupby().agg()` do pandas.

# COMMAND ----------

encounters_agg = (
    encounters.groupBy("patient")
    .agg(
        F.count("id").alias("total_encounters"),
        F.sum("total_claim_cost").alias("total_claim_cost"),
        F.avg("duration_hours").alias("avg_encounter_duration_hours"),
    )
)

patient_summary = (
    patients.join(encounters_agg, patients.id == encounters_agg.patient, "left")
    .drop("patient")
    .withColumnRenamed("id", "patient_id")
    .na.fill(0)
)

patient_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_patient_summary")
print(f"✅ gold_patient_summary criada com {patient_summary.count()} linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo por tipo de encontro

# COMMAND ----------

encounter_summary = (
    encounters.groupBy("encounterclass")
    .agg(
        F.count("id").alias("total_encounters"),
        F.avg("total_claim_cost").alias("avg_claim_cost"),
        F.sum("total_claim_cost").alias("sum_claim_cost"),
        F.avg("duration_hours").alias("avg_encounter_duration_hours"),
    )
)

encounter_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_encounter_summary")
print(f"✅ gold_encounter_summary criada com {encounter_summary.count()} linhas.")

print("\n🥇 Carga gold concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência rápida (opcional)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_patient_summary").limit(5))