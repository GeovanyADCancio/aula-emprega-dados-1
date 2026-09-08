# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Camada Silver — Limpeza, qualidade e transformação
# MAGIC
# MAGIC Lê as tabelas bronze, faz uma checagem simples de qualidade de dados e aplica as
# MAGIC mesmas transformações do script original (`transform_patients`, `transform_encounters`,
# MAGIC `transform_conditions`), agora usando funções do PySpark (`pyspark.sql.functions`)
# MAGIC no lugar do pandas.

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
# MAGIC ## 2. Ler tabelas bronze

# COMMAND ----------

from pyspark.sql import functions as F

patients = spark.table(f"{catalog}.{schema}.bronze_patients")
encounters = spark.table(f"{catalog}.{schema}.bronze_encounters")
conditions = spark.table(f"{catalog}.{schema}.bronze_conditions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Checagem de qualidade de dados
# MAGIC
# MAGIC Mesma ideia do `check_data_quality()` original: garante que não há `id` nulo em
# MAGIC `patients`/`encounters` e que não existem custos negativos em `encounters`.
# MAGIC Se algo falhar, o notebook para aqui (`dbutils.notebook.exit`) — assim, ao agendar
# MAGIC no Job, a Silver/Gold não roda em cima de dado ruim.

# COMMAND ----------

def checar_qualidade(df, nome_tabela, coluna_id="id"):
    print(f"\nVerificando qualidade dos dados: {nome_tabela}")

    if df.count() == 0:
        print(f"⚠️ Alerta: {nome_tabela} está vazia!")
        return False

    nulos_id = df.filter(F.col(coluna_id).isNull()).count()
    if nulos_id > 0:
        print(f"⚠️ Alerta: coluna '{coluna_id}' contém {nulos_id} valores nulos.")
        return False

    print(f"✅ {nome_tabela} OK.")
    return True

# COMMAND ----------

ok_patients = checar_qualidade(patients, "patients")
ok_encounters = checar_qualidade(encounters, "encounters")

if ok_encounters:
    custos_negativos = encounters.filter(
        (F.col("base_encounter_cost") < 0) | (F.col("total_claim_cost") < 0)
    ).count()
    if custos_negativos > 0:
        print(f"⚠️ Alerta: {custos_negativos} encontros com custo negativo.")
        ok_encounters = False

if not (ok_patients and ok_encounters):
    dbutils.notebook.exit("Processo abortado: falha na qualidade dos dados da camada bronze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformação — Patients
# MAGIC
# MAGIC - `full_name`: concatena first + middle + last, tirando espaços duplicados;
# MAGIC - `death`: "dead" se tiver `deathdate`, senão "alive";
# MAGIC - `coverage_minus_expenses` e `over_expenses`: mesma lógica do original;
# MAGIC - `income` nulo vira 0.

# COMMAND ----------

patients_silver = (
    patients
    .withColumn(
        "full_name",
        F.trim(F.regexp_replace(
            F.concat_ws(
                " ",
                F.coalesce(F.col("first"), F.lit("")),
                F.coalesce(F.col("middle"), F.lit("")),
                F.coalesce(F.col("last"), F.lit("")),
            ),
            r"\s+", " ",
        ))
    )
    .withColumn("death", F.when(F.col("deathdate").isNotNull(), "dead").otherwise("alive"))
    .withColumn(
        "coverage_minus_expenses",
        F.coalesce(F.col("healthcare_coverage"), F.lit(0.0)) - F.coalesce(F.col("healthcare_expenses"), F.lit(0.0)),
    )
    .withColumn("over_expenses", F.when(F.col("coverage_minus_expenses") < 0, 1).otherwise(0))
    .withColumn("income", F.coalesce(F.col("income"), F.lit(0.0)))
    .select(
        "id", "birthdate", "gender", "race", "ethnicity", "deathdate",
        "healthcare_expenses", "healthcare_coverage", "income",
        "full_name", "death", "coverage_minus_expenses", "over_expenses",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformação — Encounters
# MAGIC
# MAGIC Remove linhas sem `id`/`patient`, converte `start`/`stop` para timestamp e calcula
# MAGIC `duration_hours` (equivalente ao `.dt.total_seconds() / 3600` do pandas).

# COMMAND ----------

encounters_silver = (
    encounters
    .dropna(subset=["id", "patient"])
    .withColumn("start", F.to_timestamp("start"))
    .withColumn("stop", F.to_timestamp("stop"))
    .withColumn(
        "duration_hours",
        (F.col("stop").cast("long") - F.col("start").cast("long")) / 3600,
    )
    .select(
        "id", "start", "stop", "patient", "encounterclass", "description",
        "base_encounter_cost", "total_claim_cost", "payer_coverage",
        "reasondescription", "duration_hours",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transformação — Conditions
# MAGIC
# MAGIC Extrai o texto entre parênteses da descrição como `condition_type`
# MAGIC (ex.: "Diabetes (disorder)" -> condition="Diabetes", condition_type="disorder").

# COMMAND ----------

conditions_silver = (
    conditions
    .withColumn("condition", F.trim(F.regexp_replace(F.col("description"), r"\s*\(.*\)", "")))
    .withColumn("condition_type", F.regexp_extract(F.col("description"), r"\((.*?)\)", 1))
    .select("start", "stop", "patient", "description", "condition", "condition_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gravar tabelas silver

# COMMAND ----------

patients_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.silver_patients")
encounters_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.silver_encounters")
conditions_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.silver_conditions")

print("🥈 Carga silver concluída.")