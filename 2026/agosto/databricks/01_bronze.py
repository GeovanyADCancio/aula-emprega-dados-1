# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Camada Bronze — Ingestão dos CSVs
# MAGIC
# MAGIC Lê os arquivos brutos (`patients.csv`, `encounters.csv`, `conditions.csv`) direto do **Volume**
# MAGIC e grava cada um como uma tabela Delta na camada **bronze**, dentro do catálogo do Unity Catalog.
# MAGIC
# MAGIC Equivalente à função `load_bronze()` do script original em pandas — aqui trocamos
# MAGIC pandas + SQLAlchemy por **PySpark + tabelas gerenciadas no catálogo**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros
# MAGIC
# MAGIC Ajuste os widgets abaixo (ou rode com os valores padrão) antes de executar o notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume_path", "/Volumes/workspace/default/dados_mentoria", "Caminho do Volume com os CSVs")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_path = dbutils.widgets.get("volume_path")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Catálogo/Schema ativo: {catalog}.{schema}")
print(f"Lendo CSVs de: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mapeamento arquivo -> tabela

# COMMAND ----------

from pyspark.sql import functions as F

arquivos = {
    "bronze_patients": "patients.csv",
    "bronze_encounters": "encounters.csv",
    "bronze_conditions": "conditions.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função de leitura + gravação
# MAGIC
# MAGIC Para cada CSV:
# MAGIC 1. lê com `spark.read.csv` (o Spark já lê o arquivo inteiro em paralelo, sem carregar tudo na memória do driver);
# MAGIC 2. deixa os nomes de coluna em minúsculo (equivalente ao `.str.lower()` do pandas);
# MAGIC 3. adiciona a coluna `execution_date`;
# MAGIC 4. grava como tabela Delta gerenciada no catálogo (`saveAsTable`).

# COMMAND ----------

def carregar_bronze(nome_tabela, nome_arquivo):
    caminho = f"{volume_path}/{nome_arquivo}"
    print(f"\nLendo '{caminho}' -> tabela '{nome_tabela}'")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(caminho)
    )

    for coluna in df.columns:
        df = df.withColumnRenamed(coluna, coluna.strip().lower())

    df = df.withColumn("execution_date", F.current_date())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{nome_tabela}")
    )

    print(f"✅ Tabela {catalog}.{schema}.{nome_tabela} criada com {df.count()} linhas.")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Executar a carga para os três arquivos

# COMMAND ----------

for tabela, arquivo in arquivos.items():
    carregar_bronze(tabela, arquivo)

print("\n🥉 Carga bronze concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência rápida (opcional)

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.bronze_patients").limit(5))