# Databricks notebook source
# MAGIC %md
# MAGIC # Aula: Camada Silver com PySpark — Rede Hospitalar
# MAGIC ### Parte 1 — Ingestão Bronze e Limpeza (cast, regex, datas, dedup)
# MAGIC
# MAGIC **Como preparar o ambiente (Databricks Free Edition):**
# MAGIC 1. Rode `gerar_dados_saude.py` na sua máquina → gera `pacientes.csv`, `medicos.csv`,
# MAGIC    `procedimentos.csv`, `atendimentos.csv` em `./dados_saude/`.
# MAGIC 2. No workspace, crie um Volume: `Catalog > workspace > default > Create Volume`
# MAGIC    (ou rode o `CREATE VOLUME` na célula abaixo).
# MAGIC 3. Faça upload dos 4 CSVs para esse Volume pela UI (`Upload to this volume`).
# MAGIC 4. Ajuste as constantes de path na célula de configuração abaixo.

# COMMAND ----------

# DBTITLE 1,Configuração (ajuste aqui)
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "dados_saude"

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SILVER_SCHEMA = f"{CATALOG}.{SCHEMA}"  # onde as tabelas Delta silver serão criadas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rode uma vez se o volume ainda não existir
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.dados_saude;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura Bronze
# MAGIC Tudo como `StringType` de propósito: em produção, a camada Bronze deve ser fiel
# MAGIC à origem. Todo o cast/validação acontece explicitamente na Silver — se deixarmos
# MAGIC o Spark inferir schema, erros de formato viram `null` silenciosamente e perdemos
# MAGIC visibilidade sobre a qualidade do dado de origem.

# COMMAND ----------

def read_bronze_csv(filename):
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(f"{BASE_PATH}/{filename}")
    )

bronze_pacientes = read_bronze_csv("pacientes.csv")
bronze_medicos = read_bronze_csv("medicos.csv")
bronze_procedimentos = read_bronze_csv("procedimentos.csv")
bronze_atendimentos = read_bronze_csv("atendimentos.csv")

for nome, df in [
    ("pacientes", bronze_pacientes),
    ("medicos", bronze_medicos),
    ("procedimentos", bronze_procedimentos),
    ("atendimentos", bronze_atendimentos),
]:
    print(f"{nome}: {df.count():,} linhas, {len(df.columns)} colunas")

# COMMAND ----------

bronze_atendimentos.printSchema()
display(bronze_atendimentos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver — `pacientes`
# MAGIC Cobre: `trim`/case, extração de dígitos via regex, validação de CPF,
# MAGIC parsing de datas com múltiplos formatos (`coalesce` de `to_date`),
# MAGIC padronização categórica, e deduplicação com `Window` (paciente recadastrado).

# COMMAND ----------

# regex: mantém só dígitos do CPF, independente de como veio formatado
pacientes_clean = bronze_pacientes.withColumn(
    "full_name", F.trim(F.initcap(F.col("full_name")))
).withColumn(
    "cpf_digits", F.regexp_replace(F.col("cpf"), r"[^0-9]", "")
).withColumn(
    "cpf_valido", F.when(F.length("cpf_digits") == 11, True).otherwise(False)
)

# datas em formatos distintos no sistema de origem: tenta cada formato até um bater.
# to_date retorna null se o formato não confere, então coalesce pega o primeiro sucesso.
pacientes_clean = pacientes_clean.withColumn(
    "birth_date",
    F.coalesce(
        F.to_date("birth_date", "yyyy-MM-dd"),
        F.to_date("birth_date", "dd/MM/yyyy"),
        F.to_date("birth_date", "MM-dd-yyyy"),
        F.to_date("birth_date", "yyyy/MM/dd"),
    ),
)

# padronização categórica: mapear variações para um domínio fechado
pacientes_clean = pacientes_clean.withColumn(
    "gender_std",
    F.when(F.upper(F.trim("gender")).isin("M", "MASCULINO"), "M")
    .when(F.upper(F.trim("gender")).isin("F", "FEMININO"), "F")
    .when(F.upper(F.trim("gender")) == "OUTRO", "OUTRO")
    .otherwise("NAO_INFORMADO"),
).withColumn(
    "address_state",
    F.when(F.upper(F.trim("address_state")).isin(
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA",
        "PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO",
    ), F.upper(F.trim("address_state"))).otherwise(None),
).withColumn(
    "is_active_std",
    F.when(F.lower(F.trim("is_active")).isin("1", "true", "s", "sim"), True)
    .when(F.lower(F.trim("is_active")).isin("0", "false", "n", "nao", "não"), False)
    .otherwise(None),
)

# regex: validação simples de e-mail
pacientes_clean = pacientes_clean.withColumn(
    "email_valido", F.col("email").rlike(r"^[\w\.\-]+@[\w\.\-]+\.\w+$")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicação com Window
# MAGIC Cenário real: mesmo paciente foi recadastrado (novo `patient_id`) por falha de
# MAGIC integração. Regra de negócio: manter o cadastro **mais recente** por CPF válido;
# MAGIC quem não tem CPF válido não pode ser deduplicado com segurança (fica como está).

# COMMAND ----------

w_dedup = Window.partitionBy("cpf_digits").orderBy(F.col("registration_date").desc())

pacientes_dedup = (
    pacientes_clean.withColumn(
        "rn",
        F.when(F.col("cpf_valido"), F.row_number().over(w_dedup)).otherwise(F.lit(1)),
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qtd_removidos = pacientes_clean.count() - pacientes_dedup.count()
print(f"Duplicatas removidas por CPF: {qtd_removidos:,}")

# COMMAND ----------

pacientes_silver = pacientes_dedup.select(
    "patient_id",
    "full_name",
    "cpf_digits",
    "cpf_valido",
    "birth_date",
    "gender_std",
    "phone",
    "email",
    "email_valido",
    "address_city",
    "address_state",
    "insurance_plan",
    "registration_date",
    "is_active_std",
).withColumnRenamed("gender_std", "gender").withColumnRenamed(
    "is_active_std", "is_active"
).withColumnRenamed("cpf_digits", "cpf")

pacientes_silver.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.pacientes_silver")
display(pacientes_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — `medicos`
# MAGIC Extração de CRM/UF via regex, padronização de departamento.

# COMMAND ----------

medicos_clean = (
    bronze_medicos.withColumn(
        "crm_numero", F.regexp_extract("crm_number", r"(\d{4,6})", 1)
    )
    .withColumn(
        "crm_uf", F.upper(F.regexp_extract("crm_number", r"([A-Za-z]{2})", 1))
    )
    .withColumn("department", F.upper(F.trim(F.regexp_replace("department", r"[\.\-]", ""))))
    .withColumn("doctor_id", F.col("doctor_id").cast("long"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
)

# normaliza variações que sobraram (ex.: "PRONTO  SOCORRO" com espaço duplo)
medicos_clean = medicos_clean.withColumn(
    "department", F.regexp_replace(F.trim("department"), r"\s+", " ")
)

medicos_silver = medicos_clean.select(
    "doctor_id", "full_name", "crm_numero", "crm_uf", "specialty",
    "department", "hire_date", "is_active",
)

medicos_silver.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.medicos_silver")
display(medicos_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — `procedimentos` (catálogo pequeno, candidato a broadcast)

# COMMAND ----------

procedimentos_silver = bronze_procedimentos.withColumn(
    "avg_duration_min", F.col("avg_duration_min").cast("int")
).withColumn("avg_cost", F.col("avg_cost").cast("double"))

procedimentos_silver.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.procedimentos_silver")
display(procedimentos_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — `atendimentos` (tabela fato, alto volume)
# MAGIC Esta é a tabela onde mais vale a pena investir tempo: IDs sujos (espaço, `.0`,
# MAGIC zero-padding), timestamps em múltiplos formatos, custo em formato monetário
# MAGIC BR (`R$ 1.234,56`), CID extraído de texto livre via regex, e duplicatas de
# MAGIC `encounter_id` por reprocessamento.

# COMMAND ----------

# regex: remove qualquer coisa que não seja dígito antes do cast (resolve " 123", "123.0", "0000000123")
atendimentos_clean = (
    bronze_atendimentos.withColumn(
        "patient_id", F.regexp_extract(F.trim("patient_id"), r"(\d+)", 1).cast("long")
    )
    .withColumn(
        "doctor_id", F.regexp_extract(F.trim("doctor_id"), r"(\d+)", 1).cast("long")
    )
    .withColumn("encounter_id", F.col("encounter_id").cast("long"))
)

# nulo quando regex não encontrou dígito nenhum (regexp_extract retorna "" e cast vira null-safe, mas garantimos)
atendimentos_clean = atendimentos_clean.withColumn(
    "patient_id", F.when(F.col("patient_id") == 0, None).otherwise(F.col("patient_id"))
).withColumn(
    "doctor_id", F.when(F.col("doctor_id") == 0, None).otherwise(F.col("doctor_id"))
)

# timestamp em 3 formatos possíveis
atendimentos_clean = atendimentos_clean.withColumn(
    "encounter_timestamp",
    F.coalesce(
        F.to_timestamp("encounter_timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("encounter_timestamp", "dd/MM/yyyy HH:mm"),
        F.to_timestamp("encounter_timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
    ),
)

# regex: extrai código CID (padrão tipo "J45.0" ou "I10") do texto livre de diagnóstico
atendimentos_clean = atendimentos_clean.withColumn(
    "diagnosis_code",
    F.regexp_extract("diagnosis_text", r"\b([A-Z]\d{2}\.?\d?)\b", 1),
).withColumn(
    "diagnosis_code", F.when(F.col("diagnosis_code") == "", None).otherwise(F.col("diagnosis_code"))
)

# regex: extrai telefone e e-mail da nota de contato (texto livre)
atendimentos_clean = atendimentos_clean.withColumn(
    "contact_phone_extracted",
    F.regexp_extract("contact_note", r"(\(?\d{2}\)?\s?\d{4,5}-?\d{4})", 1),
).withColumn(
    "contact_email_extracted",
    F.regexp_extract("contact_note", r"[\w\.\-]+@[\w\.\-]+\.\w+", 0),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padronização categórica e custo
# MAGIC Custo vem em dois formatos: `"1234.56"` (ponto decimal) ou `"R$ 1.234,56"`
# MAGIC (padrão monetário BR, ponto = milhar, vírgula = decimal). Precisamos detectar
# MAGIC qual formato é qual **antes** de tratar, senão `1.234` vira `1234` errado.

# COMMAND ----------

atendimentos_clean = (
    atendimentos_clean.withColumn("encounter_type", F.upper(F.trim("encounter_type")))
    .withColumn("department", F.upper(F.trim(F.regexp_replace("department", r"[\.\-]", ""))))
    .withColumn("status", F.upper(F.trim(F.regexp_replace("status", "_", " "))))
)
atendimentos_clean = atendimentos_clean.withColumn(
    "status",
    F.when(F.col("status") == "CONCLUÍDO", "CONCLUIDO")
    .when(F.col("status") == "NO SHOW", "NO-SHOW")
    .when(F.col("status") == "FALTOU", "NO-SHOW")
    .otherwise(F.col("status")),
)

# se tem "R$" ou vírgula decimal -> formato BR; senão -> formato já numérico
cost_raw = F.trim(F.col("cost_raw"))
is_br_format = cost_raw.rlike(r"R\$|,\d{2}$")

atendimentos_clean = atendimentos_clean.withColumn(
    "cost",
    F.when(
        is_br_format,
        F.regexp_replace(F.regexp_replace(cost_raw, r"[R\$\s\.]", ""), ",", ".").cast("double"),
    ).otherwise(cost_raw.cast("double")),
)

atendimentos_clean = atendimentos_clean.withColumn(
    "duration_minutes", F.col("duration_minutes").cast("int")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regras de qualidade (DQ) + deduplicação + split good/quarantine
# MAGIC Em vez de descartar silenciosamente, marcamos cada linha com o motivo da falha —
# MAGIC essencial para auditoria e para o time de dados justificar métricas de qualidade.

# COMMAND ----------

atendimentos_flagged = atendimentos_clean.withColumn(
    "dq_issues",
    F.array_remove(
        F.array(
            F.when(F.col("patient_id").isNull(), F.lit("patient_id_nulo")),
            F.when(F.col("doctor_id").isNull(), F.lit("doctor_id_nulo")),
            F.when(F.col("encounter_timestamp").isNull(), F.lit("timestamp_invalido")),
            F.when(F.col("encounter_timestamp") > F.current_timestamp(), F.lit("timestamp_futuro")),
            F.when(F.col("cost") < 0, F.lit("custo_negativo")),
            F.when(F.col("cost") > 50000, F.lit("custo_outlier")),
            F.when(F.col("duration_minutes") < 0, F.lit("duracao_negativa")),
        ),
        None,
    ),
).withColumn("dq_status", F.when(F.size("dq_issues") == 0, "OK").otherwise("QUARENTENA"))

# dedup de encounter_id duplicado (reenvio): mantém a ocorrência mais recente
w_enc = Window.partitionBy("encounter_id").orderBy(F.col("encounter_timestamp").desc_nulls_last())
atendimentos_flagged = atendimentos_flagged.withColumn(
    "rn", F.row_number().over(w_enc)
).filter(F.col("rn") == 1).drop("rn")

# COMMAND ----------

# explain: repare no shuffle (Exchange) causado pelo Window/row_number acima
atendimentos_flagged.select("encounter_id", "dq_status").explain()

# COMMAND ----------

atendimentos_ok = atendimentos_flagged.filter(F.col("dq_status") == "OK").drop("dq_issues", "dq_status")
atendimentos_quarentena = atendimentos_flagged.filter(F.col("dq_status") == "QUARENTENA")

print(f"OK: {atendimentos_ok.count():,} | Quarentena: {atendimentos_quarentena.count():,}")

atendimentos_ok.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.atendimentos_silver")
atendimentos_quarentena.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.atendimentos_quarentena")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC Tabelas Delta criadas: `pacientes_silver`, `medicos_silver`, `procedimentos_silver`,
# MAGIC `atendimentos_silver`, `atendimentos_quarentena`.
# MAGIC
# MAGIC Siga para o notebook **02_Silver_Joins_Broadcast_Explain** para a parte de joins,
# MAGIC broadcast, planos de execução e skew.
