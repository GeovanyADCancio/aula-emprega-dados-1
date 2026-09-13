# Databricks notebook source
# MAGIC %md
# MAGIC # Aula: Camada Silver com PySpark — Rede Hospitalar
# MAGIC ### Parte 0 — Análise Exploratória (EDA)
# MAGIC
# MAGIC Objetivo: **antes** de escrever qualquer transformação, investigar os dados
# MAGIC e listar os problemas que vamos precisar resolver na Silver. Esse notebook não
# MAGIC corrige nada — só diagnostica.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "dados_saude"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

from pyspark.sql import functions as F

pacientes = spark.read.option("header", True).csv(f"{BASE_PATH}/pacientes.csv")
medicos = spark.read.option("header", True).csv(f"{BASE_PATH}/medicos.csv")
procedimentos = spark.read.option("header", True).csv(f"{BASE_PATH}/procedimentos.csv")
atendimentos = spark.read.option("header", True).csv(f"{BASE_PATH}/atendimentos.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Visão geral — tamanho e schema
# MAGIC Primeira pergunta de sempre: quantas linhas, quantas colunas, e qual o tipo
# MAGIC que o Spark leu (lembrando: lemos tudo como string de propósito).

# COMMAND ----------

for nome, df in [
    ("pacientes", pacientes), ("medicos", medicos),
    ("procedimentos", procedimentos), ("atendimentos", atendimentos),
]:
    print(f"{nome}: {df.count():,} linhas | {len(df.columns)} colunas")

# COMMAND ----------

atendimentos.printSchema()

# COMMAND ----------

display(pacientes.limit(10))

# COMMAND ----------

display(atendimentos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Valores nulos por coluna
# MAGIC Padrão muito usado: contar nulos de todas as colunas de uma vez, sem escrever
# MAGIC um `filter` pra cada uma.

# COMMAND ----------

def contar_nulos(df):
    return df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )

display(contar_nulos(pacientes))

# COMMAND ----------

display(contar_nulos(atendimentos))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pacientes — CPF duplicado (recadastro)
# MAGIC Se um mesmo CPF aparece com `patient_id` diferentes, é sinal de paciente
# MAGIC recadastrado — problema que vamos resolver com dedup na Silver.

# COMMAND ----------

cpf_duplicado = (
    pacientes.filter(F.col("cpf").isNotNull())
    .groupBy("cpf")
    .agg(F.count("*").alias("qtd_cadastros"))
    .filter(F.col("qtd_cadastros") > 1)
)

print(f"CPFs com mais de 1 cadastro: {cpf_duplicado.count():,}")
display(cpf_duplicado.orderBy(F.desc("qtd_cadastros")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pacientes — formatos inconsistentes (categóricas)
# MAGIC `distinct()` numa coluna categórica é a forma mais rápida de ver todas as
# MAGIC variações de escrita que vamos precisar padronizar.

# COMMAND ----------

display(pacientes.select("gender").distinct())

# COMMAND ----------

display(pacientes.select("address_state").distinct().orderBy("address_state"))

# COMMAND ----------

display(pacientes.select("is_active").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pacientes — CPF com formato/quantidade de dígitos estranha

# COMMAND ----------

cpf_digitos = pacientes.withColumn(
    "qtd_digitos", F.length(F.regexp_replace("cpf", r"[^0-9]", ""))
)

display(cpf_digitos.groupBy("qtd_digitos").count().orderBy("qtd_digitos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Atendimentos — integridade referencial (FKs órfãs)
# MAGIC Pergunta chave antes de qualquer join: **quantos `patient_id`/`doctor_id`
# MAGIC em `atendimentos` não existem na tabela de dimensão correspondente?**
# MAGIC
# MAGIC Repare que aqui ainda não convertemos tipo — por isso comparamos como string
# MAGIC mesmo, só pra já enxergar a ordem de grandeza do problema.

# COMMAND ----------

patient_ids_validos = pacientes.select(F.col("patient_id").alias("id")).distinct()

atend_patient_check = atendimentos.join(
    patient_ids_validos, atendimentos.patient_id == patient_ids_validos.id, "left"
)

sem_match = atend_patient_check.filter(F.col("id").isNull()).count()
total = atendimentos.count()
print(f"atendimentos.patient_id sem correspondência em pacientes: {sem_match:,} ({sem_match/total*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC Boa parte desse número não é "paciente inexistente de verdade" — é `patient_id`
# MAGIC em formato sujo (`" 123"`, `"123.0"`) que não bate como string. Dá pra confirmar
# MAGIC olhando uma amostra:

# COMMAND ----------

display(
    atend_patient_check.filter(F.col("id").isNull())
    .select("encounter_id", "patient_id")
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Atendimentos — datas em formatos diferentes
# MAGIC Se tentarmos `to_date` com um único formato, uma parte vira `null` — é assim
# MAGIC que vamos descobrir que existe mais de um formato na mesma coluna.

# COMMAND ----------

check_datas = atendimentos.withColumn(
    "parse_formato_1", F.to_date("encounter_timestamp", "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "parse_formato_2", F.to_date("encounter_timestamp", "dd/MM/yyyy HH:mm")
)

print("Não bateram em nenhum dos dois formatos testados:")
display(
    check_datas.filter(
        F.col("parse_formato_1").isNull() & F.col("parse_formato_2").isNull()
    ).select("encounter_timestamp").distinct().limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Atendimentos — custo em mais de um formato
# MAGIC Olhando a amostra dá pra ver claramente dois padrões: `"1234.56"` e
# MAGIC `"R$ 1.234,56"`. Se a gente só desse `.cast("double")` direto, o segundo
# MAGIC formato viraria `null` inteiro.

# COMMAND ----------

display(atendimentos.select("cost_raw").distinct().limit(20))

# COMMAND ----------

teste_cast_direto = atendimentos.withColumn("cost_double", F.col("cost_raw").cast("double"))
falhas_cast = teste_cast_direto.filter(
    F.col("cost_raw").isNotNull() & F.col("cost_double").isNull()
).count()
print(f"Linhas que virariam NULL com cast direto (sem tratar formato BR): {falhas_cast:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Atendimentos — outliers de custo e duração
# MAGIC `describe()`/`summary()` mostra estatísticas básicas rápido — útil pra notar
# MAGIC valor mínimo negativo ou máximo absurdamente alto (candidato a outlier/erro).

# COMMAND ----------

atendimentos.withColumn("cost_double", F.col("cost_raw").cast("double")).select(
    "cost_double", "duration_minutes"
).summary("min", "25%", "50%", "75%", "max").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Atendimentos — categóricas sujas (tipo, status, departamento)

# COMMAND ----------

display(atendimentos.select("status").distinct())

# COMMAND ----------

display(atendimentos.select("department").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Atendimentos — volume por médico (skew)
# MAGIC Fica evidente olhando um `groupBy` simples: um `doctor_id` bem à frente dos
# MAGIC outros. Isso vira problema de performance quando fizermos o join na Parte 2.

# COMMAND ----------

display(
    atendimentos.groupBy("doctor_id").count().orderBy(F.desc("count")).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Atendimentos — `encounter_id` duplicado

# COMMAND ----------

dup_encounter = (
    atendimentos.groupBy("encounter_id")
    .count()
    .filter(F.col("count") > 1)
)
print(f"encounter_id com mais de 1 ocorrência: {dup_encounter.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo do diagnóstico (preencher com a turma)
# MAGIC | Tabela | Problema encontrado | Como vamos tratar na Silver |
# MAGIC |---|---|---|
# MAGIC | pacientes | CPF duplicado (recadastro) | dedup com `Window` + `row_number` |
# MAGIC | pacientes | gender/state/is_active com grafias variadas | padronização via `when/otherwise` |
# MAGIC | pacientes | CPF com quantidade de dígitos != 11 | flag `cpf_valido` |
# MAGIC | atendimentos | patient_id/doctor_id em formato sujo | `regexp_extract` + `cast` |
# MAGIC | atendimentos | timestamp em múltiplos formatos | `coalesce(to_date(...))` |
# MAGIC | atendimentos | custo em 2 formatos (BR e numérico) | `rlike` + `regexp_replace` condicional |
# MAGIC | atendimentos | status/department/type sujos | padronização via `when/otherwise` |
# MAGIC | atendimentos | encounter_id duplicado | dedup com `Window` |
# MAGIC | atendimentos | skew no doctor_id | broadcast / salting (Parte 2) |
# MAGIC
# MAGIC Siga para o notebook **01_Bronze_para_Silver_Limpeza** para as transformações.
