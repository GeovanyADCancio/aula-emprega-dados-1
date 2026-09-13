# Databricks notebook source
# MAGIC %md
# MAGIC # Aula: Camada Silver com PySpark — Rede Hospitalar
# MAGIC ### Parte 2 — Joins, Broadcast, Explain Plans e Skew
# MAGIC
# MAGIC Pré-requisito: rodar o notebook `01_Bronze_para_Silver_Limpeza` antes deste.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
SILVER_SCHEMA = f"{CATALOG}.{SCHEMA}"

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

# COMMAND ----------

pacientes = spark.table(f"{SILVER_SCHEMA}.pacientes_silver")
medicos = spark.table(f"{SILVER_SCHEMA}.medicos_silver")
procedimentos = spark.table(f"{SILVER_SCHEMA}.procedimentos_silver")
atendimentos = spark.table(f"{SILVER_SCHEMA}.atendimentos_silver")

for nome, df in [
    ("pacientes", pacientes), ("medicos", medicos),
    ("procedimentos", procedimentos), ("atendimentos", atendimentos),
]:
    print(f"{nome}: {df.count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. INNER JOIN — descarta atendimentos sem paciente válido
# MAGIC Use quando o objetivo é análise de atendimentos **de pacientes cadastrados**,
# MAGIC e registro órfão realmente não interessa (ex.: relatório clínico por paciente).

# COMMAND ----------

atend_inner = atendimentos.join(pacientes, on="patient_id", how="inner")

print(f"atendimentos original : {atendimentos.count():,}")
print(f"após inner join       : {atend_inner.count():,}")
print(f"descartados (órfãos)  : {atendimentos.count() - atend_inner.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LEFT JOIN — auditoria de integridade referencial
# MAGIC Em produção, muitas vezes você **não quer perder** a linha órfã — você quer
# MAGIC medi-la. `LEFT JOIN` + filtro por nulo é o padrão para relatório de qualidade
# MAGIC de FK (quantos % dos atendimentos referenciam paciente inexistente).

# COMMAND ----------

atend_left = atendimentos.join(pacientes, on="patient_id", how="left")

orfaos = atend_left.filter(F.col("full_name").isNull())
pct_orfaos = orfaos.count() / atend_left.count() * 100
print(f"Atendimentos com patient_id órfão: {orfaos.count():,} ({pct_orfaos:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BROADCAST JOIN — tabelas de dimensão pequenas
# MAGIC `medicos` (~600 linhas) e `procedimentos` (~250 linhas) cabem inteiras na memória
# MAGIC de cada executor. Forçar broadcast evita o shuffle caro (`SortMergeJoin`) da
# MAGIC tabela fato de milhões de linhas.
# MAGIC
# MAGIC Compare os dois planos abaixo — procure por `BroadcastHashJoin` vs `SortMergeJoin`.

# COMMAND ----------

# Sem hint: o otimizador decide sozinho (Spark já teria feito broadcast automático,
# pois medicos está abaixo do limite padrão de spark.sql.autoBroadcastJoinThreshold — 10MB)
atend_medicos_auto = atendimentos.join(medicos, on="doctor_id", how="left")
atend_medicos_auto.explain()

# COMMAND ----------

# Forçando explicitamente com hint — mesma escolha do otimizador aqui,
# mas o hint é necessário quando o otimizador erra a estimativa (ex.: após muitos filtros)
atend_medicos_broadcast = atendimentos.join(broadcast(medicos), on="doctor_id", how="left")
atend_medicos_broadcast.explain()

# COMMAND ----------

# Desligando o auto-broadcast para forçar SortMergeJoin e ver a diferença no plano
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
atend_medicos_sortmerge = atendimentos.join(medicos, on="doctor_id", how="left")
atend_medicos_sortmerge.explain()
# repare no Exchange (shuffle) hash partitioning antes do SortMergeJoin — esse é o custo que o broadcast evita
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # volta ao padrão (10MB)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join múltiplo — construindo a tabela enriquecida
# MAGIC `atendimentos` (fato) + `pacientes` (left, para não perder órfão) +
# MAGIC `medicos` (broadcast) + `procedimentos` (broadcast).

# COMMAND ----------

atendimentos_enriquecido = (
    atendimentos.alias("a")
    .join(pacientes.alias("p"), on="patient_id", how="left")
    .join(broadcast(medicos.alias("m")), on="doctor_id", how="left")
    .join(broadcast(procedimentos.alias("proc")), on="procedure_code", how="left")
    .select(
        F.col("a.encounter_id"),
        F.col("a.patient_id"),
        F.col("p.full_name").alias("patient_name"),
        F.col("p.address_state"),
        F.col("a.doctor_id"),
        F.col("m.full_name").alias("doctor_name"),
        F.col("m.specialty"),
        F.col("a.procedure_code"),
        F.col("proc.description").alias("procedure_description"),
        F.col("proc.category").alias("procedure_category"),
        F.col("a.encounter_timestamp"),
        F.col("a.encounter_type"),
        F.col("a.department"),
        F.col("a.status"),
        F.col("a.diagnosis_code"),
        F.col("a.cost"),
        F.col("a.duration_minutes"),
    )
)

atendimentos_enriquecido.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Skew — um médico concentra ~15% dos atendimentos
# MAGIC Skew de dados é quando uma chave de join tem volume desproporcional. A task
# MAGIC que processa essa chave demora muito mais que as outras, e o job "trava" em
# MAGIC 99% de progresso esperando essa única task.

# COMMAND ----------

atendimentos.groupBy("doctor_id").count().orderBy(F.desc("count")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mitigação 1 — Adaptive Query Execution (padrão no Databricks Runtime)**
# MAGIC O Spark já tenta dividir automaticamente partições muito grandes (`skewJoin`).
# MAGIC Confirme que está ativo e veja a otimização no plano em runtime (aba SQL do Spark UI).

# COMMAND ----------

print(spark.conf.get("spark.sql.adaptive.enabled"))
print(spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Mitigação 2 — Salting manual** (útil quando AQE não é suficiente, ex.: join
# MAGIC contra tabela pequena que também tem a chave concentrada). Ideia: "espalhar"
# MAGIC a chave quente artificialmente em N sub-chaves, replicar o lado pequeno N vezes,
# MAGIC juntar pela chave composta, e no fim descartar o salt.

# COMMAND ----------

N_SALT = 8

atendimentos_salted = atendimentos.withColumn(
    "salt", (F.rand() * N_SALT).cast("int")
).withColumn("doctor_id_salted", F.concat_ws("_", F.col("doctor_id"), F.col("salt")))

medicos_expandido = medicos.crossJoin(
    spark.range(N_SALT).withColumnRenamed("id", "salt")
).withColumn("doctor_id_salted", F.concat_ws("_", F.col("doctor_id"), F.col("salt")))

atend_sem_skew = atendimentos_salted.join(
    broadcast(medicos_expandido), on="doctor_id_salted", how="left"
).drop("salt", "doctor_id_salted")

print(f"linhas antes: {atendimentos.count():,} | linhas depois do salting join: {atend_sem_skew.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Window functions — análise por paciente e por médico

# COMMAND ----------

# tempo (em dias) desde o atendimento anterior do mesmo paciente — útil para
# detectar reinternação precoce, por exemplo
w_paciente = Window.partitionBy("patient_id").orderBy("encounter_timestamp")

atend_com_recencia = atendimentos.withColumn(
    "atendimento_anterior", F.lag("encounter_timestamp").over(w_paciente)
).withColumn(
    "dias_desde_ultimo_atendimento",
    F.datediff(F.col("encounter_timestamp"), F.col("atendimento_anterior")),
)

display(
    atend_com_recencia.filter(F.col("dias_desde_ultimo_atendimento").isNotNull())
    .select("patient_id", "encounter_timestamp", "atendimento_anterior", "dias_desde_ultimo_atendimento")
    .orderBy("patient_id", "encounter_timestamp")
    .limit(10)
)

# COMMAND ----------

# ranking: atendimentos por médico, ordenados por volume, dentro de cada especialidade
w_especialidade = Window.partitionBy("specialty").orderBy(F.desc("total_atendimentos"))

ranking_medicos = (
    atendimentos.join(broadcast(medicos), on="doctor_id", how="inner")
    .groupBy("doctor_id", "full_name", "specialty")
    .agg(F.count("*").alias("total_atendimentos"))
    .withColumn("ranking_na_especialidade", F.rank().over(w_especialidade))
)

display(ranking_medicos.filter(F.col("ranking_na_especialidade") <= 3).orderBy("specialty", "ranking_na_especialidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Escrita final — Delta particionado
# MAGIC Particiona por ano/mês do atendimento — reduz o volume de dados lido em
# MAGIC consultas típicas de BI ("últimos 3 meses"). Não particione por coluna de alta
# MAGIC cardinalidade (ex.: `patient_id`) — gera excesso de arquivos pequenos.

# COMMAND ----------

atendimentos_final = atendimentos_enriquecido.withColumn(
    "ano_mes", F.date_format("encounter_timestamp", "yyyy-MM")
)

(
    atendimentos_final.write.mode("overwrite")
    .partitionBy("ano_mes")
    .saveAsTable(f"{SILVER_SCHEMA}.atendimentos_enriquecido")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compactação e ordenação física dos arquivos Delta (reduz small files, acelera filtros)
# MAGIC OPTIMIZE workspace.default.atendimentos_enriquecido ZORDER BY (doctor_id, patient_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercícios propostos para os alunos
# MAGIC 1. Quantos % dos atendimentos têm `doctor_id` órfão? E `procedure_code` inválido
# MAGIC    (não existe no catálogo)? Monte um `LEFT ANTI JOIN` para isolar essas linhas.
# MAGIC 2. Refaça o join de `procedimentos` sem `broadcast()` e compare o `explain()` —
# MAGIC    o otimizador ainda escolhe broadcast automaticamente? Por quê?
# MAGIC 3. Use `regexp_extract` para validar se `diagnosis_code` extraído bate com o
# MAGIC    padrão oficial de CID-10 (`[A-Z]\d{2}(\.\d)?`) e quantifique falsos positivos.
# MAGIC 4. Compare o tempo de execução do join com skew (sem tratamento) vs. com salting,
# MAGIC    usando a aba **Spark UI > SQL/DataFrame** para ver a distribuição de tasks.
# MAGIC 5. Crie uma tabela `atendimentos_dq_report` com a % de cada `dq_issue` do
# MAGIC    notebook 1, agrupado por mês — simulando um dashboard de qualidade de dados.
