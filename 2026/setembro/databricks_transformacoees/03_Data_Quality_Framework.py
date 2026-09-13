# Databricks notebook source
# MAGIC %md
# MAGIC # Aula: Camada Silver com PySpark — Rede Hospitalar
# MAGIC ### Parte 1 — Data Quality: diagnosticar sem corrigir
# MAGIC
# MAGIC Diferença para a Parte 0 (EDA): lá exploramos "no olho". Aqui formalizamos isso
# MAGIC em **regras nomeadas, versionadas e mensuráveis** — o padrão usado em times de
# MAGIC dados de verdade (Great Expectations, Deequ, DQX, etc. fazem essencialmente isso).
# MAGIC
# MAGIC Duas categorias de regra:
# MAGIC - **Técnica** — problema dentro da própria coluna/tabela (nulo, formato, duplicidade).
# MAGIC - **Negócio** — problema que só existe olhando a regra do domínio, às vezes
# MAGIC   cruzando tabelas (ex.: *todo atendimento precisa ter um paciente válido*).
# MAGIC
# MAGIC Este notebook **não altera nenhuma coluna original**. Ele só mede, classifica e
# MAGIC separa os dados brutos em `válido` / `quarentena` para inspeção — a limpeza de
# MAGIC verdade (parsing, padronização) continua acontecendo no notebook 02.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "dados_saude"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
DQ_SCHEMA = f"{CATALOG}.{SCHEMA}"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt

pacientes = spark.read.option("header", True).csv(f"{BASE_PATH}/pacientes.csv")
medicos = spark.read.option("header", True).csv(f"{BASE_PATH}/medicos.csv")
atendimentos = spark.read.option("header", True).csv(f"{BASE_PATH}/atendimentos.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Motor de regras
# MAGIC Em vez de escrever um monte de `.filter()` soltos, cada regra é um item de uma
# MAGIC lista: `id`, `nome`, `tipo` (tecnica/negocio), `critica` (se falhar, o registro
# MAGIC vai pra quarentena) e a condição em si (uma função que devolve uma coluna
# MAGIC booleana = "esse registro violou a regra?"). Isso deixa fácil adicionar,
# MAGIC remover ou explicar regra por regra para a turma.

# COMMAND ----------

def aplicar_regras(df, regras):
    """Adiciona uma coluna booleana 'viol_<id>' pra cada regra, sem tocar nas colunas originais."""
    df_flag = df
    for r in regras:
        df_flag = df_flag.withColumn(f"viol_{r['id']}", r["cond"](df_flag).cast("int"))
    return df_flag


def resumo_regras(df_flag, regras, nome_tabela):
    """Uma única agregação para contar violação de todas as regras de uma vez (evita 1 count() por regra)."""
    agregacoes = [F.sum(f"viol_{r['id']}").alias(r["id"]) for r in regras]
    linha = df_flag.agg(F.count("*").alias("total_linhas"), *agregacoes).collect()[0]
    total = linha["total_linhas"]

    dados = []
    for r in regras:
        qtd = linha[r["id"]] or 0
        dados.append(
            {
                "tabela": nome_tabela,
                "regra_id": r["id"],
                "regra_nome": r["nome"],
                "tipo": r["tipo"],
                "critica": r["critica"],
                "total_linhas": total,
                "qtd_violacoes": int(qtd),
                "pct_violacoes": round(qtd / total * 100, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras técnicas — `pacientes`
# MAGIC Mesma lógica que identificamos na limpeza da Silver, só que aqui **não vamos
# MAGIC corrigir** — só contar quantos registros violam cada uma.

# COMMAND ----------

regras_pacientes = [
    dict(id="P01", nome="cpf_nulo", tipo="tecnica", critica=True,
         cond=lambda df: F.col("cpf").isNull()),
    dict(id="P02", nome="cpf_formato_invalido", tipo="tecnica", critica=True,
         cond=lambda df: F.col("cpf").isNotNull()
         & (F.length(F.regexp_replace("cpf", r"[^0-9]", "")) != 11)),
    dict(id="P03", nome="telefone_nulo", tipo="tecnica", critica=False,
         cond=lambda df: F.col("phone").isNull() | (F.trim("phone") == "")),
    dict(id="P04", nome="email_invalido", tipo="tecnica", critica=False,
         cond=lambda df: F.col("email").isNotNull()
         & ~F.col("email").rlike(r"^[\w\.\-]+@[\w\.\-]+\.\w+$")),
    dict(id="P05", nome="estado_fora_do_dominio", tipo="tecnica", critica=False,
         cond=lambda df: F.col("address_state").isNotNull()
         & ~F.upper(F.trim("address_state")).isin(
             "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA",
             "PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO",
         )),
    dict(id="P06", nome="cpf_duplicado", tipo="tecnica", critica=True,
         cond=lambda df: F.count("*").over(Window.partitionBy("cpf")) > 1),
]

pacientes_flag = aplicar_regras(pacientes, regras_pacientes)
resumo_pacientes = resumo_regras(pacientes_flag, regras_pacientes, "pacientes")
display(spark.createDataFrame(resumo_pacientes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regras técnicas — `atendimentos`

# COMMAND ----------

regras_atend_tecnicas = [
    dict(id="A01", nome="patient_id_formato_sujo", tipo="tecnica", critica=False,
         cond=lambda df: F.col("patient_id").isNotNull()
         & ~F.trim("patient_id").rlike(r"^\d+$")),
    dict(id="A02", nome="timestamp_nulo", tipo="tecnica", critica=True,
         cond=lambda df: F.col("encounter_timestamp").isNull()),
    dict(id="A03", nome="custo_nulo_ou_vazio", tipo="tecnica", critica=False,
         cond=lambda df: F.col("cost_raw").isNull() | (F.trim("cost_raw") == "")),
    dict(id="A04", nome="custo_negativo", tipo="tecnica", critica=False,
         cond=lambda df: F.trim("cost_raw").rlike(r"^-")),
    dict(id="A05", nome="duracao_negativa", tipo="tecnica", critica=False,
         cond=lambda df: F.col("duration_minutes").cast("int") < 0),
    dict(id="A06", nome="encounter_id_duplicado", tipo="tecnica", critica=True,
         cond=lambda df: F.count("*").over(Window.partitionBy("encounter_id")) > 1),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Regras de negócio — `atendimentos`
# MAGIC Aqui é onde a régua muda: não é "o dado está bem formatado?", é **"isso faz
# MAGIC sentido pro negócio hospitalar?"**. Algumas dependem de cruzar com `pacientes`
# MAGIC e `medicos` — por isso criamos duas colunas de apoio só para checagem
# MAGIC (não substituem a coluna original, é só para avaliar a regra).

# COMMAND ----------

# staging só para permitir os joins de checagem — não vira coluna final em lugar nenhum
atend_staging = atendimentos.withColumn(
    "_patient_id_chk", F.regexp_extract(F.trim("patient_id"), r"(\d+)", 1).cast("long")
).withColumn(
    "_doctor_id_chk", F.regexp_extract(F.trim("doctor_id"), r"(\d+)", 1).cast("long")
)

pacientes_ref = pacientes.select(
    F.col("patient_id").cast("long").alias("_pid"),
    F.col("is_active").alias("_paciente_is_active"),
)
medicos_ref = medicos.select(
    F.col("doctor_id").cast("long").alias("_did"),
    F.col("is_active").alias("_medico_is_active"),
)

atend_staging = (
    atend_staging.join(pacientes_ref, atend_staging._patient_id_chk == pacientes_ref._pid, "left")
    .join(medicos_ref, atend_staging._doctor_id_chk == medicos_ref._did, "left")
)

# COMMAND ----------

regras_atend_negocio = [
    dict(id="B01", nome="atendimento_sem_paciente_cadastrado", tipo="negocio", critica=True,
         cond=lambda df: df["_pid"].isNull()),
    dict(id="B02", nome="atendimento_sem_medico_cadastrado", tipo="negocio", critica=True,
         cond=lambda df: df["_did"].isNull()),
    dict(id="B03", nome="atendimento_cancelado_com_custo_cobrado", tipo="negocio", critica=False,
         cond=lambda df: F.upper(F.trim("status")).rlike("CANCEL")
         & (F.regexp_replace(F.regexp_replace(F.trim("cost_raw"), r"[R\$\s\.]", ""), ",", ".").cast("double") > 0)),
    dict(id="B04", nome="atendimento_com_paciente_inativo", tipo="negocio", critica=False,
         cond=lambda df: F.lower(F.trim(df["_paciente_is_active"])).isin("0", "false", "n", "nao", "não")),
    dict(id="B05", nome="atendimento_com_medico_inativo", tipo="negocio", critica=False,
         cond=lambda df: F.trim(df["_medico_is_active"]) == "0"),
    dict(id="B06", nome="consulta_com_duracao_fora_do_padrao", tipo="negocio", critica=False,
         cond=lambda df: F.upper(F.trim("encounter_type")).rlike("CONSULTA")
         & ((F.col("duration_minutes").cast("int") > 180) | (F.col("duration_minutes").cast("int") <= 0))),
]

# COMMAND ----------

regras_atend_todas = regras_atend_tecnicas + regras_atend_negocio
atend_flag = aplicar_regras(atend_staging, regras_atend_todas)
resumo_atend = resumo_regras(atend_flag, regras_atend_todas, "atendimentos")
display(spark.createDataFrame(resumo_atend))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualização — % de violação por regra
# MAGIC Gráfico simples, direto para a aula: quanto maior a barra, mais urgente
# MAGIC tratar aquela regra na Silver.

# COMMAND ----------

resumo_geral = pd.concat([resumo_pacientes, resumo_atend], ignore_index=True)
resumo_geral = resumo_geral.sort_values("pct_violacoes", ascending=True)

cores = resumo_geral["tipo"].map({"tecnica": "#4C78A8", "negocio": "#E45756"})

plt.figure(figsize=(9, 6))
plt.barh(resumo_geral["regra_nome"], resumo_geral["pct_violacoes"], color=cores)
plt.xlabel("% de linhas que violam a regra")
plt.title("Data Quality — % de violação por regra (azul = técnica, vermelho = negócio)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Separação válido / quarentena (sem corrigir nada)
# MAGIC Regra de negócio simples e didática: um registro vai pra **quarentena** se
# MAGIC violar **qualquer regra crítica** (`critica=True`). Regras não-críticas geram
# MAGIC alerta mas não tiram o registro do fluxo — fica registrado o motivo, e quem
# MAGIC decide o que fazer com isso é o time de dados/negócio, não o pipeline sozinho.

# COMMAND ----------

regras_criticas = [r for r in regras_atend_todas if r["critica"]]
cols_criticas = [f"viol_{r['id']}" for r in regras_criticas]

atend_classificado = atend_flag.withColumn(
    "falhou_regra_critica",
    F.greatest(*[F.col(c) for c in cols_criticas]) == 1,
).withColumn(
    "motivos_falha",
    F.concat_ws(
        ", ",
        *[F.when(F.col(f"viol_{r['id']}") == 1, F.lit(r["nome"])) for r in regras_atend_todas],
    ),
)

colunas_originais = atendimentos.columns
atend_dq_validos = atend_classificado.filter(~F.col("falhou_regra_critica")).select(
    *colunas_originais, "motivos_falha"
)
atend_dq_quarentena = atend_classificado.filter(F.col("falhou_regra_critica")).select(
    *colunas_originais, "motivos_falha"
)

total = atendimentos.count()
n_valido = atend_dq_validos.count()
n_quarentena = atend_dq_quarentena.count()
print(f"Válidos    : {n_valido:,} ({n_valido/total*100:.1f}%)")
print(f"Quarentena : {n_quarentena:,} ({n_quarentena/total*100:.1f}%)")

atend_dq_validos.write.mode("overwrite").saveAsTable(f"{DQ_SCHEMA}.atendimentos_dq_validos")
atend_dq_quarentena.write.mode("overwrite").saveAsTable(f"{DQ_SCHEMA}.atendimentos_dq_quarentena")

display(atend_dq_quarentena.select("encounter_id", "patient_id", "doctor_id", "motivos_falha").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Histórico de execuções (para acompanhar a evolução da qualidade)
# MAGIC Cada vez que este notebook roda (ex.: uma vez por dia, junto da ingestão),
# MAGIC gravamos o resumo com **carimbo de data/hora** numa tabela append-only.
# MAGIC Assim, depois de algumas execuções, dá pra responder: *"a % de atendimento
# MAGIC sem paciente cadastrado está melhorando ou piorando mês a mês?"*

# COMMAND ----------

run_ts = F.current_timestamp()

historico_novo = spark.createDataFrame(resumo_geral).withColumn("run_timestamp", run_ts)

(
    historico_novo.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{DQ_SCHEMA}.dq_history")
)

print("Resumo desta execução gravado em dq_history.")
display(spark.table(f"{DQ_SCHEMA}.dq_history").orderBy(F.desc("run_timestamp")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolução de uma regra específica ao longo do tempo
# MAGIC Rode este notebook em dias diferentes (ou simule reexecuções) para ver a linha
# MAGIC ganhar mais pontos. Isso é o que vira, em produção, um painel de qualidade de
# MAGIC dados no Grafana ou no Lakeview do Databricks.

# COMMAND ----------

historico_pd = (
    spark.table(f"{DQ_SCHEMA}.dq_history")
    .filter(F.col("regra_id") == "B01")
    .orderBy("run_timestamp")
    .toPandas()
)

plt.figure(figsize=(8, 4))
plt.plot(historico_pd["run_timestamp"], historico_pd["pct_violacoes"], marker="o")
plt.xticks(rotation=30)
plt.ylabel("% de violação")
plt.title("Evolução: % de atendimentos sem paciente cadastrado (regra B01)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Parte 1
# MAGIC - Regras técnicas e de negócio ficam **declaradas como dados** (lista de dicts),
# MAGIC não espalhadas em `filter()` soltos — fácil de auditar e estender.
# MAGIC - Nenhuma coluna original foi alterada; só adicionamos flags de diagnóstico.
# MAGIC - `atendimentos_dq_validos` / `atendimentos_dq_quarentena` — split por regra
# MAGIC   crítica, cada linha em quarentena carrega o motivo.
# MAGIC - `dq_history` — tabela append-only para acompanhar tendência ao longo do tempo.
# MAGIC
# MAGIC Siga para o notebook **02_Bronze_para_Silver_Limpeza** — lá sim vamos corrigir,
# MAGIC parsear e padronizar o que identificamos aqui.
