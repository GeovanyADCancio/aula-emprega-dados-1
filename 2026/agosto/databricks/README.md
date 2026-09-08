# Pipeline Medalhão (Bronze → Silver → Gold) no Databricks

Conversão do script Airflow + pandas em 3 notebooks PySpark, gravando as tabelas
direto no **Unity Catalog**, mais um **Job** para agendar o pipeline diário.

Arquivos:
- `01_bronze.py` — lê os CSVs do Volume e grava as tabelas bronze
- `02_silver.py` — checa qualidade e aplica as transformações (silver)
- `03_gold.py` — monta as tabelas analíticas finais (gold)

## O que mudou em relação ao script original

| Script original (pandas) | Notebook (PySpark) |
|---|---|
| `pd.read_csv` + Postgres via SQLAlchemy | `spark.read.csv` + tabela gerenciada no catálogo (`saveAsTable`) |
| `df.to_sql(..., if_exists="replace")` | `.write.mode("overwrite").saveAsTable(...)` |
| Transformações com `.str`, `.fillna`, `np.where` | Mesma lógica com `pyspark.sql.functions` (`F.when`, `F.coalesce`, `F.regexp_extract`...) |
| Uma DAG do Airflow chamando 3 funções Python | Um **Job** do Databricks com 3 *tasks* em sequência |

A lógica de negócio (colunas, regras de `over_expenses`, `duration_hours`,
extração de `condition_type` etc.) é a mesma — só a forma de ler/escrever os
dados mudou.

## Pré-requisitos

- Você já criou um **Volume** (ex.: `workspace.default.dados_mentoria`) e subiu
  os arquivos `patients.csv`, `encounters.csv` e `conditions.csv` dentro dele.
- Você tem permissão para criar tabelas em algum catálogo/schema (pode usar o
  catálogo `workspace` / schema `default` para o exemplo).

## Passo a passo

### 1. Importar os notebooks
1. No menu lateral do Databricks, vá em **Workspace**.
2. Clique em **Create > Notebook** ou use **Import** (ícone de nuvem) para
   subir os três arquivos `.py` (o Databricks reconhece o cabeçalho
   `# Databricks notebook source` e já abre como notebook, com as células
   separadas por `# COMMAND ----------`).
3. Confirme que os três aparecem como notebooks separados: `01_bronze`,
   `02_silver`, `03_gold`.

### 2. Anexar um cluster
1. Abra o notebook `01_bronze`.
2. No topo, em **Connect**, selecione um cluster com Databricks Runtime
   (qualquer cluster com PySpark + Delta já serve, não precisa de nada extra).

### 3. Ajustar os widgets (parâmetros)
Cada notebook tem 3 widgets no topo — ajuste se seu Volume/catálogo tiver
nomes diferentes dos padrões:

| Widget | Valor padrão | O que é |
|---|---|---|
| `catalog` | `workspace` | catálogo do Unity Catalog onde as tabelas serão criadas |
| `schema` | `default` | schema dentro do catálogo |
| `volume_path` (só no bronze) | `/Volumes/workspace/default/dados_mentoria` | pasta do Volume com os 3 CSVs |

### 4. Rodar manualmente para testar
1. Em `01_bronze`, clique em **Run all**. Ao final, você deve ver as tabelas
   `bronze_patients`, `bronze_encounters` e `bronze_conditions` no catálogo
   (confira em **Catalog** no menu lateral).
2. Rode `02_silver` da mesma forma → cria `silver_patients`,
   `silver_encounters`, `silver_conditions`.
3. Rode `03_gold` → cria `gold_obt_encounters`, `gold_patient_summary`,
   `gold_encounter_summary`.

Se algo der errado na checagem de qualidade do `02_silver`, o notebook para
sozinho com uma mensagem de alerta (não quebra com erro feio) — é o mesmo
comportamento do `check_data_quality()` do script original.

### 5. Criar o Job (agendamento do pipeline)
1. No menu lateral, vá em **Workflows > Jobs > Create Job**.
2. Dê um nome, ex.: `etl_bronze_silver_gold`.
3. **Task 1**: nome `bronze`, tipo *Notebook*, aponte para `01_bronze`,
   selecione o cluster.
4. **Task 2**: nome `silver`, tipo *Notebook*, aponte para `02_silver`, e em
   **Depends on** selecione `bronze`.
5. **Task 3**: nome `gold`, tipo *Notebook*, aponte para `03_gold`, e em
   **Depends on** selecione `silver`.
6. Em **Schedule**, clique em **Add trigger > Scheduled**, use o cron
   `0 0 * * *` (todo dia à meia-noite, igual ao `schedule_interval` da DAG
   original) e escolha o fuso horário.
7. Salve. Para testar, clique em **Run now** e acompanhe em **Runs** — cada
   task aparece com seu status individual, na mesma ideia de
   `bronze >> silver >> gold` do Airflow.

## Dica para a aula

Para os alunos visualizarem o resultado sem escrever SQL, depois do `Run all`
de qualquer notebook, use `display(spark.table("catalogo.schema.tabela"))`
(já incluso no final do `01_bronze` e do `03_gold`) — ele mostra a tabela em
formato de grade, com opção de gráfico direto na célula.