"""
DAG geradora de logs #1 — pipeline "saudável"

Simula um pipeline de 3 etapas (extract -> transform -> load) rodando com
frequência alta, só pra gerar volume de log constante e demonstrar a
ingestão em tempo real no Grafana. De vez em quando solta um WARNING
(simulando uma linha com dado suspeito) pra dar variedade nos levels.
"""
from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")


def extract(**context):
    n_registros = random.randint(500, 5000)
    log.info("Iniciando extração da fonte 'vendas_loja_fisica'")
    time.sleep(random.uniform(1, 3))
    log.info(f"Extração concluída: {n_registros} registros lidos")
    context["ti"].xcom_push(key="n_registros", value=n_registros)


def transform(**context):
    n_registros = context["ti"].xcom_pull(task_ids="extract", key="n_registros")
    log.info(f"Iniciando transformação de {n_registros} registros")
    time.sleep(random.uniform(1, 4))

    # ~15% das execuções emitem um warning de qualidade de dado
    if random.random() < 0.15:
        n_invalidos = random.randint(1, 20)
        log.warning(
            f"{n_invalidos} registros descartados por falha de validação de schema "
            f"(campo 'valor_total' nulo ou negativo)"
        )

    log.info("Transformação concluída, dados normalizados no schema silver")


def load(**context):
    log.info("Iniciando carga na tabela silver.vendas_loja_fisica")
    time.sleep(random.uniform(0.5, 2))
    log.info("Carga concluída com sucesso")


default_args = {
    "owner": "mentoria-dados",
    "retries": 0,
}

with DAG(
    dag_id="gerador_logs_pipeline",
    description="Pipeline saudável (extract/transform/load) — gera logs INFO/WARNING continuamente",
    default_args=default_args,
    schedule=timedelta(minutes=2),
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["observabilidade", "aula", "gerador-log"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load", python_callable=load)

    t_extract >> t_transform >> t_load
