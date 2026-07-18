"""
DAG geradora de logs #2 — simulador de instabilidade

Task com ~40% de chance de falhar, retries=3 e retry_delay curto (10s) só
pra fins de aula — em produção nunca use retry_delay tão agressivo. O
objetivo é gerar ERROR + "UP_FOR_RETRY" no log real do Airflow rapidamente,
pra demonstrar consulta/alerta de taxa de erro no Grafana.
"""
from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

PROBABILIDADE_FALHA = 0.4


def chamar_api_instavel(**context):
    log.info("Chamando API externa 'estoque-fornecedor' (simulada)")

    if random.random() < PROBABILIDADE_FALHA:
        log.error(
            "Falha ao chamar API externa: timeout após 30s (connection reset by peer)"
        )
        raise AirflowException("Timeout na chamada à API externa 'estoque-fornecedor'")

    log.info("Resposta recebida com sucesso, 200 OK")


def processar_resposta(**context):
    log.info("Processando payload da API de estoque")
    log.info("Processamento concluído")


default_args = {
    "owner": "mentoria-dados",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="gerador_logs_falhas",
    description="Task instável de propósito — gera ERROR e retries pra demo de alertas no Grafana",
    default_args=default_args,
    schedule=timedelta(minutes=3),
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["observabilidade", "aula", "gerador-log"],
) as dag:

    t_chamar_api = PythonOperator(
        task_id="chamar_api_instavel", python_callable=chamar_api_instavel
    )
    t_processar = PythonOperator(
        task_id="processar_resposta", python_callable=processar_resposta
    )

    t_chamar_api >> t_processar
