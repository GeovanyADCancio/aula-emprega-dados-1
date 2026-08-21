from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_tasks import load_bronze, load_silver, load_gold


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_bronze_silver_gold",
    description="Pipeline diário: bronze -> silver -> gold",
    start_date=datetime(2026, 8, 17),
    schedule_interval="0 0 * * *",   # cron: todo dia à meia-noite
    catchup=False,                   # não roda runs retroativas ao ligar a DAG
    default_args=default_args,
    tags=["marketing"],

) as dag:

    bronze = PythonOperator(task_id="bronze", python_callable=load_bronze)
    silver = PythonOperator(task_id="silver", python_callable=load_silver)
    gold = PythonOperator(task_id="gold", python_callable=load_gold)

    bronze >> silver >> gold