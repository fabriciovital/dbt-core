from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 6),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_run_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # 🔥 Roda a cada 5 minutos
    catchup=False,  # 🔥 Evita rodar execuções acumuladas
    max_active_runs=1,  # 🔥 Garante que apenas uma instância roda por vez
) as dag:

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command="docker exec dbt-core dbt run --profiles-dir /northwind"
    )

    dbt_run