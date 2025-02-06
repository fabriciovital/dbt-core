from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Configuração padrão da DAG
default_args = {
    'owner': 'Fabricio Vital',
    'depends_on_past': False,
    'retries': 1,
}

# Função reutilizável para criar tarefas DockerOperator
def run_container(task_id, image, command, container_name=None):
    return DockerOperator(
        task_id=task_id,
        image=image,
        container_name=container_name or task_id,  # Usa task_id como nome do contêiner se não for definido
        api_version='auto',
        auto_remove=True,
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="sparkanos",
        mount_tmp_dir=False,
        do_xcom_push=False
    )

# Definição da DAG
with DAG(
    dag_id='ingest_delta_small_tables',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['sparkanos']
) as dag:

    # Task: Ingestão de dados para parquet
    ingest_delta_small_tables = run_container(
        task_id='ingest_delta_small_tables',
        image='fabriciovital/data_engineering_stack:isp-performance',
        command="python /app/ingest_delta_small_tables.py"
    )