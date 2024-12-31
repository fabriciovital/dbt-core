from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Fabricio Vital',
    'depends_on_past': False,
    'retries': 1,
}

# Função para criar tarefas com DockerOperator
def run_container(dag, image, container_name, command):
    return DockerOperator(
        task_id=container_name,
        image=image,
        container_name=container_name,
        api_version='auto',
        auto_remove=True,
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="sparkanos",
        mount_tmp_dir=False,
        do_xcom_push=False,
        dag=dag
    )

# Definição da DAG
with DAG(
    'isp_performance_update',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['sparkanos']
) as dag:

    # Agrupamento das tarefas no TaskGroup
    with TaskGroup(group_id="isp_performance_update") as etl:

        # Task: Ingestão de dados para parquet
        ingestion_parquet_update = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_parquet_update',
            command=(
                # "spark-submit "
                # "--driver-memory 4g "
                # "--executor-memory 4g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/114_update_landing.py"
                 "python /app/114_update_landing.py"
            )
        )

        # Task: Ingestão de dados para bronze
        ingestion_bronze_update = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_bronze_update',
            command=(
                # "spark-submit "
                # "--driver-memory 4g "
                # "--executor-memory 4g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/115_update_bronze.py"
                 "python /app/115_update_bronze.py"
                
            )
        )

        # Task: Processamento para camada silver
        processing_silver_update = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='processing_silver_update',
            command=(
                # "spark-submit "
                # "--driver-memory 4g "
                # "--executor-memory 4g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/116_update_silver.py"
                 "python /app/116_update_silver.py"
            )
        )

    # Dependência entre as tarefas
    ingestion_parquet_update >> ingestion_bronze_update >> processing_silver_update

etl