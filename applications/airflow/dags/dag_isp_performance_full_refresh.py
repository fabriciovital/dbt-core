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
    'isp_performance_full_refresh',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['sparkanos']
) as dag:

    # Agrupamento das tarefas no TaskGroup
    with TaskGroup(group_id="isp_performance_full_refresh") as etl:

        # Task: Ingestão de dados para parquet
        ingestion_parquet_full_refresh = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_parquet_full_refresh',
            command=(
                # "spark-submit "
                # "--driver-memory 2g "
                # "--executor-memory 2g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/106_insert_landing.py"
                "python /app/106_insert_landing.py"
            )
        )

        # Task: Ingestão de dados para bronze
        ingestion_bronze_full_refresh = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_bronze_full_refresh',
            command=(
                # "spark-submit "
                # "--driver-memory 6g "
                # "--executor-memory 6g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/107_insert_bronze.py"
                "python /app/107_insert_bronze.py"
            )
        )

                # Task: Processamento para camada silver
        processing_silver_full_refresh = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='processing_silver_full_refresh',
            command=(
                # "spark-submit "
                # "--driver-memory 4g "
                # "--executor-memory 4g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/108_insert_silver.py"
                "python /app/108_insert_silver.py"
            )
        )

        # Task: Refinamento para camada gold
        refinement_gold_full_refresh = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='refinement_gold_full_refresh',
            command=(
                # "spark-submit "
                # "--driver-memory 4g "
                # "--executor-memory 4g "
                # "--conf spark.io.compression.codec=lz4 "
                # "/app/109_insert_gold.py"
                "python /app/109_insert_gold.py"
            )
        )

    # Dependência entre as tarefas
    ingestion_parquet_full_refresh >> ingestion_bronze_full_refresh >> processing_silver_full_refresh >> refinement_gold_full_refresh

etl