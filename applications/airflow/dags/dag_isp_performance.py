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
    'isp_performance',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['sparkanos']
) as dag:

    # Agrupamento das tarefas no TaskGroup
    with TaskGroup(group_id="isp_performance") as etl:

        # Task: Ingestão de dados para parquet
        ingestion_parquet = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_parquet',
            command=(
                "spark-submit "
                "--driver-memory 1g "
                "--executor-memory 1g "
                "--num-executors 2 "
                "--conf spark.io.compression.codec=lz4 "
                "/app/114_update_landing.py"
            )
        )

        # Task: Ingestão de dados para bronze
        ingestion_bronze = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_bronze',
            command=(
                "spark-submit "
                "--driver-memory 1g "
                "--executor-memory 1g "
                "--num-executors 2 "
                "--conf spark.io.compression.codec=lz4 "
                "/app/115_update_bronze.py"
            )
        )

        # Task: Processamento para camada silver
        processing_silver = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='processing_silver',
            command=(
                "spark-submit "
                "--driver-memory 1g "
                "--executor-memory 1g "
                "--num-executors 2 "
                "--conf spark.io.compression.codec=lz4 "
                "/app/116_update_silver.py"
            )
        )

        # Task: Refinamento para camada gold
        refinement_gold = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='refinement_gold',
            command=(
                "spark-submit "
                "--driver-memory 1g "
                "--executor-memory 1g "
                "--num-executors 2 "
                "--conf spark.io.compression.codec=lz4 "
                "/app/117_update_gold.py"
            )
        )

        # Tarefa de limpeza de volumes não utilizados
        clean_volumes_task = PythonOperator(
            task_id="clean_unused_volumes",
            python_callable=clean_unused_volumes,
            dag=dag,
        )

    # Dependência entre as tarefas
    ingestion_parquet >> ingestion_bronze >> processing_silver >> refinement_gold

etl