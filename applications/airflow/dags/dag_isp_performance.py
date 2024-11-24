from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Fabricio Vital',
    'depends_on_past': False,
}

# Definição da função run_container
def run_container(dag, image, container_name, command):
    runner = DockerOperator(
        task_id=container_name,
        image=image,
        container_name=container_name,
        api_version='auto',
        auto_remove=True,
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="sparkanos",
        mount_tmp_dir=False,  # Disable mounting the temporary directory
        dag=dag  # Passando a referência da DAG para o operador
    )
    return runner

# Definição da DAG
with DAG(
    'isp_performance',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),  # Use a fixed start date
    schedule_interval='*/5 * * * *',  # Executa a cada 5 minutos
    catchup=False,  # Adiciona este parâmetro para evitar a execução de tarefas passadas
    max_active_runs=1,  # Limita a DAG para uma execução ativa por vez
    tags=['sparkanos']
) as dag:
    
    with TaskGroup(group_id="isp_performance") as etl:

        ingestion_parquet = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_parquet',
            command="spark-submit --driver-memory 1g --executor-memory 1g /app/106_insert_landing.py"
        )

        ingestion_bronze = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_bronze',
            command="spark-submit --driver-memory 1g --executor-memory 1g /app/115_update_bronze.py"
        )

        processing_silver = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='processing_silver',
            command="spark-submit --driver-memory 1g --executor-memory 1g /app/116_update_silver.py"
        )

        refinement_gold = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='refinement_gold',
            command="spark-submit --driver-memory 1g --executor-memory 1g /app/109_insert_gold.py"
        )

    ingestion_parquet >> ingestion_bronze >> processing_silver >> refinement_gold

etl