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
    'isp_performance_produtividade',
    default_args=default_args,
    start_date=datetime(2024, 11, 4),  # Use a fixed start date
    schedule_interval='*/60 * * * *',  # Executa a cada 2 horas
    catchup=False,  # Adiciona este parâmetro para evitar a execução de tarefas passadas
    max_active_runs=1,  # Limita a DAG para uma execução ativa por vez
    tags=['sparkanos']
) as dag:
    
    with TaskGroup(group_id="isp_performance_produtividade") as etl:

        ingestion_parquet_produtividade = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_parquet_produtividade',
            command="spark-submit --driver-memory 2g --executor-memory 2g /app/106_insert_landing_produtividade.py"
        )

        ingestion_bronze_produtividade = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='ingestion_bronze_produtividade',
            command="spark-submit --driver-memory 2g --executor-memory 2g /app/115_update_bronze_produtividade.py"
        )

        processing_silver_produtividade = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='processing_silver_produtividade',
            command="spark-submit --driver-memory 2g --executor-memory 2g /app/116_update_silver_produtividade.py"
        )

        refinement_gold_produtividade = run_container(
            dag=dag,
            image='fabriciovital/data_engineering_stack:isp-performance',
            container_name='refinement_gold_produtividade',
            command="spark-submit --driver-memory 2g --executor-memory 2g /app/109_insert_gold_produtividade.py"
        )

    ingestion_parquet_produtividade >> ingestion_bronze_produtividade >> processing_silver_produtividade >> refinement_gold_produtividade

etl