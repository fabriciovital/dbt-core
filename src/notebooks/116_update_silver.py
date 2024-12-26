import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from configs import configs
from functions import functions as F
from dotenv import load_dotenv
import os
from delta.tables import DeltaTable

# Carregar variáveis de ambiente
load_dotenv()

# Variáveis do MinIO
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

def process_table(spark, query_input, output_path, table_name):
    try:
        # Registrar hora de início
        start_time = datetime.now()
        logging.info(f'Starting process for {table_name} at {start_time}')
        
        # Consultar dados da tabela de entrada
        df_input_data = spark.sql(query_input)
        df_with_update_date = F.add_metadata(df_input_data)

        # Verificar se a tabela Delta já existe no output path
        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)

            # Condição de união para identificar registros para operações de merge
            merge_condition = "target.id = source.id"  # Substitua 'id' pela chave primária da tabela
            
            # Aplicar merge: `update`, `insert`, e `delete` com base em critérios
            delta_table.alias("target").merge(
                df_with_update_date.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll(
                condition="source.last_update > target.last_update"  # Atualizar apenas registros mais recentes
            ).whenNotMatchedInsertAll(
            ).execute()

            logging.info(f"Table {table_name} processed with merge logic for inserts, updates, and deletes.")
        
        else:
            # Se a tabela não existe, crie uma nova tabela Delta e realize um insert inicial
            df_with_update_date.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy('month_key') \
                .save(output_path)

            logging.info(f"{table_name} - Created new table with initial insert.")

        # Limpar versões antigas imediatamente
        spark.sql(f"VACUUM delta.`{output_path}` RETAIN 0 HOURS")
        logging.info(f"Old versions of Delta table '{table_name}' have been removed (VACUUM).")

        # Registrar hora de término
        end_time = datetime.now()
        logging.info(f'Completed process for {table_name} at {end_time} - Duration: {end_time - start_time}')

    except Exception as e:
        logging.error(f"Error processing table {table_name}: {str(e)}")

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("process_bronze_to_silver_isp_performance") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("hive.metastore.uris", "thrift://metastore:9083") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()
    
    # Configurações específicas do Delta Lake
    spark.conf.set("spark.delta.logRetentionDuration", "interval 1 day")  # Manter logs por 1 dia
    spark.conf.set("spark.delta.deletedFileRetentionDuration", "interval 1 day")  # Manter arquivos deletados por 1 dia
    
    # Desabilitar a verificação de retenção de duração no Delta Lake
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Starting processing from bronze to silver...")

    input_prefix_layer_name = configs.prefix_layer_name['1']  # bronze layer
    input_path = configs.lake_path['bronze']

    output_prefix_layer_name = configs.prefix_layer_name['2']  # silver layer
    output_path = configs.lake_path['silver']

    try:
        # Lista de índices manual dos itens a serem processados (até o item 6)
        indices_to_process = [6]  # Exemplo: processar os primeiros 6 itens

        # Iterar apenas pelos índices desejados
        for i in indices_to_process:
            # Obter a tabela de acordo com o índice
            table_name, query_input = list(configs.tables_silver.items())[i]
            table_name = F.convert_table_name(table_name)
            query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_silver)        
            storage_output = f'{output_path}{output_prefix_layer_name}{table_name}'

            # Processar a tabela
            process_table(spark, query_input, storage_output, table_name)
        
        logging.info("Process to silver completed!")
    
    except Exception as e:
        logging.error(f'Error processing table: {str(e)}')