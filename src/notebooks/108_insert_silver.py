import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from configs import configs
from functions import functions as F
from dotenv import load_dotenv
import os

load_dotenv()
HOST_ADDRESS=os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY=os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY=os.getenv('MINIO_SECRET_KEY')

# Lista de índices dos itens que você deseja processar (exemplo até o item 6)
indices_para_processar = [0, 1, 2, 3, 4, 5, 7, 8, 9]  # Isso corresponde às tabelas que serão processadas

def process_table(spark, query_input, output_path):
    try:
        df_input_data = spark.sql(query_input)
        df_with_update_date = F.add_metadata(df_input_data)
        #df_with_update_date = df_with_update_date.repartition(100)
        df_with_update_date.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(output_path)
        
        logging.info(f"query '{query_input}' successfully processed and saved to {output_path}")
        
        # Limpar versões antigas imediatamente
        spark.sql(f"VACUUM delta.`{output_path}` RETAIN 0 HOURS")
        logging.info(f"Old versions of Delta table have been removed (VACUUM).")
        
    except Exception as e:
        logging.error(f"Error processing query '{query_input}': {str(e)}")

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
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
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
    # Itera sobre os índices definidos e processa apenas as tabelas correspondentes
    for idx in indices_para_processar:
        # Pegando a tabela e a query correspondentes pelo índice
        table_name = list(configs.tables_silver.keys())[idx]
        query_input = configs.tables_silver[table_name]

        table_name = F.convert_table_name(table_name)
        
        query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_silver)        
        
        storage_output = f'{output_path}{output_prefix_layer_name}{table_name}'
        
        process_table(spark, query_input, storage_output)
        
    logging.info("Process to silver completed!")
    
except Exception as e:
    logging.error(f'Error processing table: {str(e)}')