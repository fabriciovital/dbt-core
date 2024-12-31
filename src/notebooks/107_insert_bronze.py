import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from pyspark.sql.functions import lit, coalesce, col, current_date
from pyspark.sql.functions import year, month, lpad, concat
from configs import configs
from functions import functions as F
from dotenv import load_dotenv
import os

load_dotenv()
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

spark = SparkSession.builder \
    .appName("el_landing_to_bronze_isp_performance") \
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
logging.info("Starting conversions from Minio to Minio Delta...")

input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

def process_table(table):
    table_name = F.convert_table_name(table)
    
    try:
        
        # Caminho da tabela destino (Bronze)
        delta_table_path = f'{storage_output}{output_prefix_layer_name}{table_name}'
        
        df_input_data = spark.read.format("parquet").load(f'{table_input_name}{input_prefix_layer_name}{table_name}')        
        #df_input_data = df_input_data.repartition(1)        
        df_with_update_date = F.add_metadata(df_input_data)
        
        if 'data_abertura' in df_with_update_date.columns:
            df_with_month_key = df_with_update_date.withColumn(
                'month_key',
                concat(year(col('data_abertura').cast('date')), lpad(month(col('data_abertura').cast('date')), 2, '0'))
            )
        else:
            df_with_month_key = df_with_update_date.withColumn(
                'month_key',
                concat(year(current_date()), lpad(month(current_date()), 2, '0'))
            )
            
        df_with_month_key.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy('month_key').save(delta_table_path)
        logging.info(f'Table {table_name} successfully processed and saved to Minio: {storage_output}{output_prefix_layer_name}{table_name}')
        
        # Limpar versões antigas imediatamente
        spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN 0 HOURS")
        logging.info(f"Old versions of Delta table '{table_name}' have been removed (VACUUM).")
        
    except Exception as e:
        logging.error(f'Error processing table {table_name}: {str(e)}')

# Lista de índices das tabelas que você deseja processar
table_indexes = [0, 1, 2, 3, 4, 5, 7, 8]  # Exemplo, você pode alterar os índices conforme necessário

# Processar apenas as tabelas com os índices especificados
for index in table_indexes:
    if index < len(configs.tables_api_isp_performance):
        table_name = list(configs.tables_api_isp_performance.values())[index]
        process_table(table_name)
    else:
        logging.error(f"Índice {index} fora do alcance das tabelas.")

logging.info("Conversion from parquet to Delta completed successfully!")