import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from pyspark.sql.functions import lit, coalesce, col, current_date
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Starting conversions from Minio to Minio Delta...")

input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

from pyspark.sql.functions import year, month, lpad, concat

def process_table(table):
    table_name = F.convert_table_name(table)
    try:
        # Carregar os dados da tabela de entrada
        df_input_data = spark.read.format("parquet").load(f'{table_input_name}{input_prefix_layer_name}{table_name}')
        
        # Reparticionar os dados para melhorar o desempenho de escrita
        df_input_data = df_input_data.repartition(200)  # Ajuste o número de partições conforme necessário
        
        # Adicionar metadados ao DataFrame
        df_with_update_date = F.add_metadata(df_input_data)

        # Verificar a existência da coluna 'data_abertura' e criar 'month_key'
        if 'data_abertura' in df_with_update_date.columns:
            df_with_month_key = df_with_update_date.withColumn(
                'month_key',
                concat(year(col('data_abertura').cast('date')), lpad(month(col('data_abertura').cast('date')), 2, '0'))
            )
        else:
            # Se 'data_abertura' não existe, usa o ano e mês atuais como 'month_key'
            df_with_month_key = df_with_update_date.withColumn(
                'month_key',
                concat(year(current_date()), lpad(month(current_date()), 2, '0'))
            )

        # Salvar os dados em um arquivo Delta no MinIO
        df_with_month_key.write.format("delta").mode("overwrite").partitionBy('month_key').save(f'{storage_output}{output_prefix_layer_name}{table_name}')
        
        logging.info(f'Table {table_name} successfully processed and saved to Minio: {storage_output}{output_prefix_layer_name}{table_name}')
    
    except Exception as e:
        logging.error(f'Error processing table {table_name}: {str(e)}')

# Processar cada tabela especificada em 'configs.tables_api_isp_performance'
for key, value in configs.tables_api_isp_performance.items():
    process_table(value)

logging.info("Conversion from parquet to Delta completed successfully!")