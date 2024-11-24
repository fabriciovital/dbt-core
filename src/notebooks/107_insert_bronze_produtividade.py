import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, coalesce, col, current_date
import logging
import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from configs import configs
from functions import functions as F

# Carregar variáveis de ambiente
load_dotenv()

# Variáveis do MinIO
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# Configurar sessão do Spark
spark = SparkSession.builder \
    .appName("full_refresh_to_bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting full refresh...")

# Caminhos e prefixos de entrada e saída
input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

# Configurar cliente do MinIO
minio_client = Minio(
    f"{HOST_ADDRESS}:9000",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # Use False para HTTP e True para HTTPS
)

def delete_minio_folder(bucket_name, folder_name):
    try:
        # Listar os objetos no diretório
        objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
        for obj in objects:
            # Excluir os objetos um por um
            minio_client.remove_object(bucket_name, obj.object_name)
            logging.info(f"Deleted object: {obj.object_name}")
    except S3Error as e:
        logging.error(f"Error deleting objects from MinIO: {str(e)}")

from pyspark.sql.functions import year, month, lpad, concat

def process_table(table):
    table_name = F.convert_table_name(table)
    output_path = f'{storage_output}{output_prefix_layer_name}{table_name}'
    
    try:
        # Limpar o diretório de destino no MinIO
        logging.info(f'Clearing the output path: {output_path}')
        delete_minio_folder('bronze', f'isp_performance/{output_prefix_layer_name}{table_name}')

        # Carregar os dados da tabela de entrada
        df_input_data = spark.read.format("parquet").load(f'{table_input_name}{input_prefix_layer_name}{table_name}')
        
        # Reparticionar para otimizar a escrita
        df_input_data = df_input_data.repartition(100)
        
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
        
        # Salvar os dados em modo overwrite com particionamento
        df_with_month_key.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy('month_key') \
            .save(output_path)
        
        logging.info(f'Table {table_name} successfully processed and fully overwritten at: {output_path}')
    
    except Exception as e:
        logging.error(f'Error processing table {table_name}: {str(e)}')

# Processar todas as tabelas configuradas
for key, value in configs.tables_api_isp_performance_produtividade.items():
    process_table(value)

logging.info("Full refresh completed successfully!")