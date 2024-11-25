import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from pyspark.sql.functions import lit, coalesce, col, current_date
from pyspark.sql.functions import year, month, lpad, concat
from delta.tables import DeltaTable
from configs import configs
from functions import functions as F
from dotenv import load_dotenv
import os
import boto3
from pyspark.sql.utils import AnalysisException

# Carregar variáveis de ambiente
load_dotenv()
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

# Inicializar SparkSession com configurações Delta Lake e MinIO
spark = SparkSession.builder \
    .appName("el_landing_to_bronze_isp_performance") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("hive.metastore.uris", "thrift://metastore:9083") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting conversions from Minio to Minio Delta with merge logic...")

# Parâmetros de entrada e saída
input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

# Função para verificar a existência do arquivo Parquet
def file_exists(path):
    try:
        df = spark.read.format("parquet").load(path)
        return True
    except AnalysisException:
        return False

# Função para garantir que o diretório exista no MinIO
def ensure_s3_directory_exists(bucket_name, prefix):
    s3 = boto3.client('s3', endpoint_url=f'http://{HOST_ADDRESS}:9000', aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
    
    try:
        # Tentar listar os objetos no prefixo especificado (diretório)
        s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    except Exception as e:
        logging.error(f"Error checking directory in MinIO: {str(e)}")
        # Tentar criar um arquivo vazio para garantir que o diretório exista
        s3.put_object(Bucket=bucket_name, Key=f"{prefix}/_placeholder")

# Função para processar as tabelas com merge (insert, update, delete)
def process_table_with_merge(table):
    table_name = F.convert_table_name(table)
    
    try:
        # Verificar se o arquivo Parquet existe antes de tentar carregar
        input_path = f'{table_input_name}{input_prefix_layer_name}{table_name}'
        if not file_exists(input_path):
            logging.error(f"File does not exist: {input_path}")
            return  # Se o arquivo não existir, não faz nada e passa para a próxima tabela

        # Ler dados da camada de origem (Landing)
        df_input_data = spark.read.format("parquet").load(input_path)
        df_input_data = df_input_data.repartition(100)
        df_with_metadata = F.add_metadata(df_input_data)
        
        # Adicionar coluna 'month_key'
        if 'data_fechamento' in df_with_metadata.columns:
            df_with_month_key = df_with_metadata.withColumn(
                'month_key',
                concat(year(col('data_fechamento').cast('date')), lpad(month(col('data_fechamento').cast('date')), 2, '0'))
            )
        else:
            df_with_month_key = df_with_metadata.withColumn(
                'month_key',
                concat(year(current_date()), lpad(month(current_date()), 2, '0'))
            )
        
        # Caminho da tabela destino (Bronze)
        delta_table_path = f'{storage_output}{output_prefix_layer_name}{table_name}'
        
        # Verificar se o diretório de destino existe e criar se necessário
        bucket_name = "bronze"  # Substitua pelo nome correto do seu bucket
        ensure_s3_directory_exists(bucket_name, f'{output_prefix_layer_name}{table_name}')
        
        # Verificar se a tabela Delta já existe
        if DeltaTable.isDeltaTable(spark, delta_table_path):
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            
            # Aplicar o merge (insert, update, delete)
            delta_table.alias("target").merge(
                source=df_with_month_key.alias("source"),
                condition="target.id = source.id"  # Substitua pela chave correta
            ).whenMatchedUpdateAll(
                condition="source.last_update > target.last_update"  # Atualiza apenas registros mais recentes
            ).whenNotMatchedInsertAll(
            ).execute()

            # Atualizar a tabela Delta com o comando REFRESH
            spark.sql(f"REFRESH TABLE delta.`{delta_table_path}`")
            
            # Obter e registrar operações realizadas
            last_operation = delta_table.history(1).select("operationMetrics").collect()[0][0]
            num_inserted = last_operation.get("numInsertedRows", 0)
            num_updated = last_operation.get("numUpdatedRows", 0)
            num_deleted = last_operation.get("numDeletedRows", 0)
            logging.info(f'Table {table_name}: Inserts={num_inserted}, Updates={num_updated}, Deletes={num_deleted}')
        else:
            # Se a tabela não existir, criar a partir do zero
            df_with_month_key.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy('month_key') \
                .save(delta_table_path)

            # Atualizar a tabela Delta com o comando REFRESH
            spark.sql(f"REFRESH TABLE delta.`{delta_table_path}`")
            
            logging.info(f'Table {table_name}: Created new Delta table.')
        
        logging.info(f'Table {table_name} successfully processed with merge logic and saved to Minio: {delta_table_path}')
    except Exception as e:
        logging.error(f'Error processing table {table_name}: {str(e)}')

# Processar todas as tabelas configuradas
for key, value in configs.tables_api_isp_performance_produtividade.items():
    process_table_with_merge(value)

logging.info("Conversion from parquet to Delta with merge logic completed successfully!")