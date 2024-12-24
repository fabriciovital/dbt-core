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

# Desabilitar a verificação de retenção de duração no Delta Lake
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting conversions from Minio to Minio Delta with full load...")

# Parâmetros de entrada e saída
input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

# Função para processar as tabelas com carga full
def process_table_full_load(table):
    table_name = F.convert_table_name(table)
    
    try:
        # Caminho da tabela destino (Bronze)
        delta_table_path = f'{storage_output}{output_prefix_layer_name}{table_name}'
        
        # Ler dados da camada de origem (Landing)
        input_path = f'{table_input_name}{input_prefix_layer_name}{table_name}'
        df_input_data = spark.read.format("parquet").load(input_path)
        df_input_data = df_input_data.repartition(100)
        df_with_metadata = F.add_metadata(df_input_data)
        
        # Adicionar coluna 'month_key' considerando `data_abertura` ou a data atual
        if 'data_abertura' in df_with_metadata.columns:
            df_with_month_key = df_with_metadata.withColumn(
                'month_key',
                concat(
                    year(coalesce(col('data_abertura').cast('date'), current_date())), 
                    lpad(month(coalesce(col('data_abertura').cast('date'), current_date())), 2, '0')
                )
            )
        else:
            df_with_month_key = df_with_metadata.withColumn(
                'month_key',
                concat(
                    year(current_date()),
                    lpad(month(current_date()), 2, '0')
                )
            )
        
        # Sobrescrever a tabela Delta com os dados completos
        df_with_month_key.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy('month_key') \
            .save(delta_table_path)
        
        # Limpar versões antigas imediatamente
        spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN 0 HOURS")
        logging.info(f"Old versions of Delta table '{table_name}' have been removed (VACUUM).")

    except Exception as e:
        logging.error(f'Erro ao processar a tabela {table_name}: {str(e)}')

# Lista de índices das tabelas que você deseja processar
table_indexes = [6]  # Exemplo, você pode alterar os índices conforme necessário

# Processar as tabelas selecionadas pelos índices
for index in table_indexes:
    table = configs.tables_api_isp_performance.get(list(configs.tables_api_isp_performance.keys())[index])
    process_table_full_load(table)

logging.info("Processamento concluído com carga full e limpeza imediata de versões anteriores!")