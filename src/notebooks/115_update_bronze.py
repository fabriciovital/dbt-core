import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, coalesce, col, current_date, year, month, lpad, concat
import logging
import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from delta.tables import DeltaTable
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
    .appName("incremental_update_to_bronze") \
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
logging.info("Starting incremental update...")

# Caminhos e prefixos de entrada e saída
input_prefix_layer_name = configs.prefix_layer_name['0']
table_input_name = configs.lake_path['landing']
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']

def process_table(table):
    table_name = F.convert_table_name(table)
    output_path = f'{storage_output}{output_prefix_layer_name}{table_name}'
    
    try:
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
        
        # Se a tabela Delta já existe, realizar merge (insert, update e delete)
        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            
            # Realizar merge para inserir, atualizar e excluir registros
            delta_table.alias("target").merge(
                df_with_month_key.alias("source"),
                "target.id = source.id"  # Substitua 'id' pela coluna chave da sua tabela
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

            logging.info(f"Table {table_name} successfully merged with new data at: {output_path}")
        
        else:
            # Se a tabela não existe, crie uma nova tabela Delta
            df_with_month_key.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy('month_key') \
                .save(output_path)

            logging.info(f"Table {table_name} created and data inserted at: {output_path}")
    
    except Exception as e:
        logging.error(f'Error processing table {table_name}: {str(e)}')

# Processar todas as tabelas configuradas
for key, value in configs.tables_api_isp_performance.items():
    process_table(value)

logging.info("Incremental update completed successfully!")