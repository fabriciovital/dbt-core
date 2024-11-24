import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime
from configs import configs
from functions import functions as F
from dotenv import load_dotenv
import os
from minio import Minio
from delta.tables import DeltaTable
from minio.error import S3Error

# Carregar variáveis de ambiente
load_dotenv()

HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

def merge_data(spark, df_input_data, output_path, primary_key):
    try:
        # Adicionar metadados aos dados
        df_with_update_date = F.add_metadata(df_input_data)

        # Verificar se a tabela Delta já existe no caminho de destino
        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)

            # Realizar merge para inserir novos registros e atualizar os existentes
            delta_table.alias("target").merge(
                df_with_month_key.alias("source"),
                "target.id = source.id AND (target.data_fechamento < source.data_fechamento OR target.id IS NULL)"  # Filtra registros novos ou atualizados
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

            logging.info(f"Data merged successfully into {output_path}")
        else:
            # Se a tabela Delta não existe, cria a partir dos dados de entrada
            df_with_update_date.write \
                .format("delta") \
                .option("mergeSchema", "true") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy('month_key') \
                .save(output_path)
            logging.info(f"New Delta table created at {output_path}")

    except Exception as e:
        logging.error(f"Error during merge operation at '{output_path}': {str(e)}")

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("process_bronze_to_gold_isp_performance") \
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

    logging.info("Starting processing from bronze to gold...")

    input_prefix_layer_name = configs.prefix_layer_name['2']  # silver layer
    input_path = configs.lake_path['silver']

    output_prefix_layer_name = configs.prefix_layer_name['3']  # gold layer
    output_path = configs.lake_path['gold']

    try:
        for table_name, query_input in configs.tables_gold_produtividade.items():
            table_name = F.convert_table_name(table_name)
            query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_gold_produtividade)        

            storage_output = f'{output_path}{output_prefix_layer_name}{table_name}'
            
            # Processar dados e realizar merge
            df_input_data = spark.sql(query_input)
            merge_data(spark, df_input_data, storage_output)  # Ajuste 'id' para a chave primária específica de cada tabela
            
        logging.info("Process to gold completed!")

    except Exception as e:
        logging.error(f'Error processing table: {str(e)}')