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

def process_table(spark, df_input_data, output_path, primary_key=None):
    """
    Processa uma tabela com merge (se chave primária fornecida) ou sobrescrita direta (caso contrário).
    """
    try:
        # Adicionar metadados
        df_with_update_date = F.add_metadata(df_input_data)

        if primary_key:
            # Realizar o merge para tabelas com chave primária
            if DeltaTable.isDeltaTable(spark, output_path):
                delta_table = DeltaTable.forPath(spark, output_path)
                delta_table.alias("target").merge(
                    df_with_update_date.alias("source"),
                    f"target.{primary_key} = source.{primary_key}"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
                logging.info(f"Data merged successfully into {output_path}")
            else:
                # Criar nova tabela Delta
                df_with_update_date.write \
                    .format("delta") \
                    .option("mergeSchema", "true") \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite") \
                    .partitionBy('month_key') \
                    .save(output_path)
                logging.info(f"New Delta table created at {output_path}")
        else:
            # Sobrescrever diretamente para tabelas sem chave primária
            df_with_update_date.write \
                .format("delta") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .partitionBy('month_key') \
                .save(output_path)
            logging.info(f"Table overwritten successfully at {output_path}")

        # Aplicar VACUUM após operação para remover versões antigas
        if DeltaTable.isDeltaTable(spark, output_path):
            logging.info(f"Applying VACUUM on table at {output_path}")
            spark.sql(f"VACUUM '{output_path}' RETAIN 0 HOURS")
            logging.info(f"VACUUM completed for {output_path}")
            
    except Exception as e:
        logging.error(f"Error processing table at '{output_path}': {str(e)}")

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("process_silver_to_gold_isp_performance") \
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

    logging.info("Starting processing from silver to gold...")

    input_prefix_layer_name = configs.prefix_layer_name['2']  # silver layer
    input_path = configs.lake_path['silver']

    output_prefix_layer_name = configs.prefix_layer_name['3']  # gold layer
    output_path = configs.lake_path['gold']

    try:
        for table_name, query_input in configs.tables_gold.items():
            table_name = F.convert_table_name(table_name)
            query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_gold)
            
            storage_output = f'{output_path}{output_prefix_layer_name}{table_name}'
            
            # Carregar dados da tabela
            df_input_data = spark.sql(query_input)
            
            # Verificar se a tabela possui a coluna "id"
            if "id" in df_input_data.columns:
                process_table(spark, df_input_data, storage_output, primary_key="id")
            else:
                process_table(spark, df_input_data, storage_output)  # Sobrescreve sem merge
            
        logging.info("Process to gold completed!")

    except Exception as e:
        logging.error(f'Error processing table: {str(e)}')