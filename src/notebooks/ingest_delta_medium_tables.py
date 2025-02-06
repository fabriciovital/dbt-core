import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do MinIO
HOST_ADDRESS = os.getenv('HOST_ADDRESS')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BRONZE_BUCKET = "s3a://bronze/isp_performance"

# Configurações do PostgreSQL
PG_URL = f"jdbc:postgresql://{os.getenv('HOST_ADDRESS')}:{os.getenv('DB_PORT_PROD')}/{os.getenv('DB_NAME_PROD')}"
PG_USER = os.getenv("DB_USER_PROD")
PG_PASSWORD = os.getenv("DB_PASS_PROD")
PG_SCHEMA = os.getenv("DB_SCHEMA_PROD")
PG_DRIVER = "org.postgresql.Driver"

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("MinIODeltaToPostgres") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.postgresql:postgresql:42.2.23,org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{HOST_ADDRESS}:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def read_delta_table(delta_path):
    """Ler tabela Delta da camada Bronze no MinIO"""
    try:
        logging.info(f"Lendo tabela Delta de {delta_path}")
        return spark.read.format("delta").load(delta_path)
    except AnalysisException as e:
        logging.error(f"Erro ao ler a tabela Delta em {delta_path}: {e}")
        return None

def write_to_postgres(df, table_name):
    """Gravar DataFrame no PostgreSQL"""
    if df:
        try:
            logging.info(f"Gravando tabela {table_name} no PostgreSQL...")
            df.write \
                .format("jdbc") \
                .option("url", PG_URL) \
                .option("dbtable", f"{PG_SCHEMA}.{table_name}") \
                .option("user", PG_USER) \
                .option("password", PG_PASSWORD) \
                .option("driver", PG_DRIVER) \
                .mode("overwrite") \
                .save()
            logging.info(f"Tabela {table_name} gravada com sucesso no PostgreSQL.")
        except Exception as e:
            logging.error(f"Erro ao gravar {table_name} no PostgreSQL: {e}")
    else:
        logging.warning(f"Erro: DataFrame para {table_name} está vazio.")

# Lista de tabelas a serem ingeridas
tabelas = [
    "bronze_ordem_servico_fechado",
]

# Loop para processar cada tabela
for tabela in tabelas:
    delta_path = f"{BRONZE_BUCKET}/{tabela}"
    df = read_delta_table(delta_path)
    if df is not None:
        write_to_postgres(df, tabela)
    else:
        logging.warning(f"Erro ao processar {tabela}, pulando...")

# Finalizar Spark
logging.info("Finalizando sessão Spark...")
spark.stop()