from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf()

conf.setAppName("Spark minIO Test")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://172.21.121.140:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "admin")
conf.set("spark.hadoop.fs.s3a.secret.key", "8Ggy2k7tugeKWy@")
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
conf.set("hive.metastore.uris", "thrift://metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()