from pyspark.sql import SparkSession

from kafka_streaming_job.main import start_job
from kafka_streaming_job.config import get_config

production_config = get_config("/etc/configs/application.conf")

spark = SparkSession.builder \
    .appName("IntegrationTest") \
    .master("local[*]") \
    .getOrCreate()

start_job(spark, production_config)
