"""Main module."""

from pyspark.sql import SparkSession

from kafka_streaming_job.config import SerializationFormat
from kafka_streaming_job.sync.avro_sync import AvroSync
from kafka_streaming_job.sync.json_sync import JsonSync


def start_job(spark: SparkSession, config):
    job = None
    if config.kafka.serialization_format == SerializationFormat.JSON:
        job = JsonSync(spark, config)
    elif config.kafka.serialization_format == SerializationFormat.AVRO:
        job = AvroSync(spark, config)
    job.sync()

