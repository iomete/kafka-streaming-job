#!/usr/bin/env python

"""Tests for `kafka_streaming_job` package."""
from time import sleep

from kafka_streaming_job.config import get_config
from kafka_streaming_job.sync.avro_sync import AvroSync
from kafka_streaming_job.sync.json_sync import JsonSync
from kafka_streaming_job.tests._spark_session import get_spark_session
from kafka_streaming_job.tests.producer.kafka_producer import kafka_avro_producer, kafka_json_producer


def test_json_sync():
    # create test spark instance
    test_config = get_config("application-json.conf")
    spark = get_spark_session()
    kafka_json_producer()
    sleep(10)
    # run target
    job = JsonSync(spark, test_config)
    job.sync()
    job.streaming_query.awaitTermination(2000)

    # check
    # job.stop()
    df = spark.sql(f"select * from {test_config.database.schema}.{test_config.database.table}")
    df.printSchema()
    assert df.count() > 0


def test_avro_sync():
    # create test spark instance
    test_config = get_config("application-avro.conf")
    spark = get_spark_session()
    kafka_avro_producer()
    sleep(10)
    # run target
    job = AvroSync(spark, test_config)
    job.sync()

    # check
    job.streaming_query.awaitTermination(2000)
    df = spark.sql(f"select * from {test_config.database.schema}.{test_config.database.table}")
    df.printSchema()
    assert df.count() > 0


if __name__ == '__main__':
    test_avro_sync()
