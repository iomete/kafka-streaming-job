#!/usr/bin/env python

"""Tests for `kafka_streaming_job` package."""
from time import sleep

from kafka_streaming_job.main import start_job
from kafka_streaming_job.config import get_config
from kafka_streaming_job.tests._spark_session import get_spark_session
from kafka_streaming_job.tests.producer.kafka_producer import kafka_avro_producer, kafka_json_producer


def test_json_sync():
    # create test spark instance
    test_config = get_config("tests/application-json.conf")
    spark = get_spark_session()
    kafka_json_producer()

    # run target
    start_job(spark, test_config)

    # check
    df = spark.sql(f"select * from {test_config.database.table_name}")
    df.printSchema()
    assert df.count() > 0


def test_avro_sync():
    # create test spark instance
    test_config = get_config("tests/application-avro.conf")
    spark = get_spark_session()
    kafka_avro_producer()
    sleep(10)
    # run target
    start_job(spark, test_config)

    # check
    df = spark.sql(f"select * from {test_config.database.table_name}")
    df.printSchema()
    assert df.count() > 0
