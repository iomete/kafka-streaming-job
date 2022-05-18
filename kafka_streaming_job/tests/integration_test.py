#!/usr/bin/env python

"""Tests for `kafka_streaming_job` package."""

from kafka_streaming_job.main import start_job
from kafka_streaming_job.config import get_config
from kafka_streaming_job.tests._spark_session import get_spark_session

# set in the application.conf
test_table_name = "tbl1"


def test_happy_path():
    # create test spark instance
    test_config = get_config("application.conf")
    spark = get_spark_session()

    # run target
    start_job(spark, test_config)

    # check
    df = spark.sql(f"select * from {test_table_name}")
    df.printSchema()
    assert df.count() > 0

