import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

from kafka_streaming_job.sync._data_sync import DataSync
from kafka_streaming_job.utils import PySparkLogger


class AvroSync(DataSync):
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config

        self.schema = self.retrieve_schema(self.config.kafka.schema_registry_url,
                                           self.config.kafka.topic_name)

        self.spark.sparkContext.setLogLevel(os.getenv("LOG_LEVEL"))
        self.logger = PySparkLogger(spark).get_logger(__name__)
        self.logger.info("pyspark script logger initialized")

    def sync(self):
        self.logger.info(f"avro data sync started for topic = {self.config.kafka.topic_name}")
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.config.kafka.topic_name) \
            .option("startingOffsets", self.config.kafka.starting_offsets) \
            .load() \
            .selectExpr("substring(value, 6) as value") \
            .select(from_avro(col("value"), self.schema).alias("value"))

        df.writeStream \
            .foreachBatch(
                lambda df, epoch_id:
                self.foreach_batch_sync(
                    df=df, epoch_id=epoch_id, logger=self.logger,
                    table_name=self.config.database.table_name,
                    topic_name=self.config.kafka.topic_name)
            ) \
            .trigger(processingTime=self.config.kafka.processing_time) \
            .start() \
            .awaitTermination()

    @staticmethod
    def foreach_batch_sync(
            df, epoch_id, logger,
            table_name, topic_name):
        """
        Responsible for processing micro batches for every batch processing.;

        :param df: Batch dataframe to be written.;
        :param epoch_id: Micro batch epoch id.;
        :param table_name: Database table name;
        :param topic_name: Kafka topic name;
        :param logger: Logger
        """
        logger.debug(f"foreach_batch_sync epoc_id = {epoch_id} "
                     f"start for table = {table_name}")
        if not df.rdd.isEmpty():
            try:
                df.write.saveAsTable(table_name,
                                     format='iceberg',
                                     mode='append')
            except Exception as e:
                logger.error(f"error stream processing for table = {table_name}, "
                             f"topic = {topic_name}")
                logger.error(e)
        else:
            logger.debug(f"kafka topic = {topic_name} is empty")

    @staticmethod
    def retrieve_schema(schema_registry_url, topic_name):
        """
        Retrieve avro schema from schema registry.

        :param schema_registry_url: Schema registry url
        :param topic_name: Kafka topic name
        :return: Avro schema
        """
        response = requests.get(
            '{}/subjects/{}-value/versions/latest/schema'.format(schema_registry_url,
                                                                 topic_name))
        # error check
        response.raise_for_status()
        # extract the schema from the response
        schema = response.text
        return schema
