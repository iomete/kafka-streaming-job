import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

from kafka_streaming_job.sync._data_sync import DataSync
from kafka_streaming_job.iometeLogger import iometeLogger


class AvroSync(DataSync):
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.schema = self.retrieve_schema(self.config.kafka.schema_registry_url,
                                           self.config.kafka.topic_name)
        self.logger = iometeLogger(__name__).get_logger()

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
                    schema=self.config.database.schema,
                    table=self.config.database.table,
                    topic_name=self.config.kafka.topic_name)
            ) \
            .trigger(processingTime=self.config.kafka.processing_time) \
            .start() \
            .awaitTermination()

    @staticmethod
    def foreach_batch_sync(
            df, epoch_id, logger,
            schema, table, topic_name):
        """
        Responsible for processing micro batches for every batch processing.;

        :param df: Batch dataframe to be written.;
        :param epoch_id: Micro batch epoch id.;
        :param schema: Database schema name;
        :param table: Database table name;
        :param topic_name: Kafka topic name;
        :param logger: Logger
        """
        logger.debug(f"epoc_id = {epoch_id} start for table = {table}")
        if not df.rdd.isEmpty():
            try:
                df.write.saveAsTable(
                    AvroSync.complete_db_destination(schema, table),
                    format='iceberg',
                    mode='append'
                )
            except Exception as e:
                logger.error(f"error stream processing for table = {table}, "
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

    @staticmethod
    def complete_db_destination(schema, table):
        return "{}.{}".format(schema, table)
