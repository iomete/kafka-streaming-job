import logging
from pyspark.sql import SparkSession

from kafka_streaming_job.sync._data_sync import DataSync
from pyspark.sql.functions import schema_of_json, from_json, col

logger = logging.getLogger(__name__)


class JsonSync(DataSync):
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.streaming_query = None

    def sync(self):
        logger.info(f"json data sync started for topic = {self.config.kafka.topic}")
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.config.kafka.topic) \
            .option("startingOffsets", self.config.kafka.starting_offsets) \
            .option("group.id", self.config.kafka.group_id) \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        self.streaming_query = df.writeStream \
            .foreachBatch(self.foreach_batch_sync) \
            .trigger(processingTime=self.config.kafka.processing_time) \
            .option("checkpointLocation", self.config.checkpoint_location) \
            .start() \
            .awaitTermination()

    def foreach_batch_sync(self, df, epoch_id):
        """
        Responsible for processing micro batches for every batch processing.

        :param df: Batch dataframe to be written.
        :param epoch_id: Micro batch epoch id.
        """
        logger.debug(f"epoc_id = {epoch_id} start for table = {self.config.database.table}")
        if not df.rdd.isEmpty():
            try:
                parsedDF = self.bytes_to_catalyst(df).select("value.*")

                parsedDF.write.saveAsTable(
                    self.complete_db_destination(self.config.database.schema, self.config.database.table),
                    format='iceberg',
                    mode='append'
                )
            except Exception as e:
                logger.error(f"error stream processing for table = {self.config.database.table}, "
                             f"topic = {self.config.kafka.topic}")
                logger.error(e)
        else:
            logger.debug(f"kafka topic = {self.config.kafka.topic} is empty")

    def bytes_to_catalyst(self, df):
        """
        Converts kafka bytes to json string to catalyst dataframe.

        :param df: Dataframe to convert.
        :return: Catalyst dataframe.
        """
        schema = self.infer_json_schema(df)

        return self.convert_column_from_json_schema(df, "value", schema)

    @staticmethod
    def infer_json_schema(df):
        """
        Inference of a JSON schema from a dataframe.
        It considers the schema of the first row and assumes the rest of the rows is compatible.

        :param df: Dataframe to be inferred.
        :return: Inferred string JSON schema.
        """
        first_row = df.head()
        return df.select(schema_of_json(first_row.value)).head()[0]

    @staticmethod
    def convert_column_from_json_schema(df, column, schema):
        """
        Converts kafka bytes to json string to catalyst dataframe.

        :param df: Dataframe to be converted.
        :param column: Column to be converted.
        :param schema: Reference schema to convert column.
        :return: Data frame
        """
        return df.withColumn(column, from_json(df.select(col(column)).value, schema))

    @staticmethod
    def complete_db_destination(schema, table):
        return "{}.{}".format(schema, table)

    def stop(self):
        logger.info("stopping...")
        self.streaming_query.stop()
        logger.info("stopped")
