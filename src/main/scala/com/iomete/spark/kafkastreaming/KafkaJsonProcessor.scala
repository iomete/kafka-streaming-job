package com.iomete.spark.kafkastreaming

import com.iomete.spark.kafkastreaming.util.SparkUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.functions.{from_json, schema_of_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

class KafkaJsonProcessor(spark: SparkSession) extends StreamProcessor {

  private val log = LogFactory.getLog(this.getClass)

  private val BOOTSTRAP_SERVERS = spark.sparkContext.getConf.get(SparkUtils.BOOTSTRAP_SERVERS)
  private val TOPIC_NAME = spark.sparkContext.getConf.get(SparkUtils.TOPIC_NAME)
  private val STARTING_OFFSETS = spark.sparkContext.getConf.get(SparkUtils.STARTING_OFFSETS)
  private val FULL_TABLE_NAME = spark.sparkContext.getConf.get(SparkUtils.FULL_TABLE_NAME)
  private var VALUE_SCHEMA = spark.sparkContext.getConf.get(SparkUtils.VALUE_SCHEMA)
  private val TRIGGER_INTERVAL = spark.sparkContext.getConf.get(SparkUtils.TRIGGER_INTERVAL)

  /**
   * Responsible for processing spark streaming
   */
  override def process(): Unit = {
    log.info(s"Stream processing starting for topic=$TOPIC_NAME")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TOPIC_NAME)
      .option("startingOffsets", STARTING_OFFSETS)
      .load()

    df.writeStream
      .foreachBatch(microBatch _)
      .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
      .start()
      .awaitTermination()
  }

  /**
   * Responsible for processing micro batches for every batch processing.
   *
   * @param batchDF Batch dataframe to be wrote.
   * @param batchId Micro batch ID.
   */
  override def microBatch(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      var parsedDF: DataFrame = null
      try {
        parsedDF = bytesToCatalyst(batchDF).select("value.*")

        parsedDF.write.mode(SaveMode.Append).saveAsTable(FULL_TABLE_NAME)
      } catch {
        case _: AnalysisException =>
          if (!parsedDF.isEmpty) {
            log.warn(s"table=$FULL_TABLE_NAME does not exists")
            parsedDF.createOrReplaceTempView(FULL_TABLE_NAME)
            spark.sql(createTableQuery(FULL_TABLE_NAME))
          }
        case e: Exception =>
          log.error(s"error stream processing table=$FULL_TABLE_NAME")
          e.printStackTrace()
      }
    }
  }

  /**
   * Converts kafka bytes to json string to catalyst dataframe.
   *
   * @param df Dataframe to convert.
   * @return Catalyst dataframe.
   */
  def bytesToCatalyst(df: DataFrame): DataFrame = {

    if (VALUE_SCHEMA.isEmpty) { // user defined schema.
      VALUE_SCHEMA = inferJsonSchema(df, "value")
    }

    convertColumn(df, "value", VALUE_SCHEMA)
  }

  /**
   * Inference of a JSON schema from a dataframe.
   * It considers the schema of the first row and assumes the rest of the rows is compatible.
   *
   * @param df Dataframe to be inferred.
   * @param column Reference column.
   * @return Inferred string JSON schema.
   */
  def inferJsonSchema(df: DataFrame, column: String): String = {
    val firstRow = df.select(df(column).cast(StringType)).head()

    df.select(schema_of_json(firstRow.getAs[String](column))).head().getAs[String](0)
  }

  /**
   * This method convert the provided dataframe's column with schema.
   *
   * @param df Dataframe to be converted.
   * @param column Column to be converted.
   * @param schema Reference schema to convert column.
   * @return Dataframe
   */
  def convertColumn(df: DataFrame, column: String, schema: String): DataFrame = {
    df.withColumn(column, from_json(df(column).cast(StringType), schema,
      Map.empty[String, String]).as(column))
  }

  /**
   * Create table query for spark sql.
   *
   * @param fullTableName Full table name to be created.
   * @return String spark sql for execution.
   */
  def createTableQuery(fullTableName: String): String = {
    s"CREATE TABLE IF NOT EXISTS $fullTableName AS SELECT * FROM $fullTableName"
  }

}
