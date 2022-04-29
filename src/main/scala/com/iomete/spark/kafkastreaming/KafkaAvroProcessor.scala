package com.iomete.spark.kafkastreaming
import com.iomete.spark.kafkastreaming.serializer.AvroDeserializer
import com.iomete.spark.kafkastreaming.util.SparkUtils
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

class KafkaAvroProcessor(spark: SparkSession) extends StreamProcessor {

  private val log = LogFactory.getLog(this.getClass)

  private val SCHEMA_REGISTRY_CLIENT_URL = spark.sparkContext.getConf.get(SparkUtils.SCHEMA_REGISTRY_CLIENT_URL)
  private val BOOTSTRAP_SERVERS = spark.sparkContext.getConf.get(SparkUtils.BOOTSTRAP_SERVERS)
  private val VALUE_SCHEMA = spark.sparkContext.getConf.get(SparkUtils.VALUE_SCHEMA)
  private val TOPIC_NAME = spark.sparkContext.getConf.get(SparkUtils.TOPIC_NAME)
  private val STARTING_OFFSETS = spark.sparkContext.getConf.get(SparkUtils.STARTING_OFFSETS)
  private val FULL_TABLE_NAME = spark.sparkContext.getConf.get(SparkUtils.FULL_TABLE_NAME)

  private val schemaRegistryClient: SchemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_CLIENT_URL, 50)

  private val kafkaAvroDeserializer: AvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  /**
   * Method which responsible for processing spark streaming
   */
  override def process(): Unit = {
    log.info(s"ActionLog.process streaming starting for topic $TOPIC_NAME")

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TOPIC_NAME)
      .option("startingOffsets", STARTING_OFFSETS)
      .load()

    import org.apache.spark.sql.functions._

    val jsonDf = df.select(
      call_udf("deserialize", col("key")).as("key"),
      call_udf("deserialize", col("value")).as("value")
    )

    val dfValueSchema = {
      val rawSchema = retrieveTopicSchema(TOPIC_NAME)
      avroSchemaToSparkSchema(rawSchema)
    }

    println(dfValueSchema)
    val parsedDf = jsonDf.select(from_json(col("value"), dfValueSchema.dataType).alias("value"))
      .select("value.*")

    println(parsedDf.printSchema())
    parsedDf.writeStream
      .foreachBatch(microBatch _)
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
    try {
      println(batchDF.show())
      batchDF.write.mode(SaveMode.Append).saveAsTable(FULL_TABLE_NAME)
    } catch {
      case _: AnalysisException =>
        if (!batchDF.isEmpty) {
          log.warn(s"table=$FULL_TABLE_NAME does not exists")
          batchDF.createOrReplaceTempView(FULL_TABLE_NAME)
        }
      case e: Exception =>
        log.error(s"error stream processing table=$FULL_TABLE_NAME")
        e.printStackTrace()
    }
  }

  def toCatalyst(df: DataFrame): DataFrame = {
    if (VALUE_SCHEMA.isEmpty) {
      val avroSchema = retrieveSchema(TOPIC_NAME)
      parseJsonDFtoAvro(df, avroSchema)
    } else if (SCHEMA_REGISTRY_CLIENT_URL.isEmpty) {
      convertColumn(df, "value", VALUE_SCHEMA)
    } else {
      throw new IllegalArgumentException("Not supported")
    }
  }

  def retrieveSchema(topic: String, isKey: Boolean = false): SchemaConverters.SchemaType = {
    val avroSchema = retrieveTopicSchema(topic, isKey)

    avroSchemaToSparkSchema(avroSchema)
  }

  def parseJsonDFtoAvro(jsonDF: DataFrame, schemaType: SchemaConverters.SchemaType): DataFrame = {
    jsonDF.select(from_json(col("value"), schemaType.dataType).alias("value"))
      .select("value.*")
  }

  object DeserializerWrapper {
    val deserializer: AvroDeserializer = kafkaAvroDeserializer
  }

  def retrieveTopicSchema(topic: String, isKey: Boolean = false): String = {
    schemaRegistryClient.getLatestSchemaMetadata(topic + (if (isKey) "-key" else "-value")).getSchema
  }

  def avroSchemaToSparkSchema(avroSchema: String): SchemaConverters.SchemaType = {
    SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
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
