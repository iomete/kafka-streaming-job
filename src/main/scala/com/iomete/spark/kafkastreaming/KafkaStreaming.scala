package com.iomete.spark.kafkastreaming

import com.iomete.spark.kafkastreaming.util.SparkUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession

object KafkaStreaming {
  private val log = LogFactory.getLog(this.getClass)

  def main(args: Array[String]): Unit = {
    log.info("Kafka Streaming: 0.1.1 start")
    val spark = SparkSession.builder().appName("Kafka Streaming Job").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    try {
      processStream(spark)
    } finally {
      spark.close()
    }
    log.info("Kafka Streaming: 0.1.1 end")
  }

  def processStream(spark: SparkSession): Unit = {
    val serializationFormat = SerializationFormat.parse(spark.sparkContext.getConf.get(SparkUtils.SERIALIZATION_FORMAT))

    serializationFormat match {
      case SerializationFormat.Avro =>
        new KafkaAvroProcessor(spark).process()
      case SerializationFormat.JSON =>
        new KafkaJsonProcessor(spark).process()
    }
  }
}
