package com.iomete.spark.kafkastreaming

trait SerializationFormat

object SerializationFormat {
  case object JSON extends SerializationFormat
  case object Avro extends SerializationFormat

  def parse(s: String): SerializationFormat = {
    s.toLowerCase() match {
      case "json" => JSON
      case "avro" => Avro
    }
  }
}
