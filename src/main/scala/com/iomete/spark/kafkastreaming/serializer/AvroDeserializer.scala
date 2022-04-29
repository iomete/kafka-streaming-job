package com.iomete.spark.kafkastreaming.serializer

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

class AvroDeserializer extends AbstractKafkaAvroDeserializer {

  def this(client: SchemaRegistryClient) {
    this()
    this.schemaRegistry = client
  }

  /**
   * Kafka avro deserializer for convert kafka bytes to string
   * @param payload Kafka bytes
   * @return converted string payload
   */
  override def deserialize(payload: Array[Byte]): String = {
    val value = super.deserialize(payload)
    value match {
      case str: String =>
        str
      case _ =>
        val genericRecord = value.asInstanceOf[GenericRecord]
        genericRecord.toString
    }
  }
}
