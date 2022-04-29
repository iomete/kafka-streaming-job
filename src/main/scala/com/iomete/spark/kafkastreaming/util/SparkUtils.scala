package com.iomete.spark.kafkastreaming.util

import org.apache.spark.sql.SparkSession

object SparkUtils {
  val SCHEMA_REGISTRY_CLIENT_URL = "spark.iomete.schemaregistry.client.url"
  val FULL_TABLE_NAME = "spark.iomete.fulltablename"
  val SERIALIZATION_FORMAT = "spark.iomete.serialization.format"
  val VALUE_SCHEMA = "spark.iomete.valueSchema"
  val BOOTSTRAP_SERVERS = "spark.iomete.bootstrap.servers"
  val TOPIC_NAME = "spark.iomete.topicName"
  val STARTING_OFFSETS = "spark.iomete.startingOffsets"
  val TRIGGER_INTERVAL = "spark.iomete.trigger.interval"

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("IntegrationTest")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://S3_LOCATION_TO_WAREHOUSE")
      .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
      .config("spark.sql.catalog.spark_catalog.uri", "jdbc:mysql://user:pass@localhost:3306/catalog_db")
      .config("spark.sql.catalog.spark_catalog.jdbc.verifyServerCertificate", "true")
      .config("spark.sql.catalog.spark_catalog.jdbc.useSSL", "true")
      .config("spark.hive.metastore.warehouse.dir", "s3a://${S3_LOCATION_TO_WAREHOUSE}")
      .config("spark.hive.metastore.uris", "http://localhost:9083")
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .config("spark.hadoop.fs.s3a.access.key", "${AWS_ACCESS_KEY}")
      .config("spark.hadoop.fs.s3a.secret.key", "${AWS_SECRET_KEY}")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.${REGION_CODE}.amazonaws.com")
      .config(SCHEMA_REGISTRY_CLIENT_URL, "http://127.0.0.1:8081")
      .config(FULL_TABLE_NAME, "myrecordavro2")
      .config(SERIALIZATION_FORMAT, "avro")
      .config(VALUE_SCHEMA, "")
      .config(BOOTSTRAP_SERVERS, "localhost:9092")
      .config(TOPIC_NAME, "test-avro")
      .config(STARTING_OFFSETS, "latest")
      .config(TRIGGER_INTERVAL, "5 seconds")
      .getOrCreate()
  }
}
