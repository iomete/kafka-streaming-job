local_spark_location := ${SPARK_LOCATION}

build:
	sbt clean assembly

submit:
	/Users/namig/Projects/infra/spark/spark-google-operator/docker/public-spark-3.2.1/spark/spark3.2.1-iomete-release-v4/bin/spark-submit \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
		--class com.iomete.spark.kafkastreaming.KafkaStreaming \
		target/scala-2.12/kafka-streaming-job-assembly-0.1.1.jar