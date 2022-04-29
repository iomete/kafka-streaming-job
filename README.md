# Kafka Streaming Job

This is a collection of data movement capabilities. This streaming job copies data from Kafka to Iceberg.

Currently, two serialization format supported.
1. JSON
2. Avro

## JSON deserialization
In the Spark configuration, a user-defined reference json schema can be defined, 
and the system processes the binary data accordingly. Otherwise, 
It considers the schema of the first row and assumes the rest of the rows is compatible.

## Avro deserialization
Converts binary data according to the schema defined by the user or retrieves the schema from the schema registry.

![avro-deserialization-diagram](docs/diagram/avro-diagram.jpg)

## Get started
1. Create `.envrc` file with path to local spark

```shell
export SPARK_LOCATION=(location to local spark folder)
```

2. Submit Spark
```shell
make submit
```