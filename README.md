# iomete: Kafka Streaming Job

<p align="center">
<img src="docs/iomete-logo.png" width="250" /> <img src="docs/apache-kafka-logo.png" width="250" />
</p>

This is a collection of data movement capabilities. This streaming job copies data from Kafka to Iceberg.

## Table of Contents
 * [Deserialization](#Deserialization)
 * [Job creation](#job-creation)
 * [Profile Configuration](#profile-configuration)
 * [Database User Privileges](#database-user-privileges)
 * [Running Tests](#running-tests)
 * [Example](#example)
 * [Contributing](#contributing)


## Deserialization
Currently, two deserialization format supported.
1. JSON
2. AVRO

### JSON
In the Spark configuration, a user-defined reference json schema can be defined, 
and the system processes the binary data accordingly. Otherwise, 
It considers the schema of the first row and assumes the rest of the rows is compatible.

### Avro
Converts binary data according to the schema defined by the user or retrieves the schema from the schema registry.

![avro-deserialization-diagram](docs/diagram/avro-diagram.jpg)

## Job creation
Following configuration parameters needed for create a new job in iomete UI.

*NOTE: Don't forget to replace the parameters with your own.

#### Configuration

```hocon
{
      "kafka": {
          "bootstrap_servers": "localhost:9092",
          "topic_name": "warehouse.status-change.0",
          "serialization_format": "json",
          "starting_offsets": "latest",
          "trigger": {
            "interval": "5"
            "unit": "seconds" # minutes
          },
          "schema_registry_url": "http://127.0.0.1:8081" for avro topics
      },
      "database": {
        "schema": "default",
        "table": "spark_usage_3"
      }
}
```

## Development

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

**Run test**

```shell
pytest
```
