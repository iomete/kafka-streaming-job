# iomete: Kafka Streaming Job

<p align="center">
<img src="docs/img/iomete-logo.png" width="250" /> <img src="docs/img/apache-kafka-logo.png" width="250" />
</p>

This is a collection of data movement capabilities. This streaming job copies data from Kafka to Iceberg.

## Table of Contents
 * [Deserialization](#Deserialization)
 * [Job creation](#job-creation)
 * [Tests](#tests)


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

![avro-deserialization-diagram](docs/img/diagram/avro-diagram.jpg)

## Job creation

- Go to `Spark Jobs`.
- Click on `Create New`.

Specify the following parameters (these are examples, you can change them based on your preference):
- **Name:** `kafka-streaming-job`
- **Docker Image:** `iomete/iomete_kafka_streaming_job:0.2.1`
- **Main application file:** `local:///app/driver.py`
- **Environment Variables:** `LOG_LEVEL`: `INFO` or `ERROR`
- **Config file:** 
```hocon
{
  kafka: {
      bootstrap_servers: "localhost:9092",
      topic: "usage.spark.0",
      serialization_format: json,
      group_id: group_1,
      starting_offsets: latest,
      trigger: {
        interval: 5
        unit: seconds # minutes
      },
      schema_registry_url: "http://127.0.0.1:8081"
  },
  database: {
    schema: default,
    table: spark_usage_20
  }
}
```

## Configuration properties
<table>
  <thead>
    <tr>
      <th>Property</th>
      <th>Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <code>kafka</code><br/>
      </td>
      <td>
        <p>Required properties to connect and configure.</p>
        <table>
            <tbody>
                <tr>
                  <td>
                    <code>bootstrap_servers</code>
                  </td>
                  <td>
                    <p>Kafka broker server.</p>
                  </td>
                </tr>
                <tr>
                  <td>
                    <code>topic</code>
                  </td>
                  <td>
                    <p>Kafka topic name.</p>
                  </td>
                </tr>
                <tr>
                  <td>
                    <code>serialization_format</code>
                  </td>
                  <td>
                    <p>Value data serialization format.</p>
                  </td>
                </tr>
                <tr>
                  <td>
                    <code>group_id</code>
                  </td>
                  <td>
                    <p>Consumer group id.</p>
                  </td>
                </tr>
                <tr>
                  <td>
                    <code>starting_offsets</code>
                  </td>
                  <td>
                    <p>Specify where to start instead.</p>
                  </td>
                </tr>
                <tr>
                  <td>
                    <code>trigger</code>
                  </td>
                  <td>
                    <ul>
                      <li><code>interval</code> Processing trigger interval.</li>
                      <li><code>unit</code> Processing trigger unit: seconds, minutes</li>
                    </ul>
                  </td>
                </tr>
            </tbody>
        </table>
      </td>
    </tr>
    <tr>
      <td>
        <code>database</code><br/>
      </td>
      <td>
        <p>Destination database properties.</p>
        <ul>
          <li><code>schema</code> Specify the schema (database) to store into.</li>
          <li><code>table</code> Specify the table.</li>
        </ul>
      </td>
    </tr>
</tbody>
</table>

Create Spark Job
![Create Spark Job.png](docs/img/job-creation-deployment.png)

Create Spark Job - Instance

>You can use **Environment Variables** to store your sensitive data like password, secrets, etc. Then you can use these variables in your config file using the <code>${ENV_NAME}</code> syntax.

![Create Spark Job.png](docs/img/job-creation-environment.png)

Create Spark Job - Application Environment
![Create Spark Job - Application Config.png](docs/img/job-creation-config.png)

Create Spark Job - Application dependencies
![Create Spark Job - Application Environment.png](docs/img/job-creation-dependencies.png)

## Tests

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
