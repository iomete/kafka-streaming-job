import os
from dataclasses import dataclass

from pyhocon import ConfigFactory

checkpointLocation = "kafka-streaming/data/_checkpoints_" + os.getenv("SPARK_APPLICATION_ID")


class SerializationFormat:
    JSON = "json"
    AVRO = "avro"

    @staticmethod
    def from_str(serialization_format):
        serialization_format = serialization_format.lower()
        if serialization_format == "json":
            return SerializationFormat.JSON
        elif serialization_format == "avro":
            return SerializationFormat.AVRO
        raise NotImplementedError


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    starting_offsets: str
    group_id: str
    processing_time: str
    serialization_format: SerializationFormat
    schema_registry_url: str


@dataclass
class DbConfig:
    schema: str
    table: str


@dataclass
class ApplicationConfig:
    kafka: KafkaConfig
    database: DbConfig
    checkpoint_location: str


def format_processing_time(interval, unit):
    return "{} {}".format(interval, unit)


def get_config(application_path) -> ApplicationConfig:
    config = ConfigFactory.parse_file(application_path)

    kafka = KafkaConfig(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        topic=config['kafka']['topic'],
        starting_offsets=config['kafka']['starting_offsets'],
        group_id=config['kafka']['group_id'],
        processing_time=format_processing_time(config['kafka']['trigger']['interval'],
                                               config['kafka']['trigger']['unit']),
        serialization_format=SerializationFormat.from_str(config['kafka']['serialization_format']),
        schema_registry_url=config['kafka']['schema_registry_url']
    )

    database = DbConfig(
        schema=config['database']['schema'],
        table=config['database']['table']
    )

    return ApplicationConfig(
        kafka=kafka,
        database=database,
        checkpoint_location=checkpointLocation
    )
