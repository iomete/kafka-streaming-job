from uuid import uuid4

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer


def user_to_dict(user, ctx):
    return dict(first_name=user.first_name,
                last_name=user.last_name)


def kafka_json_producer():
    topic = "json-topic"

    schema_registry_conf = {'url': "http://127.0.0.1:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(JSON_SCHEMA,
                                     schema_registry_client,
                                     user_to_dict)

    producer_conf = {
        'bootstrap.servers': "localhost:9092",
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': json_serializer
    }

    producer = SerializingProducer(producer_conf)

    produce_messages(topic=topic, producer=producer)


def kafka_avro_producer():
    topic = "avro-topic"
    schema_registry_conf = {'url': "http://127.0.0.1:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     AVRO_SCHEMA,
                                     user_to_dict)

    producer_conf = {
        'bootstrap.servers': "localhost:9092",
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }

    producer = SerializingProducer(producer_conf)

    produce_messages(topic=topic, producer=producer)


def produce_messages(topic: str, producer: SerializingProducer):
    for i in range(10):
        producer.poll(0.0)
        try:
            user = User(
                first_name="asdasd",
                last_name="sdsds"
            )
            producer.produce(
                topic=topic, value=user, on_delivery=delivery_report
            )
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

        print("\nFlushing records...")
        producer.flush()


JSON_SCHEMA = """
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "User",
          "description": "A Confluent Kafka Python User",
          "type": "object",
          "properties": {
            "first_name": {
              "description": "User's first name",
              "type": "string"
            },
            "last_name": {
              "description": "User's last name",
              "type": "string"
            }
          },
          "required": [ "first_name", "last_name" ]
        }
        """

AVRO_SCHEMA = """
        {
          "namespace": "com.avro.stream",
          "type": "record",
          "name": "user_record",
          "fields": [
            {"name": "first_name", "type": "string"},
            {"name": "last_name",  "type": "string"}
          ]
        }
        """


class User(object):
    """
    User record
    Args:
        first_name (str): first name
        last_name (str): lastname
    """

    def __init__(self, first_name: str, last_name: str):
        self.first_name = first_name
        self.last_name = last_name


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
