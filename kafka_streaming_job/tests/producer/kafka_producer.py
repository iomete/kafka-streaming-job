from time import sleep
from json import dumps
from uuid import uuid4

from kafka import KafkaProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def kafka_json_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    for e in range(10):
        data = {'number': e}
        producer.send('numtest', value=data)
        sleep(5)


def kafka_avro_producer():
    schema_str = """
        {
          "namespace": "com.avro.stream",
          "type": "record",
          "name": "myrecord",
          "fields": [
            {"name": "f1", "type": "string"},
            {"name": "f2",  "type": "int"}
          ]
        }
        """
    topic = "test-avro"
    schema_registry_conf = {'url': "http://127.0.0.1:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str)

    producer_conf = {'bootstrap.servers': "localhost:9092",
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing mock class records to topic {}. ^C to exit.".format(topic))
    for i in range(10):
        producer.poll(0.0)
        try:
            f1 = "{} {}".format("random", i)
            f2 = i
            user = MockClass(f1=f1,
                             f2=f2, )
            producer.produce(topic=topic, key=str(uuid4()), value=user,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

        print("\nFlushing records...")
        producer.flush()


class User(object):
    """
    User record
    Args:
        firstname (str): first name
        lastname (int): lastname
    """

    def __init__(self, firstname, lastname):
        self.firstname = firstname
        self.lastname = lastname


class MockClass(object):
    """
    User record
    Args:
        f1 (str): first field
        f2 (int): second field
    """

    def __init__(self, f1, f2):
        self.f1 = f1
        self.f2 = f2


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
