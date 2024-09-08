import mysql.connector
import polars as pl
from dotenv import load_dotenv, find_dotenv
import os
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

load_dotenv(find_dotenv('sys1.env'))

mydb = mysql.connector.connect(
  host = "localhost",
  user = os.environ.get("MYSQL_USER"),
  password = os.environ.get("MYSQL_PASSWORD")
)
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM retail_product.product")
myresult = mycursor.fetchall()

df = pl.DataFrame(myresult, schema=mycursor.column_names, orient="row")
# print(df)

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


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVER'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.environ.get('KAFKA_API_KEY'),
    'sasl.password': os.environ.get('KAFKA_API_SECRET')
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': os.environ.get('SCHEMA_URL'),
    'basic.auth.user.info': '{}:{}'.format(os.environ.get('SCHEMA_API'), os.environ.get('SCHEMA_SECRET'))
})

# Fetch the latest Avro schema for the value
subject_name = 'product_tracker-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


# Iterate over DataFrame rows and produce to Kafka
for row in df.iter_rows():
    value = dict(zip(df.columns, row))
    producer.produce(topic='product_tracker', key=str(value['id']), value=value, on_delivery=delivery_report)
    producer.flush()
    time.sleep(2)

print("All Data successfully published to Kafka")