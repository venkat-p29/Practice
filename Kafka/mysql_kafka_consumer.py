import json
import polars as pl
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

load_dotenv(find_dotenv('sys1.env'))

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Return ISO format for datetime
    raise TypeError(f"Type {type(obj)} not serializable")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVER'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.environ.get('KAFKA_API_KEY'),
    'sasl.password': os.environ.get('KAFKA_API_SECRET'),
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': os.environ.get('SCHEMA_URL'),
    'basic.auth.user.info': '{}:{}'.format(os.environ.get('SCHEMA_API'), os.environ.get('SCHEMA_SECRET'))
})

# Fetch the latest Avro schema for the value
subject_name = 'product_tracker-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['product_tracker'])

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        msg.value()['category'] = msg.value()['category'].upper() 
        
        # updating the price to half if product belongs to 'Category A'
        if msg.value()['category'] == 'CATEGORYA':           
            msg.value()['price'] = msg.value()['price'] * 0.5
            msg.value()['price'] = round(msg.value()['price'],2)
            
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
            
        with open('consumer3.json', 'a') as f:
            json.dump(msg.value(), f, default=default_serializer)
            f.write("\n")
        
except KeyboardInterrupt:
    pass

finally:
    consumer.close()