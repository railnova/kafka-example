#!/usr/bin/env python
# coding: utf-8
from pprint import pprint

# Import the Kafka library
try:
    from confluent_kafka import DeserializingConsumer, KafkaException
    from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    import os
except:
    raise Exception("You need to install the dependancies")
# Import the settings you defined and that were provided to you
try:
    from settings import (
        KAFKA_BROKER,
        KAFKA_USER,
        KAFKA_PASSWORD,
        KAFKA_TOPIC,
        KAFKA_SCHEMA_REGISTRY,
        KAFKA_GROUP_ID,
    )
except:
    raise Exception(
        "You need to create a file settings.py with the variables KAFKA_BROKER, KAFKA_USER, KAFKA_PASSWORD, KAFKA_TOPIC, KAFKA_SCHEMA_REGISTRY"
    )
# Validate the content of settings
assert len(KAFKA_BROKER) > 0, "The setting KAFKA_BROKER is empty"
assert len(KAFKA_USER) > 0, "The setting KAFKA_USER is empty"
assert len(KAFKA_PASSWORD) > 0, "The setting KAFKA_PASSWORD is empty"
assert len(KAFKA_TOPIC) > 0, "The setting KAFKA_TOPIC is empty"
assert len(KAFKA_SCHEMA_REGISTRY) > 0, "The setting KAFKA_SCHEMA_REGISTRY is empty"
assert len(KAFKA_GROUP_ID) > 0
# Validate the presence of the CA
assert os.path.exists(
    "./ca.pem"
), "The file 'ca.pem' provided to you was not found in the current directory"


SCHEMA_REGISTRY_CONFIG = {
    "url": f"https://{KAFKA_USER}:{KAFKA_PASSWORD}@{KAFKA_SCHEMA_REGISTRY}",
    # "ssl.ca.location": "ca.pem",
}
sr = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

# Defining the kafka config, a dictionnary to be passed to a consumer for access to the Kafka
KAFKA_CONFIG = {
    "sasl.username": KAFKA_USER,
    "sasl.password": KAFKA_PASSWORD,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-256",
    "ssl.ca.location": "ca.pem",
    "bootstrap.servers": KAFKA_BROKER,
    "message.max.bytes": 5000000,
    "group.id": KAFKA_GROUP_ID,
    "enable.auto.commit": True,
    "auto.offset.reset": "earliest",
    "partition.assignment.strategy": "roundrobin",
    "key.deserializer": AvroDeserializer(sr),
    "value.deserializer": AvroDeserializer(sr),
}
consumer = DeserializingConsumer(KAFKA_CONFIG)

# Validate the connexion by making a request to the Kafka
try:
    metadata = consumer.list_topics(timeout=10)
    if metadata.orig_broker_id in [None, 0, -1] and not metadata.brokers:
        raise KafkaInitialConnectionError("Initial connection broker id is invalid")
    print("Connection to Railnova Kafka successful!")
except KafkaException as exception:
    error = exception.args[0]

    class KafkaInitialConnectionError(Exception):
        pass

    raise KafkaInitialConnectionError(error.str())

consumer.subscribe([KAFKA_TOPIC])
print("Polling Kafka with a timeout of 120s...")
msg = consumer.poll(timeout=120)
print("Key:\t", end="")
print(msg.key())
print("Value:")
pprint(msg.value(), sort_dicts=False)
print("\nOne message succesfully consumed!")
print("You can configure the Kafka interface in your own service.")
