import argparse
import logging
import os
import sys
import typing

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

SSL_CA_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "ca.pem"))


class Arguments(argparse.Namespace):
    username: str
    password: str
    topic: str
    hostname: str
    group_id: str


def arguments() -> Arguments:
    parser = argparse.ArgumentParser(description="Railnova Kafka Avro consumer example with SASL")
    parser.add_argument(
        "--username", dest="username", required=True, help="SASL username"
    )
    parser.add_argument(
        "--password", dest="password", required=True, help="SASL password"
    )
    parser.add_argument("--topic", dest="topic", required=True, help="Kafka topic name")
    parser.add_argument(
        "--hostname",
        dest="hostname",
        default="kafka-13e7abdf-test-railnova-5ffc.aivencloud.com",
        help="Kafka broker hostname, defaults to Railnova's test broker",
    )
    parser.add_argument(
        "--group-id",
        dest="group_id",
        default="railnova_kafka_example",
        help="Kafka consumer group id",
    )
    return typing.cast(Arguments, parser.parse_args())


def main() -> int:
    # Parse command line arguments and configure a logger
    args = arguments()
    logHandler = logging.StreamHandler()
    logHandler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger = logging.getLogger("railnova_kafka_example")
    logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)

    # Create a deserializer and serialization contexts to decode message keys and values from AVRO.
    schema_registry = SchemaRegistryClient(
        {"url": f"https://{args.username}:{args.password}@{args.hostname}:27249"}
    )
    key_context = SerializationContext(args.topic, MessageField.KEY)
    value_context = SerializationContext(args.topic, MessageField.VALUE)
    avro_deserializer = AvroDeserializer(schema_registry)
    #
    # See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avrodeserializer

    # Test connectivity to the schema registry by fetching schema with id 1.
    schema_registry.get_schema(1)
    logger.info(f"Schema registry is accessible at '{args.hostname}'")

    # Create a Kafka consumer with a sensible configuration a for a single consumer.
    kafka_consumer = Consumer(
        {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": args.username,
            "sasl.password": args.password,
            "ssl.ca.location": SSL_CA_LOCATION,
            "bootstrap.servers": f"{args.hostname}:27257",
            "message.max.bytes": 5000000,
            "group.id": args.group_id,
            "enable.auto.commit": True,  # commit the offset automatically.
            "auto.offset.reset": "earliest",  # start from the beginning of the topic (for this consumer group).
            "partition.assignment.strategy": "roundrobin",  # consume from all partitions in a round-robin fashion.
        },
        logger=logger,
    )
    # see https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-configuration
    #
    # Subscribe to the given topic
    kafka_consumer.subscribe([args.topic])
    #
    logger.info(f"Kafka consumer subscribed to topic '{args.topic}'")

    # poll for messages until one is consumed or a keyboard SIGINT is received ...
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            message = kafka_consumer.poll(1.0)
            if message is None:
                continue

            # Check for Kafka errors, as Confluent bundle them as messages.
            error: KafkaError | None = message.error()
            if error is None:
                # log the deserialized message's key and value
                k = avro_deserializer(message.key() or b"", key_context)
                v = avro_deserializer(message.value() or b"", value_context)
                logger.info(f"Received {k} -> {v}")
                break

            else:
                # Print the error message found in the message's value
                logger.error(bytes.decode(message.value() or b"error message missing"))
        except KeyboardInterrupt:
            break

    kafka_consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
