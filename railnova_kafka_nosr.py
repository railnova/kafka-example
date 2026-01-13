import argparse
import io
import json
import logging
import glob
import os
import struct
import sys
import typing

from confluent_kafka import Consumer, KafkaError
from fastavro.types import Schema
from fastavro import parse_schema, schemaless_reader

# Parse all schemas from the `schemas` folder and index them in a dictionary by their id.
SCHEMAS_LOCATION = os.path.join(os.path.dirname(__file__), "schemas")
SCHEMAS: dict[int, Schema] = {
    int(os.path.basename(f).split(".")[0]): parse_schema(json.loads(open(f).read()))
    for f in glob.glob(os.path.join(SCHEMAS_LOCATION, "*.json"))
}


def decode_avro(payload: bytes) -> dict:
    """
    A function to decode an Avro payload without depending on the Schema Registry
    using instead the schemas supplied.
    """
    f = io.BytesIO(payload)
    try:
        # Read the magic byte and the schema id from the payload.
        magic, schema_id = struct.unpack(">bI", f.read(5))
        if schema_id not in SCHEMAS:
            raise ValueError(f"Unknown schema id: {schema_id}")

        # Decode the payload using the schema found and return the decoded result.
        return schemaless_reader(f, SCHEMAS[schema_id])  # type: ignore

    finally:
        f.close()


SSL_CA_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "ca.pem"))


class Arguments(argparse.Namespace):
    cert_path: str
    key_path: str
    topic: str
    hostname: str
    group_id: str


def arguments() -> Arguments:
    parser = argparse.ArgumentParser(
        description="Railnova Kafka schema-less Avro consumer example with mTLS"
    )
    parser.add_argument(
        "--certificate",
        dest="cert_path",
        required=True,
        help="Path to the Access Certificate file",
    )
    parser.add_argument(
        "--key", dest="key_path", required=True, help="Path to the Access Key file"
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

    # Create a Kafka consumer with a sensible configuration a for a single consumer.
    kafka_consumer = Consumer(
        {
            "security.protocol": "SSL",
            "ssl.ca.location": SSL_CA_LOCATION,
            "ssl.certificate.location": args.cert_path,
            "ssl.key.location": args.key_path,
            "bootstrap.servers": f"{args.hostname}:27246",
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
                k = decode_avro(message.key() or b"")
                v = decode_avro(message.value() or b"")
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
