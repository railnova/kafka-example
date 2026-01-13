import argparse
import logging
import io
import json
import os
from typing import TypedDict
import typing
import requests
import struct
import sys

from confluent_kafka import Consumer, KafkaError
from fastavro.types import Schema
from fastavro import parse_schema, schemaless_reader

SSL_CA_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "ca.pem"))


class SchemaDefinition(TypedDict):
    id: int
    schema: str


def load_schemas(base_url: str, username: str, password: str) -> dict[int, Schema]:
    resp = requests.get(f"{base_url}/schemas", auth=(username, password))
    assert (
        resp.status_code == 200
    ), f"Failed to fetch schemas from registry: {resp.text}"
    response_json: list[SchemaDefinition] = resp.json()
    return {
        definition["id"]: parse_schema(json.loads(definition["schema"]))
        for definition in response_json
    }


def decode_avro(payload: bytes, schemas: dict[int, Schema]) -> tuple[int, dict]:
    """
    A function to decode an Avro payload using the schemas supplied.
    """
    f = io.BytesIO(payload)
    try:
        # Read the magic byte and the schema id from the payload.
        magic, schema_id = struct.unpack(">bI", f.read(5))
        if schema_id not in schemas:
            raise ValueError(f"Unknown schema id: {schema_id}")

        # Decode the payload using the schema found and return the decoded result.
        return schema_id, schemaless_reader(f, schemas[schema_id], None)  # type: ignore

    finally:
        f.close()


class Arguments(argparse.Namespace):
    username: str
    password: str
    cert_path: str
    key_path: str
    topic: str
    hostname: str
    group_id: str


def arguments() -> Arguments:
    parser = argparse.ArgumentParser(
        description="Railnova Kafka Avro consumer example with mTLS"
    )
    parser.add_argument(
        "--username", dest="username", required=True, help="SASL username"
    )
    parser.add_argument(
        "--password", dest="password", required=True, help="SASL password"
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
    args: Arguments = arguments()
    logHandler = logging.StreamHandler()
    logHandler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger = logging.getLogger("railnova_kafka_example")
    logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)

    # Load the schemas from the schema registry
    schemas = load_schemas(
        f"https://{args.hostname}:27249", args.username, args.password
    )
    logger.info(f"Schema loaded from registry at '{args.hostname}:27249'")

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
                k = decode_avro(message.key() or b"", schemas)
                v = decode_avro(message.value() or b"", schemas)
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
