import argparse
import logging
import io
import json
import os
import typing
import requests
import struct
import sys

from confluent_kafka import Producer, Message, admin
from fastavro.types import Schema
from fastavro import parse_schema, schemaless_writer

SSL_CA_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "ca.pem"))

class SchemaDefinition(typing.TypedDict):
    id: int
    schema: str
    schemaType: str
    subject: str
    version: int


def load_schemas(base_url: str, username: str, password: str, subjects: set[str]) -> dict[str, tuple[int, Schema]]:
    resp = requests.get(f"{base_url}/schemas", auth=(username, password))
    assert (
        resp.status_code == 200
    ), f"Failed to fetch schemas from registry: {resp.text}"
    response_json: list[SchemaDefinition] = resp.json()
    # TODO: handle multiple versions properly, return the latest version only for each subject
    return {
        definition["subject"]: (definition["id"], parse_schema(json.loads(definition["schema"])))
        for definition in response_json
        if definition["subject"] in subjects
    }

class IngestValue(typing.TypedDict):
    type: str
    timestamp: str
    asset_uic: str
    content: dict[str, str | float | int | bool | None]


def encode_avro(value, schema_id: int, schema: Schema) -> bytes:
    """
    A function to encode an ingest message as an Avro payload using the schemas supplied.
    """
    f = io.BytesIO()
    try:
        # Write the magic byte and the schema id from the payload.
        f.write(struct.pack(">bI", 0, schema_id))
        # Decode the payload using the schema found and return the decoded result.
        schemaless_writer(f, schema, value)  # type: ignore
        return f.getvalue()

    finally:
        f.close()


class Arguments(argparse.Namespace):
    input: str
    username: str
    password: str
    cert_path: str
    key_path: str
    topic: str
    hostname: str


def arguments() -> Arguments:
    parser = argparse.ArgumentParser(
        description="Railnova Kafka Avro producer example with mTLS"
    )
    parser.add_argument(
        "--input", dest="input", required=True, help="Input JSON file path"
    )
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
        f"https://{args.hostname}:27249", args.username, args.password,
        {"ingest-key", "ingest-value"},
    )
    ingest_key_id, ingest_key_schema = schemas["ingest-key"]
    ingest_value_id, ingest_value_schema = schemas["ingest-value"]
    logger.info(f"Schema loaded from registry at '{args.hostname}:27249'")

    # Create a Kafka producer with a sensible configuration a for a single producer.
    kafka_producer = Producer(
        {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "ssl.ca.location": SSL_CA_LOCATION,
            "sasl.username": args.username,
            "sasl.password": args.password,
            "bootstrap.servers": f"{args.hostname}:27257",
            "message.max.bytes": 5000000,
            "compression.codec": "zstd",
            "compression.level": "1",
            "queue.buffering.max.ms": 10,  # Wait 10ms for higher compression
        },
        logger=logger,
    )
    # see https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-configuration
    #    
    # Check that the topic exists in the Kafka cluster
    metadata: admin.ClusterMetadata = kafka_producer.list_topics(args.topic, timeout=5)
    if args.topic not in metadata.topics:
        logger.error(f"Topic '{args.topic}' does not exist in the Kafka cluster")
        return 1

    def produce_callback(err, msg: Message) -> None:
        if err is None:
            return

        logger.error(
            f"Producer error in produce_callback: {err}",
            extra={
                "topic": msg.topic(),
                "kafka_timestamp": msg.timestamp(),
                "kafka_key": msg.key(),
                "partition": msg.partition(),
            },
        )

    logger.info(f"Ready to produce into Kafka topic '{args.topic}'")        
    with open(args.input, "r", encoding="utf-8") as f:
        ingest_values: list[IngestValue] = json.load(f)
    logger.info(f"Loaded {len(ingest_values)} ingest values from '{args.input}', start producing ...")
    for value in ingest_values:
        try:
            k = encode_avro(value["asset_uic"], ingest_key_id, ingest_key_schema)
            v = encode_avro(value, ingest_value_id, ingest_value_schema)
            kafka_producer.produce(
                topic=args.topic,
                headers={"source": "railnova/kafka-example"},
                key=k,
                value=v,
                callback=produce_callback,
            )
        except KeyboardInterrupt:
            break

    logger.info("Flushing outstanding messages ...")
    kafka_producer.flush(20)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
