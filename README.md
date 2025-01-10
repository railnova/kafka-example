# Railnova Kafka Example

This repository provides an example of how to fetch and decode messages from one of Railnova's Kafka sharing topic,
using the Python programming language.

Besides this `README.md` and a `CHANGELOG.md` it contains two files:

- `ca.pem`: a Certificate Authority used to verify the Kafka broker's certificate.
- `railnova_kafka_example.py`: a program to poll one message from a given topic

Note that this example requires at least Python 3.9.

## Install

First, clone this repository in a `railnova-kafka-example` folder:

```bash
git clone https://github.com/railnova/kafka-example railnova-kafka-example
```

Then move to that folder, create a Python virtual environment called `venv` and activate it:

```bash
cd railnova-kafka-example
python3 -m venv venv
source venv/bin/activate
```

Finally, install the required dependencies in the activated virtual environment:

```bash
pip install confluent-kafka[avro,schemaregistry].
```

This will install Confluent's latest client library with the optional support for AVRO
and Kafka Schema Registry along with all their dependencies.


## Run

The example Python program called `railnova_kafka_example.py` requires three arguments
to connect to Railnova's Kafka broker and fetch a single message from a topic.

```bash
python railnova_kafka_example.py --username=... --password=... --topic=...
```

You should see the following log output:

```log
2025-01-10 10:58:08,638 INFO Schema registry is accessible at 'kafka-13e7abdf-test-railnova-5ffc.aivencloud.com'
2025-01-10 10:58:08,640 INFO Kafka consumer subscribed to topic 'output-sharing-********'
2025-01-10 10:58:12,126 WARNING OFFSET [rdkafka#consumer-1] [thrd:main]: output-sharing-**** [1]: offset reset (at offset 10436673 (leader epoch 122), broker 70) to offset BEGINNING (leader epoch -1): fetch failed due to requested offset not available on the broker: Broker: Offset out of range
2025-01-10 10:58:12,265 INFO Received {'type': 'A', 'id': 12838} -> {'type': 'merged', 'timestamp': '2025-01-04T13:30:38Z', 'asset': 12838, 'device': None, 'asset_uic': '************', 'is_open': True, 'content': '{"gps_time": "2025-01-04T13:30:38Z", "latitude": 51836109.16137695, "longitude": 4823332.786560059, "speed": 0, "course": 0, "fix": 1, "location": "Hardinxveld Blauwe Zoom", "country": "Netherlands", "total_km": 816234, "period_km": 0, "status": "parking", "status_last_change": "2022-09-30T22:40:07Z"}', 'version': None, 'recv_time': '2025-01-04 13:30:54', 'processed_time': '2025-01-04 13:30:54', 'source': None, 'header': {'nohistory': None, 'nomerge': None, 'job': None, 'nonotifications': None}}
```

By default, this program will connect to Railnova's `test` Kafka cluster.

To test connection to the production cluster you may specify a `--hostname`, for instance:

```bash
python railnova_kafka_example.py --hostname=kafka-prod-railnova-5ffc.aivencloud.com ...
```

By default, this programm will use `railnova_kafka_example` as its [consumer's group identifier](https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/). If you need to provide another, use the `--group-id` argument.


## Troubleshoot

This program uses the bundled `ca.pem` Certificate Authority to verify the certificate provided by the
Kafka broker.

If that Certificate Authority has been revoked or is otherwise invalid then you may see the
following output on STDERR:

```log
2025-01-10 11:03:16,527 ERROR FAIL [rdkafka#consumer-1] [thrd:sasl_ssl://kafka-13e7abdf-test-railnova-5ffc.aivencloud.com:272]: sasl_ssl://kafka-13e7abdf-test-railnova-5ffc.aivencloud.com:27257/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified, verify that ssl.ca.location is correctly configured or root CA certificates are installed (install ca-certificates package) (after 19ms in state SSL_HANDSHAKE)
```
