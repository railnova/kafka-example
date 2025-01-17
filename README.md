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
pip install confluent-kafka[avro,schemaregistry]==2.8.0
```

This will install version `2.8.0` of Confluent's client library with the optional support for AVRO
and Kafka Schema Registry along with all their dependencies.


## Run - SASL Authentication

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


## Run - mTLS Authentication

Another example Python program called `railnova_kafka_mtls.py` uses SSL client key
and certificate to connect to Railnova's Kafka broker and fetch a single message from a topic.

```bash
python railnova_kafka_mtls.py --username=... --password=... --topic=... --key=... --certificate=...
```

The `--key` and `--certificate` arguments are respectively the locations to the `service.key`
and `service.cert` files supplied separately.

Note that a user name and a password are still required, but only to connect to the Kafka Schema Registry.

You should see the following log output:

```log
2025-01-17 09:29:28,049 INFO Schema registry is accessible at 'kafka-13e7abdf-test-railnova-5ffc.aivencloud.com'
2025-01-17 09:29:28,052 INFO Kafka consumer subscribed to topic 'output-sharing-******'
2025-01-17 09:29:35,407 INFO Received {'type': 'A', 'id': 10752} -> {'type': 'analog', 'timestamp': '2025-01-14T11:14:04Z', 'asset': 10752, 'device': 4006, 'asset_uic': None, 'is_open': True, 'content': '{"fuel_gauge_volt": 0.006280615626568774, "main_battery": 26.696541797683896, "main_battery_volt": 26.696541797683896, "support_battery": 25.100480351582107, "support_battery_volt": 25.100480351582107}', 'version': None, 'recv_time': '2025-01-14T11:14:06Z', 'processed_time': '2025-01-14 11:14:06', 'source': 'railster-pipe', 'header': {'nohistory': None, 'nomerge': None, 'job': None, 'nonotifications': None}}
```

## Run - mTLS Authentication and no Schema Registry

If you do not want to distribute user name and password, there is a third example Python program 
called `railnova_kafka_nosr.py` that uses SSL client key and certificate to connect to Railnova's
Kafka broker but which does not depend on the Kafka Schema Registry to decode AVRO payloads.

```bash
python railnova_kafka_nosr.py --topic=... --key=... --certificate=...
```

As above the `--key` and `--certificate` arguments are respectively the locations to the `service.key`
and `service.cert` files supplied separately. But since it does not depend on the Schema Registry, 
user name and password are not required.

You should see the following log output:

```log
2025-01-17 10:07:37,158 INFO Kafka consumer subscribed to topic 'output-sharing-******'
2025-01-17 10:07:43,207 INFO Received {'type': 'A', 'id': 10752} -> {'type': 'telematic', 'timestamp': '2025-01-14T11:14:17Z', 'asset': 10752, 'device': 4006, 'asset_uic': None, 'is_open': True, 'content': '{"decoded_as": "bb75000", "state": 2, "status": "parking", "status_last_change": "2025-01-14T09:08:29Z"}', 'version': None, 'recv_time': '2025-01-14T11:14:18Z', 'processed_time': '2025-01-14 11:14:19', 'source': None, 'header': {'nohistory': None, 'nomerge': None, 'job': None, 'nonotifications': None}}
```

If an unknown schema is used in the topic consumed, the exception below will be raised:

```
ValueError: Unknown schema id: ...
```

Make sure to update to the latest version of this repository to have all the schemas in use by Railnova.


## Troubleshoot

This program uses the bundled `ca.pem` Certificate Authority to verify the certificate provided by the
Kafka broker.

If that Certificate Authority has been revoked or is otherwise invalid then you may see the
following output on STDERR:

```log
2025-01-10 11:03:16,527 ERROR FAIL [rdkafka#consumer-1] [thrd:sasl_ssl://kafka-13e7abdf-test-railnova-5ffc.aivencloud.com:272]: sasl_ssl://kafka-13e7abdf-test-railnova-5ffc.aivencloud.com:27257/bootstrap: SSL handshake failed: error:0A000086:SSL routines::certificate verify failed: broker certificate could not be verified, verify that ssl.ca.location is correctly configured or root CA certificates are installed (install ca-certificates package) (after 19ms in state SSL_HANDSHAKE)
```
