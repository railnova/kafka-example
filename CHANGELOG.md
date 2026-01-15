# Changelog

## 2026-01-13

### Changed

- Add a `railnova_kafka_ingest.py` example to showcase the production of telematic events in an ingest topic
  (see issue [#9](https://github.com/railnova/kafka-example/issues/9)).

## 2026-01-12

### Changed

- Use REST endpoint of Karapace to resolve AVRO schemas (see issue [#8](https://github.com/railnova/kafka-example/issues/8)).
- Updated the `README.md` to account for added dependencies: `requests` and `types-confluent-kafka`.
- Let sources pass `basic` type checking using Pylance.

## 2025-02-04

### Added

- the latest schema for output sink values, as `85.json`.

## 2025-01-17

### Added

- A `railnova_kafka_mtls.py` program to access the Kafka broker using mTLS instead of SASL.
- A `railnova_kafka_nosr.py` program to access the Kafka broker using mTLS without Schema Registry.
- All the AVRO schemas required for Railnova's output sharing topics in a `schemas` folder.

### Changed

- The program `railnova_kafka_example.py`'s help now specify its use of SASL.
- The `README.md` has been updated to reflect changes above.

## 2025-01-14

### Added

- This `CHANGELOG.md` file.

### Changed

- The `README.md` has been updated to reflect all changes below.
- The `ca.pem` Certificate Authority's self-signed certificate is bundled.
- The example program `main.py` has been renamed `railnova_kafka_example.py`.
- The dependencies have been reduced to `confluent-kafka[avro,schemaregistry]` but upgraded to version `2.8.0`.
- Usage of the experimental `DeserializingConsumer` API has been replaced by `AvroDeserializer` and `SchemaRegistryClient`.
- Settings have been replaced by command line arguments, using the standard `argparse` module.
- Proper logging has been added, using the standard `logging` module.
- The program sources are more extensively commented.

### Removed

- The `requirements.txt` file, as dependencies are now reduced to `confluent-kafka[avro,schemaregistry]`.
- The `settings.py` which were replaced by command line arguments.
