# Changelog

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
