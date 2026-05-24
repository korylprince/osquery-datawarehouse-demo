# Flink Osquery Job

Apache Flink streaming job that ingests osquery logs from Kafka and writes them to Apache Iceberg tables via the Dynamic Iceberg Sink. Each osquery table name becomes a separate Iceberg table, with automatic schema evolution as new query columns appear.

## Architecture

```
osquery agents ──► Kafka ──► Flink Job ──► Iceberg (S3/Garage)
                                      └──► Polaris Catalog (REST)
```

## Configuration

All configuration is passed via environment variables.

### Required

| Variable | Description |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address(es) |
| `KAFKA_TOPIC` | Kafka topic containing osquery JSON logs |
| `POLARIS_URI` | Polaris Iceberg REST catalog URI |
| `POLARIS_WAREHOUSE` | Warehouse name in Polaris |
| `POLARIS_CLIENT_ID` | Polaris OAuth2 client ID |
| `POLARIS_CLIENT_SECRET` | Polaris OAuth2 client secret |
| `S3_ENDPOINT` | S3-compatible storage endpoint (e.g. Garage) |
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |

### Optional

| Variable | Default | Description |
|---|---|---|
| `KAFKA_GROUP_ID` | `flink-osquery-job-iceberg` | Kafka consumer group ID |
| `ICEBERG_DATABASE` | `default` | Iceberg namespace for output tables |
| `PARSER_PARALLELISM` | `2` | Number of parallel parsing subtasks |

## Building

```bash
docker build -t flink-osquery-job .
```

The image is based on `flink:2.0.1-java17` and bundles the Iceberg Flink runtime, Kafka connector, AWS S3 bundle, Hadoop client, and PostgreSQL JDBC driver at build time.

## Running

This job is expected to be run by the Flink Kubernetes Operator.
