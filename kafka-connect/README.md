# Kafka Connect worker image

`Dockerfile` extends `confluentinc/cp-kafka-connect-base:7.6.1` with exactly
two additions:

1. `confluent-hub install confluentinc/kafka-connect-jdbc:10.7.6` — the JDBC
   sink connector plugin used by `../connectors/notifications-sink-connector.json`.
2. The PostgreSQL JDBC driver jar, dropped directly into the plugin's `lib/`
   folder — the JDBC connector needs a driver for whichever database it's
   talking to; Confluent doesn't bundle one.

Nothing else is installed. In particular, no Avro converter — that's a
Phase 2 addition once Schema Registry is introduced (see `PLAN.md`).

## Worker-level environment (`docker-compose.yml`)

The connect worker's own `CONNECT_*` env vars in `docker-compose.yml` are the
mandatory baseline the `cp-kafka-connect-base` image requires to start at
all — not a "least needed configs" choice the way the connector JSON is
(there's no smaller working set for a Connect worker). The two worth calling
out specifically:

- `CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter` —
  message keys are plain strings (`request_id`), not JSON/Avro.
- `CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter` with
  `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=true` — `producer-api` sends each
  value as a JSON envelope (`{"schema": ..., "payload": ...}`); the `schemas.enable`
  flag tells the worker to expect that envelope and read the schema from it,
  which is what lets the JDBC sink connector build a typed `INSERT`/`UPSERT`
  without a Schema Registry. This is the "JSON w/ inline schema" labeled in
  `diagrams/notification-pipeline/02-event-streaming-flow.mmd`.

The three `CONNECT_*_REPLICATION_FACTOR=1` settings exist only because this
is a single-broker cluster — Kafka Connect's internal topics
(`connect_configs`, `connect_offsets`, `connect_statuses`) default to
replication factor 3, which a one-broker cluster can never satisfy.
