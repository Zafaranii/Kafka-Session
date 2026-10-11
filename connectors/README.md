# Sink connector: `notifications-sink-connector.json`

Registers a JDBC sink connector on the `kafka-connect` worker that consumes
`notification-requests` and writes each message into Postgres. This is the
"no custom code" leg of the streaming path — everything below is
declarative connector config, not a service we wrote.

## Registering it

```bash
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/notifications-sink-connector.json
```

Check status:

```bash
curl http://localhost:8084/connectors/notifications-sink-connector/status
```

## Config, key by key — every line is load-bearing

We kept this to the minimum set of keys that actually change behavior.
Nothing here just restates a Confluent default — if a default was fine,
we left the key out entirely rather than writing it down for documentation's
sake.

| Key | Value | Why it's required |
|---|---|---|
| `connector.class` | `io.confluent.connect.jdbc.JdbcSinkConnector` | Selects the connector implementation. No default. |
| `connection.url` | `jdbc:postgresql://postgres:5432/notifdb?reWriteBatchedInserts=true` | Target database. No default. `reWriteBatchedInserts=true` is a Postgres driver option: it rewrites a JDBC batch into multi-row statements, which is usually the biggest single throughput gain for a JDBC sink. |
| `connection.user` / `connection.password` | `notifuser` / `notifpass` | Credentials — matches `POSTGRES_USER`/`POSTGRES_PASSWORD` in `docker-compose.yml`. No default. |
| `topics` | `notification-requests` | Which topic to consume. No default. |
| `table.name.format` | `stream_notifications` | Without this, the connector targets a table named after the topic (`notification-requests`), which doesn't exist — our table is `stream_notifications` (see `init.sql`). Required because our table name deliberately doesn't match the topic name. |
| `insert.mode` | `upsert` | Default is `insert`. Kafka Connect sink connectors are **at-least-once**: a consumer-group rebalance or reprocessing can redeliver a message. With the default `insert` mode, a redelivered message becomes a duplicate row. `upsert` on the primary key makes redelivery safe (same row, overwritten, no duplicate). This is the behavior the demo is meant to highlight, not an incidental setting. |
| `pk.mode` | `record_value` | Default is `none` (no primary key used). Required for `upsert` to have something to upsert *on*. `record_value` says: take the primary key from a field of the message value. Not `record_key`: the key is the recipient (it decides the partition, so one person's notifications stay in order), and many notifications share a recipient. |
| `pk.fields` | `request_id` | The value field to use as the primary key — unique per notification, and `UNIQUE` in `stream_notifications`. |
| `tasks.max` | `3` | Default is `1`. The topic has 3 partitions, and a sink task consumes whole partitions, so 3 tasks write to Postgres in parallel. More than 3 would leave tasks idle. |
| `consumer.override.max.poll.records` | `3000` | Every sink task wraps a regular Kafka consumer, and the connector can only batch what one `poll()` returns. That consumer's `max.poll.records` defaults to 500, which silently caps each DB batch at 500 rows even though the connector's `batch.size` is 3000. Raising it lets batches reach `batch.size`. The `consumer.override.` prefix applies the setting to this connector only; it requires `CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY=All` on the worker (set in `docker-compose.yml`), because the default policy rejects overrides. |

## Left at their defaults on purpose (i.e., not written down as config)

- **`auto.create` / `auto.evolve`** — both default to `false` already. We don't
  want the connector creating or altering `stream_notifications` — the schema
  lives in `init.sql`, deliberately hand-authored (see the `PLAN.md` data
  model section). Writing `"false"` explicitly here would just restate the
  default, so we didn't.
- **`fields.whitelist`** — not set. `producer-api`'s JSON envelope payload
  today contains exactly `request_id` and `message`, i.e. exactly the columns
  we want inserted, so there's nothing to filter out. If a field is ever
  added to the payload that isn't a real column, the connector will fail
  fast (rather than silently dropping it via a whitelist someone forgot to
  update) — `auto.evolve: false` means that failure is loud, which is the
  behavior we want in a demo.
- **`batch.size`** — default `3000`, already large enough for the burst
  sizes used here; `max.poll.records` above is what actually limited it.
