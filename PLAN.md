# Notification Demo: Synchronous Request-Driven vs Event-Driven Asynchronous Processing

## Context

The existing project at `d:\Work\Workspaces\Kafka\Kafka` is an IBM ACE + Kafka + Debezium CDC demo for an Orders/Shipment pipeline. This is a **separate, self-contained demo** that makes a different point: showing, side by side, the difference between a **synchronous, request-driven call chain** and **event-driven asynchronous processing**, using "send a notification" as the running example.

**Framing note (per manager feedback):** this is *not* "microservices vs event streaming" — both paths are built out of microservices (the event-driven path has just as many independent services: `producer-api`, the sink connector, `notifier-consumer`). The real distinction is architectural style: does the client block on a chain of synchronous calls, or does it hand off an event and let processing happen asynchronously off a topic?

Concretely:
- **Synchronous, request-driven path**: a client calls a Notify API, which synchronously calls a DB service to persist the notification, then synchronously calls a WS service to push it to the browser over a WebSocket. Every hop blocks on the previous one and the caller waits for the whole chain to finish before getting a response.
- **Event-driven, asynchronous path**: a client calls a Producer API that publishes an event to Kafka and returns as soon as Kafka has **accepted** it — the response is a `202 Accepted` (event published), not a `200 OK` implying the notification has been processed or delivered ("What are HTTP status codes? Complete Guide for API Developers" is the reference the manager shared on this distinction: `202` signals "request accepted, processing continues asynchronously," which is exactly what happened here). Two independent things then happen off the same topic in parallel: a Kafka Connect **Postgres JDBC Sink connector** (off-the-shelf, no custom code) writes the message straight into Postgres, and a separate consumer service picks up the same message and pushes it to the browser over its own WebSocket. No service calls another service directly, and no custom code is needed for the DB write — everything is decoupled through the topic.

**The core point to prove (per manager feedback):** the demo is not claiming the event-driven path delivers the notification faster end-to-end than the sync path — it may or may not, depending on load. The point is narrower and more important: **downstream processing time (the DB write + the WS push) is removed from the client request's critical path.** The client gets its response the moment Kafka has durably accepted the event; everything after that is decoupled from what the client is waiting on. The demo also makes the **coupling/failure-mode** difference visible (a downed WS service breaks the whole sync request; a downed notifier consumer just delays delivery, the event waits in Kafka).

**Revision note (this update):** the event-streaming path is now explicitly staged as **Phase 1 (plain JSON)** followed by an optional **Phase 2 (+Avro / Schema Registry)** enhancement, rather than building on Avro from day one. This matches the diagrams and connector config already in the repo (see Status below) and lets the demo also make a *third* point on top of latency/coupling: **schema governance** — "by convention" JSON vs. registry-enforced Avro compatibility — as an upgrade you bolt on, not a prerequisite.

## Status (as of this update)

**Phase 1 is fully built, wired end-to-end, and tested — both paths.**

- `docker-compose.yml` — full stack: postgres, the three sync services,
  frontend, kafka, kafka-init (explicit topic creation), kafka-connect,
  kafka-ui, producer-api, notifier-consumer.
- `README.md` — how to run it, topic-creation rationale, comparison table,
  resilience-check walkthrough.
- Sync path (`services/sync/*`): `notify-api`, `db-service`, `ws-service` —
  built, CORS-enabled, and tested with a real browser and a scripted
  WebSocket client. `/notify` responses include a `timeline` of per-hop
  server timestamps; `ws-service`'s push payload includes `broadcast_at`.
  The frontend renders a per-request milestone breakdown (4 legs + total +
  a waterfall bar) rather than just ack/delivered.
- Streaming path (`services/streaming/*`): `producer-api` (produces the
  JSON-with-inline-schema envelope, `flush()`s before acking) and
  `notifier-consumer` (background-thread consumer, fixed `group.id`,
  broadcasts over `/ws`) — built and tested the same way, including the
  timeline/`broadcast_at` fields the frontend needs.
- `connectors/notifications-sink-connector.json` — registered against
  `kafka-connect` and confirmed `RUNNING`; a produced message was confirmed
  to land in `stream_notifications`. Trimmed to the minimum working config
  (9 keys → see `connectors/README.md` for why each one is there, and what
  was deliberately left at Confluent's defaults instead of restated).
- `kafka-connect/Dockerfile` — installs only `kafka-connect-jdbc` + the
  Postgres driver (no Avro converter yet — that's Phase 2).
- Topic creation is explicit, not auto-create — see `README.md#topic-creation`
  and the `kafka-init` service.
- **Resilience check verified directly, not just asserted**: killed
  `ws-service` — sync path breaks, as expected. Killed `notifier-consumer`,
  produced a message while it was down, confirmed the row still landed in
  Postgres via the sink connector (unaffected), restarted it, confirmed a
  WebSocket client received the message. Getting this actually reliable
  (not just "usually works if you wait") needed two fixes beyond the
  original plan — see `services/streaming/notifier-consumer/README.md`:
  a graceful shutdown (so the consumer cleanly leaves its Kafka consumer
  group instead of stalling the next instance's rebalance) and a small
  in-memory replay buffer (so a browser reconnecting a couple seconds late
  doesn't miss a message that already broadcast to zero listeners).
- Diagrams: `01-microservices-flow`, `02-event-streaming-flow`,
  `03-full-pipeline-comparison` (Phase 2, +Avro/Schema Registry),
  `04-full-pipeline-comparison-no-avro` — all built with `.mmd` sources and
  regenerated (`.svg`/`.png`) via `npx @mermaid-js/mermaid-cli`. `03` adds
  a `Schema Registry` node with `register/lookup schema id` (from
  `producer-api`) and `fetch schema by id` (from the JDBC sink connector
  and `notifier-consumer`) edges, mirroring `04`'s structure/styling and
  using the same `202 Accepted` / "Synchronous, Request-Driven" /
  "Event-Driven, Asynchronous" framing.

Not built yet — Phase 2 only:
- `schemas/notification.avsc` and the rest of the Avro/Schema Registry
  wiring (see the Phase 2 section below). The `03-full-pipeline-comparison`
  diagram depicts this target architecture; the running stack doesn't
  implement it yet.

**Manager feedback (see Context) — applied, still Phase 1 scope:**
- `producer-api`'s `POST /notify` now returns `202 Accepted` (was FastAPI's
  default `200 OK`), with `"status": "accepted"` in the body — the response
  means Kafka accepted the event, not that the notification was processed.
  Updated in `services/streaming/producer-api/main.py` and its `README.md`.
- Frontend (`frontend/index.html`) now reports two distinct metrics per
  request — **Client response** and **End-to-end delivery** — instead of a
  single blended "Total," so the divergence between them (small/flat on the
  event-driven panel, roughly equal to delivery on the sync panel) is
  visible directly in the table. Panel titles/legends updated to
  "Synchronous, Request-Driven" / "Event-Driven, Asynchronous" and the
  streaming panel's Kafka leg now reads "Kafka accept (202)".
- `README.md`'s title, intro, "what each panel demonstrates" section, and
  comparison table updated to the new framing and to call out that a
  `202`/client-response reading doesn't mean the notification was delivered.
- Diagrams `02-event-streaming-flow.mmd` and
  `04-full-pipeline-comparison-no-avro.mmd` updated (`200 OK (ack...)` →
  `202 Accepted (event published...)`, subgraph/title labels reframed) and
  regenerated (`.svg`/`.png`) via `npx @mermaid-js/mermaid-cli`.

Not verified yet (needs a live run): `docker compose up --build` with the
`202` change and the new frontend columns, end to end.

## Directory Layout

```
notification-demo/
├── docker-compose.yml
├── README.md
├── init.sql
├── kafka-connect/
│   └── Dockerfile          (Phase 1: JDBC sink plugin + Postgres driver only;
│                             Phase 2 adds the Avro converter here)
├── connectors/
│   └── notifications-sink-connector.json   (Phase 1 config; Phase 2 adds a
│                                             value.converter override, see below)
├── schemas/                 (Phase 2 only)
│   └── notification.avsc    (Avro record `Notification`: required strings
│                              `request_id`, `message`)
├── diagrams/
│   └── notification-pipeline/
│       ├── 01-microservices-flow.mmd          (sync chain, unchanged by phase)
│       ├── 02-event-streaming-flow.mmd        (Phase 1: JSON w/ inline schema)
│       ├── 03-full-pipeline-comparison.png    (Phase 2: +Avro/Schema Registry)
│       └── 04-full-pipeline-comparison-no-avro.mmd  (Phase 1: JSON only)
├── frontend/
│   └── index.html
└── services/
    ├── sync/
    │   ├── notify-api/      (FastAPI: POST /notify orchestrates the chain)
    │   ├── db-service/      (FastAPI: POST /write inserts into Postgres)
    │   └── ws-service/      (FastAPI: WebSocket /ws to browser, POST /push to broadcast)
    └── streaming/
        ├── producer-api/      (FastAPI: POST /notify -> Kafka produce, returns immediately)
        └── notifier-consumer/ (FastAPI: WebSocket /ws to browser + background Kafka consumer on notification-requests)
```

Each Python service gets its own `main.py`, `requirements.txt`, and `Dockerfile`.

## Data Model (`init.sql`) — already built, matches this exactly

One Postgres instance, two tables so each path writes to its own, independent table:

```sql
CREATE TABLE sync_notifications (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE stream_notifications (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL UNIQUE,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
```

`request_id` is `UNIQUE` on `stream_notifications` so the sink connector can upsert on redelivery (see delivery-semantics note below) instead of silently duplicating rows.

Postgres runs with `command: ["postgres", "-c", "wal_level=logical"]`, same as the existing project's `docker-compose.yml`.

## Synchronous, request-driven path — unchanged by phase

1. **notify-api** (`POST /notify`, body `{request_id, message}`): calls `db-service` over HTTP, waits for the response, then calls `ws-service` over HTTP, waits for that response, then returns to the caller. Uses `httpx` for outbound calls. This is the "chain of synchronous hops" — every step is on the critical path and a failure anywhere aborts the whole request.
2. **db-service** (`POST /write`): inserts a row into `sync_notifications` and returns the inserted row.
3. **ws-service**: holds live WebSocket connections from the frontend at `/ws`; `POST /push {request_id, message}` broadcasts a JSON message to all connected clients and returns once the broadcast is sent.

## Event-driven, asynchronous path — Phase 1: JSON (build this first)

Note: this path is still built out of independent microservices (`producer-api`, the sink connector, `notifier-consumer`) — what makes it "event-driven" is that none of them call each other directly or block the client on each other's work; they're all decoupled through the topic.

1. **producer-api** (`POST /notify`, body `{request_id, message}`): produces to the `notification-requests` Kafka topic using `confluent-kafka` and returns as soon as the broker has accepted the message — it does not wait on anything downstream (DB write, WS push). **Responds `202 Accepted`, not `200 OK`**: the response means "Kafka has accepted this event," not "the notification has been processed or delivered" — those happen asynchronously afterward. The message **key** is the plain `request_id` string (`StringSerializer`). The message **value** is plain JSON, but wrapped in Kafka Connect's schema envelope (`{"schema": {...}, "payload": {"request_id": ..., "message": ...}}`) so the JDBC sink connector can infer column types without a Schema Registry — this is the "inline schema" referenced in the diagrams.
2. **Postgres JDBC Sink connector** (`connectors/notifications-sink-connector.json`, already built): `io.confluent.connect.jdbc.JdbcSinkConnector` consuming `notification-requests` and upserting into `stream_notifications` — `insert.mode: upsert`, `pk.mode: record_key`, `pk.fields: request_id`, `auto.create`/`auto.evolve` off, `fields.whitelist: request_id,message`. No per-connector converter override — it uses the Kafka Connect worker's default converters, which must be set to `CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter` with `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=true` in `docker-compose.yml`. Zero custom code — purely declarative.
3. **notifier-consumer**: a FastAPI app that runs a Kafka consumer for `notification-requests` — a second, independent consumer group on the *same* topic, with a **fixed `group.id`** so that killing and restarting it resumes from its last committed offset instead of joining as a fresh group (this is what makes the "kill it, restart it, message still arrives" resilience check in Verification actually work) — in a background thread, and exposes `/ws` for the frontend. It parses the plain JSON envelope (unwraps `payload`) and broadcasts `{request_id, message}` to connected WebSocket clients.

The sink connector and `notifier-consumer` both read the same topic independently; neither knows the other exists. No service ever calls another service directly — `notifier-consumer` being briefly down just delays that consumer group's delivery (it resumes from its committed offset) instead of failing the request or losing the write to Postgres.

`kafka-connect/Dockerfile` (already built) extends `confluentinc/cp-kafka-connect-base:7.6.1` with `confluent-hub install confluentinc/kafka-connect-jdbc:10.7.6` plus the PostgreSQL JDBC driver jar — no Avro converter needed for Phase 1.

**Delivery-semantics callout (intentional, documented in README):** the sink connector is at-least-once, like any Kafka consumer — a rebalance or reprocessing can redeliver a message. Upserting on `request_id` makes that safe here (same row, no duplicate) whereas the naive default (`insert.mode: insert`, `pk.mode: none`) would silently insert duplicate rows on redelivery. This is worth surfacing verbally in the demo ("event streaming gives you at-least-once by default; deduplication is something *you* design for") even though the connector config itself is already the safe/correct choice.

## Event-driven, asynchronous path — Phase 2: +Avro / Schema Registry (optional follow-on)

Once Phase 1 works end-to-end, layer in schema governance without changing the topic, the tables, or the sync path:

1. Add a `schema-registry` service (`confluentinc/cp-schema-registry`) to `docker-compose.yml`, pointed at the broker.
2. Add `schemas/notification.avsc` (Avro record `Notification`: required strings `request_id`, `message`).
3. Rebuild `kafka-connect/Dockerfile` to also `confluent-hub install confluentinc/kafka-connect-avro-converter`.
4. Switch `producer-api` from the JSON envelope to `AvroSerializer` against Schema Registry using `notification.avsc`.
5. Switch `notifier-consumer` from plain-JSON parsing to `AvroDeserializer` (same Schema Registry client config).
6. Add a per-connector override to `connectors/notifications-sink-connector.json`: `value.converter: io.confluent.connect.avro.AvroConverter`, `value.converter.schema.registry.url: http://schema-registry:8081` (the connector's own config wins over the worker default, so Phase 1's worker-level JSON default can stay as-is for anything else registered on the worker).
7. Regenerate `03-full-pipeline-comparison.png`'s `.mmd` source (currently missing) alongside this work, mirroring how `04-full-pipeline-comparison-no-avro.mmd` was built, so both comparison diagrams have sources.

This is what `03-full-pipeline-comparison.png` already depicts (Schema Registry box, `register/lookup schema id` / `fetch schema by id` edges) — Phase 2 just needs the compose/service work to catch up to that diagram.

## Frontend (`frontend/index.html`)

Single static page, two side-by-side panels ("Synchronous, Request-Driven" / "Event-Driven, Asynchronous"), vanilla JS, no build step:
- Opens two WebSocket connections on load: one to `ws-service` (sync), one to `notifier-consumer` (event-driven).
- Each panel has a "Send Notification" button. On click: generate a `request_id` (`crypto.randomUUID()`), record `sendTs = performance.now()`, POST to the panel's API (`notify-api` or `producer-api`), and record the time the HTTP call resolves (`responseTs`).
- When a WebSocket message with a matching `request_id` arrives, record `deliverTs`.
- **Two metrics, captured and shown for both panels (per manager feedback):**
  - **Client response time** = `responseTs - sendTs` — how long the client's request was actually blocked. On the sync panel this is `notify-api`'s full chain (DB write + WS push both already done). On the event-driven panel this is only the time for Kafka to accept the event (`202 Accepted`) — the DB write and WS push haven't happened yet.
  - **End-to-end delivery time** = `deliverTs - sendTs` — when the notification actually reaches the browser over the WebSocket, regardless of path.
- Render a running log/table per panel: Request ID (short), client response time (ms), end-to-end delivery time (ms). **What this is meant to show (per manager feedback): on the sync panel these two numbers are the same** — the response *is* the delivery, because every downstream step is on the critical path. **On the event-driven panel they diverge** — the client response comes back as soon as Kafka accepts the event, while delivery happens later once `notifier-consumer` processes it. The point being demonstrated is that downstream processing time (DB write + WS push) has been removed from the client's request critical path — not that the event-driven path's end-to-end delivery number is necessarily smaller than the sync path's.

## Kafka topics & ports

Run alongside the existing project's stack, so use distinct host ports:
- postgres: `5434:5432`
- kafka: `9094:9092` (broker/controller listener setup mirrors the existing `docker-compose.yml`)
- kafka-ui: `8081:8080`
- schema-registry: `8085:8081` (`confluentinc/cp-schema-registry` — **Phase 2 only**, not needed to run Phase 1)
- kafka-connect: `8084:8083`
- notify-api: `8004` (moved off `8001` — that port was already bound by an unrelated process on this machine), db-service: `8002`, ws-service: `8003`
- producer-api: `8011`, notifier-consumer: `8013`
- frontend: served by a small `nginx:alpine` (or `python:3.12-slim` + `http.server`) container on `8090`

Compose dependency ordering: for Phase 1, `producer-api`, `notifier-consumer`, and `kafka-connect` each depend on `kafka` (healthy) — no `schema-registry` dependency needed. Once Phase 2 is added, `schema-registry` depends on `kafka` (healthy), and `producer-api`, `notifier-consumer`, and `kafka-connect` each additionally depend on `schema-registry` (healthy).

Topic: `notification-requests` — consumed independently by the JDBC sink connector and by `notifier-consumer` (two separate consumer groups), auto-created (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`, matching the existing compose file).

## README / diagrams

`README.md` explains how to run (`docker compose up --build`, then register the sink connector against `kafka-connect:8083/connectors` the same way the existing project registers `orders-connector.json`), what each panel demonstrates, and a short comparison table (client response semantics [`200 OK` = processed vs. `202 Accepted` = published], critical-path composition, coupling, failure behavior, scalability, amount of custom code, and **schema governance**: no contract between `notify-api` and `db-service` on the sync side — they just agree on an HTTP JSON shape by convention — vs. Phase 2's registry-enforced Avro compatibility on the event-driven side). The framing throughout is **Synchronous Request-Driven vs Event-Driven Asynchronous Processing**, not "microservices vs event streaming" — both sides are microservices.

Diagrams (in `diagrams/notification-pipeline/`, mirroring the `mermaid-cli` convention from the existing project):
- `01-microservices-flow.mmd` — sequence diagram of the sync chain. **Built.**
- `02-event-streaming-flow.mmd` — sequence diagram of Phase 1 (JSON). **Built.**
- `04-full-pipeline-comparison-no-avro.mmd` — full side-by-side flowchart, Phase 1 only. **Built.**
- `03-full-pipeline-comparison.mmd` — full side-by-side flowchart, Phase 2 (+Avro/Schema Registry), with a `Schema Registry` node and `register/lookup`/`fetch schema by id` edges. **Built.**
- File names/labels above are legacy from before the reframing; not worth renaming the `.mmd` files themselves, but their in-diagram labels and any "Microservices"/"Event Streaming" titles now read "Synchronous Request-Driven" / "Event-Driven Asynchronous Processing", and the event-driven diagrams' response arrows say `202 Accepted` rather than `200 OK`.

## Verification

- `docker compose up --build` brings up the Phase 1 stack; confirm all containers report healthy/running.
- Register the sink connector via `curl -X POST localhost:8084/connectors -d @connectors/notifications-sink-connector.json`, confirm it's in `RUNNING` state and a test message lands in `stream_notifications`.
- Open `http://localhost:8090`, click "Send Notification" in each panel, confirm the WebSocket message arrives in both, `producer-api` responds `202 Accepted`, and both the client response time and end-to-end delivery time render for each panel.
- Kill `ws-service` mid-demo and show the sync panel's request fails/hangs; kill `notifier-consumer`, send a notification, then restart it and show the message still arrives (delayed) once it resumes from its committed offset — while the sink connector kept writing to Postgres the whole time, unaffected. That's the core "complexity vs. resilience/decoupling" point of the demo.
- **The headline comparison to walk through (per manager feedback):** on the sync panel, client response time ≈ end-to-end delivery time (the whole chain is on the critical path). On the event-driven panel, client response time is small and roughly load-independent, while end-to-end delivery time is separate and can vary — because DB write + WS push happen *after* the client already has its `202 Accepted`. The takeaway is "downstream processing time is off the critical path," not "the notification arrives faster overall."
- After Phase 2 is layered in: repeat the same checks, and additionally show the Schema Registry UI/API (`register/lookup schema id`, `fetch schema by id`) and a rejected incompatible schema change, to land the schema-governance point.
