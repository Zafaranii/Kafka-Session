# Notification Demo: Synchronous Request-Driven vs Event-Driven Asynchronous Processing

Two ways to build "send a notification, persist it, push it to the browser"
side by side: a synchronous, request-driven call chain, and an event-driven,
asynchronous pipeline built on Kafka. Both sides are built out of
independent microservices — the distinction isn't "microservices vs event
streaming," it's whether the client blocks on a chain of synchronous calls
or hands off an event and gets a response once Kafka has merely *accepted*
it. See `PLAN.md` for the full design and rationale; this file is the
practical "how to run it" + "why is each piece configured the way it is"
reference.

## Running it

```bash
docker compose up -d --build
```

Then register the JDBC sink connector (not automatic — Kafka Connect
connectors are registered via its REST API, not compose):

```bash
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/notifications-sink-connector.json
```

Open the frontend: **http://localhost:8090**

| Service | Port |
|---|---|
| frontend | 8090 |
| notify-api (sync) | 8004 |
| db-service (sync) | 8002 |
| ws-service (sync) | 8003 |
| producer-api (streaming) | 8011 |
| notifier-consumer (streaming) | 8013 |
| postgres | 5434 |
| kafka (host listener) | 9094 |
| kafka-ui | 8081 |
| kafka-connect | 8084 |

## Topic creation

`notification-requests` is created **explicitly**, not implicitly. The
broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, and a one-shot
`kafka-init` container runs on startup:

```bash
kafka-topics.sh --create --if-not-exists \
  --topic notification-requests \
  --bootstrap-server broker:19092 \
  --partitions 1 \
  --replication-factor 1
```

(see the `kafka-init` service in `docker-compose.yml` — `producer-api`,
`notifier-consumer`, and `kafka-connect` all `depends_on: kafka-init:
condition: service_completed_successfully`, so nothing tries to
produce/consume/sink before the topic exists.)

**Why explicit instead of auto-create**, and **why these exact flags**:

- Auto-create is convenient but invisible — it hides the one genuinely
  interesting design decision a topic has (partition count) behind "it just
  appeared the first time something touched it." Making it a real step means
  there's something to point at and explain.
- `--partitions 1` — the minimum. More partitions would let multiple
  consumer *instances* in the same group split the load, but neither
  consumer here needs that: the JDBC sink connector's `tasks.max` defaults
  to `1` anyway (see `connectors/README.md`), and `notifier-consumer` runs
  as a single instance. One partition also means strict per-key ordering
  across the whole topic, which is a nice property to have for free in a
  demo and costs nothing at this scale.
- `--replication-factor 1` — the minimum, because there's exactly one
  broker in this cluster. Replication factor can't exceed broker count;
  this isn't a "least needed configs" choice so much as the only legal
  value here. In a real multi-broker cluster you'd want at least 2-3 for
  durability — the topic-loses-nothing-if-a-broker-dies property doesn't
  exist at RF=1.

Same minimal-config philosophy as the sink connector — see
`connectors/README.md` for that side of it.

## What each panel demonstrates

- **Synchronous, Request-Driven**: `notify-api` → `db-service` → `ws-service`,
  every hop blocking, caller waits for the whole chain. See
  `services/sync/*/README.md` and the per-hop timing breakdown in the UI.
- **Event-Driven, Asynchronous**: `producer-api` publishes and returns
  `202 Accepted` as soon as Kafka has the event — not once it's been
  processed or delivered. The JDBC sink connector and `notifier-consumer`
  are two independent, decoupled readers of the same topic — neither knows
  the other exists, and both do their work *after* the client already has
  its response. See `services/streaming/*/README.md` and
  `connectors/README.md`.

**The point being demonstrated is not "the event-driven path delivers
faster overall."** It's narrower: downstream processing time (the DB write
+ the WS push) is removed from the client request's critical path. The UI
shows this directly by reporting two separate numbers per request —
**client response time** and **end-to-end delivery time** — instead of one
blended total. On the sync panel they're roughly equal (the whole chain *is*
the critical path); on the event-driven panel they diverge (response comes
back the moment Kafka accepts the event, delivery happens later).

| | Synchronous, Request-Driven | Event-Driven, Asynchronous |
|---|---|---|
| Client response means | the notification has been persisted **and** delivered | Kafka has **accepted** the event (`202 Accepted`) — not processed or delivered |
| Caller waits for | the whole chain (DB write + WS push) | just the broker's accept |
| Coupling | `notify-api` calls the other two directly | nothing calls anything; both consumers just read the topic |
| If the delivery leg is down | the whole request fails | the DB write still succeeds; delivery is delayed, not lost |
| Redelivery/duplicates | not applicable (no retries in the chain) | at-least-once by default — `upsert` on `request_id` makes it safe (see `connectors/README.md`) |
| Schema contract | none — HTTP JSON shape "by convention" between services | Phase 1: inline JSON schema envelope. Phase 2 (optional): Schema Registry-enforced Avro compatibility — see `PLAN.md` |
| Custom code for persistence | `db-service` (hand-written) | none — JDBC sink connector, purely declarative |

## Resilience check (live demo)

1. Kill `ws-service` mid-demo (`docker compose stop ws-service`) → the sync
   panel's request now fails/hangs, because `notify-api` calls it directly
   and waits.
2. Kill `notifier-consumer` (`docker compose stop notifier-consumer` — use
   `stop`, not `kill`/`docker kill`; the graceful shutdown is what makes the
   next step fast, see below), send a notification, then restart it
   (`docker compose start notifier-consumer`) → the message still arrives,
   within a second or two of the reconnect, once it resumes from its last
   committed offset (fixed `group.id` — see
   `services/streaming/notifier-consumer/README.md`, which also covers the
   two things that had to be fixed to make this actually reliable rather
   than a race: a graceful shutdown that leaves the consumer group cleanly,
   and a small replay buffer that covers the browser's own reconnect delay).
   The sink connector kept writing to Postgres the entire time, unaffected
   — that's the coupling/failure-mode point in one demo. Verified directly:
   produced a message while the consumer was down, confirmed the row landed
   in `stream_notifications` via the sink connector regardless, restarted
   the consumer, confirmed a WebSocket client received it.

## Project layout

See `PLAN.md` for the full directory layout, data model, and phase
breakdown (Phase 1: plain JSON — what's running now; Phase 2: optional
Avro/Schema Registry upgrade).
