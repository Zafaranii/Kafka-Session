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

Before a session, put the demo in its starting state with one command:

```bash
scripts/prepare-demo.sh
```

It tears the stack down (fresh Kafka log and Postgres), builds and starts
it, registers the JDBC sink connector, creates the `email-sender-1..4`
containers without starting them, and waits until the connector is running
and every partition has 3 in-sync replicas.

The presenters' walkthrough, act by act and mapped to the deck, is in
**[`DEMO-SCRIPT.md`](DEMO-SCRIPT.md)**.

Open the frontend: **http://localhost:8090** (the **Inside Kafka** page is
http://localhost:8090/kafka.html).

Doing it by hand instead: `docker compose up -d --build`, then register the
connector (Kafka Connect connectors are registered via its REST API, not
compose):

```bash
curl -X PUT http://localhost:8084/connectors/notifications-sink-connector/config \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json; print(json.dumps(json.load(open("connectors/notifications-sink-connector.json"))["config"]))')"
```

| Service | Port |
|---|---|
| frontend | 8090 |
| notify-api (sync) | 8004 |
| db-service (sync) | 8002 |
| ws-service (sync) | 8003 |
| producer-api (streaming) | 8011 |
| notifier-consumer (streaming) | 8013 |
| postgres | 5434 |
| kafka-1 / kafka-2 / kafka-3 (host listeners) | 9094 / 9095 / 9096 |
| kafka-inspector (read-only cluster view for the UI) | 8098 |
| control-api (stop/start buttons) | 8099 |
| load-runner (behind the Run Load Test button) | 8097 |
| kafka-ui | 8081 |
| kafka-connect | 8084 |

## Topic creation

`notification-requests` is created **explicitly**, not implicitly. The
broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, and a one-shot
`kafka-init` container runs on startup:

```bash
kafka-topics.sh --create --if-not-exists \
  --topic notification-requests \
  --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
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
- `--partitions 3` — lets the JDBC sink connector run 3 parallel tasks
  (`tasks.max: 3` in `connectors/README.md`), one per partition, so the
  topic drains into Postgres faster. `producer-api` keys each message by
  `recipient`, so every notification for the same person lands in the same
  partition, in order; ordering is only guaranteed per key, not across the
  whole topic. (The sink upserts on `request_id`, taken from the value.)
  `notifier-consumer` still runs as a single instance (it holds its
  WebSocket sessions in memory), so it simply owns all 3 partitions. Note
  `--if-not-exists`: on an existing stack the topic is not changed; run
  `kafka-topics.sh --alter --topic notification-requests --partitions 3`
  once, or recreate the volumes.
- `--replication-factor 3` with `min.insync.replicas=2` — the cluster has
  three brokers (KRaft, each one both broker and controller), so every
  partition has a leader and two followers. `producer-api` uses
  `acks=all`: a write is acknowledged only once the in-sync replicas have
  it, and refused if fewer than 2 are in sync. So one broker can be stopped
  live without losing or refusing anything (deck slides 16 and 19); with
  two down, writes are refused rather than accepted onto a single copy.
  Three containers on one laptop simulate fault tolerance — they share one
  machine, so this is not real high availability.

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
| Schema contract | none — HTTP JSON shape "by convention" between services | inline JSON schema envelope (Kafka Connect's JSON-with-schema format) |
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

## Making Kafka visible

Two pieces exist only so the audience can see what Kafka is doing, not take
it on trust:

- **Lag badge** on the event-driven panel: how many records each consumer
  group hasn't processed yet (log-end offset minus committed offset).
  Stopping `notifier-consumer` makes its number climb while db-sink stays
  at 0.
- **Inside Kafka** page (`frontend/kafka.html`): the record a send produced
  (key, value, headers, partition, offset, timestamp); each partition as an
  append-only log with every group's "next offset" pointer; the three
  brokers with leader/follower/ISR per partition and stop/start buttons; and
  the consumer groups with members, assigned partitions, lag and throughput,
  including the `email-sender` group that can be scaled from 0 to 4
  instances.

The **Run Load Test** button is driven by `services/load-runner`, which
plays the business service: it calls notify-api on the sync path, and
publishes straight to Kafka (waiting for each event's own acks=all
acknowledgement) on the event-driven path, with the same number of requests
in flight on both. See the docstring in `services/load-runner/main.py`.
`scripts/load-test.mjs` is the older CLI version that goes through
producer-api over HTTP.

The lag badge and the Inside Kafka page both read from `services/kafka-inspector`, which runs every Kafka client
call in a worker process — see `services/kafka-inspector/isolated.py` for
the librdkafka crash that requires it.

## Project layout

See `PLAN.md` for the original design, data model and directory layout.
Added since: the 3-broker cluster, `services/kafka-inspector`,
`services/load-runner`,
`services/streaming/email-sender`, `frontend/kafka.html`,
`scripts/prepare-demo.sh` and `DEMO-SCRIPT.md`.
