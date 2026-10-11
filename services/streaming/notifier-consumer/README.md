# notifier-consumer

Independent Kafka consumer + WebSocket bridge. Polls `notification-requests`
in a background thread and broadcasts each message to connected browser
clients over `/ws`. This is one of two independent readers of the topic —
the other is the JDBC sink connector (see `../../../connectors/README.md`).
Neither knows the other exists; that's the point of the demo.

## Why a fixed `group.id`

`GROUP_ID` defaults to the literal `notifier-consumer`, not a random/generated
ID. A consumer group's committed offsets are keyed by group ID — a fixed ID
means that if this container is killed and restarted, it rejoins the *same*
group and resumes from its last committed offset instead of starting fresh
(which, with `auto.offset.reset=earliest`, would replay the entire topic
from the start). This is what makes the demo's resilience check true: kill
`notifier-consumer`, send a notification, restart it, and the message still
arrives — delayed, not lost — while the sink connector kept writing to
Postgres the whole time, unaffected.

Two more things were needed to make that actually reliable in practice, not
just eventually-true-if-you-wait-long-enough (both were found by testing the
restart scenario directly, not assumed):

- **Graceful shutdown (`KafkaConsumerLoop.stop()` + a JVM shutdown hook)**.
  The Kafka consumer runs on a dedicated thread; a plain `docker stop` kills
  the process without that thread ever calling `consumer.close()`. Without a
  clean `LeaveGroup`, the broker doesn't realize the old member is gone
  until its session times out — the *new* instance's partition assignment
  (and therefore any delivery) stalls until then, which is far longer than a
  demo restart should take. `Runtime.addShutdownHook` flips the stop flag
  and joins the consumer thread (up to 8s, inside Docker's default 10s stop
  grace period) so `consumer.close()` actually runs and the new instance is
  reassigned the partition immediately.
- **A small in-memory replay buffer (`NotifyHub`'s `recentMessages`)**.
  Restarting this container drops every browser WebSocket connected to it —
  they live in this process, not the broker. The browser's reconnect
  (`frontend/index.html` retries every 1.5s) and the consumer resuming are
  two independent timers; the consumer can broadcast the backlogged message
  before the browser reconnects, so a live-only broadcast would silently
  lose it from the browser's point of view even though Kafka never lost it.
  Every message gets appended to a bounded deque, and every new/reconnecting
  `/ws` connection is replayed the current buffer before joining the set of
  live sessions — so a reconnect landing even a few seconds late still
  sees it.

## Message shape

Reads the same JSON-with-schema envelope `producer-api` writes
(`{"schema": ..., "payload": {"request_id", "message"}}`), unwraps
`payload`, adds its own `broadcast_at` timestamp (mirroring `ws-service`'s
pattern on the sync side — see `services/sync/ws-service/main.py`), and
sends that over the WebSocket:

```json
{ "request_id": "...", "message": "...", "broadcast_at": <epoch ms> }
```

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-1:19092,kafka-2:19092,kafka-3:19092` | The three brokers' internal listeners. |
| `NOTIFICATION_TOPIC` | `notification-requests` | Topic to consume. |
| `KAFKA_GROUP_ID` | `notifier-consumer` | Fixed consumer group ID — see above. |

## Implementation

Plain Java (no framework) on Java 17, built with Maven into a single
runnable jar:

- `KafkaConsumerLoop` — wraps `org.apache.kafka.clients.consumer.KafkaConsumer`,
  polling on its own daemon thread (`KafkaConsumer.poll()` is a blocking
  call, so it can't run on the same thread serving HTTP/WebSocket traffic).
  Unwraps the JSON envelope's `payload`, adds `broadcast_at`, and hands the
  result to `NotifyHub`.
- `NotifyHub` — holds the set of connected WebSocket sessions and the
  bounded replay buffer described above; `broadcast()` is called from the
  consumer thread, `register()`/`unregister()` from the server's connection
  threads.
- `ServerFactory` / `Main` — an embedded Jetty server exposing `/health`
  (a plain servlet) and `/ws` (Jetty's native WebSocket API via
  `NotifyWebSocketListener`) on the same port, plus the shutdown-hook wiring
  described above.

Build: `mvn package` (or via the multi-stage `Dockerfile`, which builds with
`maven:3.9-eclipse-temurin-17` and runs on `eclipse-temurin:17-jre`).
