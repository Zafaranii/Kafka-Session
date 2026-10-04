# producer-api

`POST /notify` — produces one message to `notification-requests` and
returns as soon as the broker acks it. Does not wait on the JDBC sink
connector or `notifier-consumer` — they're independent, decoupled readers of
the same topic.

## Endpoint

```
POST /notify
{ "request_id": "<uuid>", "message": "<text>" }
```

Response: **`202 Accepted`**, not `200 OK`. The status code is deliberate —
this response means "Kafka has accepted the event," not "the notification
has been processed or delivered." The DB write and the WebSocket push both
happen later, asynchronously, off this request's critical path.

```json
{
  "request_id": "...",
  "status": "accepted",
  "timeline": { "received_at": <epoch ms>, "produced_at": <epoch ms> }
}
```

`timeline` mirrors the pattern used by `notify-api` (sync path) — lets the
frontend show per-hop latency instead of just a single round-trip number.

## Message shape on the wire

- **Key**: `request_id`, plain UTF-8 string (`StringSerializer` on the Connect
  worker side).
- **Value**: JSON, wrapped in Kafka Connect's schema envelope
  (`{"schema": {...}, "payload": {"request_id": ..., "message": ...}}`) so
  the JDBC sink connector can build a typed `UPSERT` without a Schema
  Registry. This is Phase 1 — see `PLAN.md` for the Phase 2 Avro upgrade.

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `broker:19092` | Kafka broker's internal (in-network) listener. |
| `NOTIFICATION_TOPIC` | `notification-requests` | Topic to produce to. |

## Why we wait for a delivery report, not a bare `flush()`

`producer.produce()` alone only queues the message locally; it doesn't
guarantee the broker received it. We need *some* acknowledgment before
responding, so a produce failure becomes a visible `502` instead of a
silently dropped message — the "fire-and-forget" property this panel is
demonstrating is about not waiting on *downstream consumers* (DB write, WS
push), not about skipping the broker's own ack.

The naive way to get that ack is a single shared `Producer` plus
`producer.flush(timeout=5)` on every request. That's wrong under concurrent
load: `flush()` with no arguments blocks until the producer's **entire**
outstanding queue is empty, not just the message this request produced. With
many concurrent requests, one request's `flush()` call ends up waiting on
every other in-flight request's delivery too — client response time starts
scaling with total concurrent load instead of reflecting "did Kafka accept
my event," which defeats the point of this panel (verified directly: at
1000 concurrent requests, `flush()`-per-request pushed p50 client response
to ~1.6s and p99 to ~2.1s, purely from queue contention).

Instead, a single background thread (`_poll_loop`) continuously calls
`producer.poll(1.0)`, which is what actually drives delivery-report
callbacks. Each request registers its own `on_delivery` callback and an
`asyncio.Event`, then `await`s only that event — so it unblocks as soon as
*its own* message is acked, independent of how many other requests are
in flight. `Producer.produce()` is safe to call concurrently from multiple
threads/coroutines, which is what makes this pattern work.

**Why `async def`, not a sync `def`, for the endpoint:** a sync `def`
handler in FastAPI runs through Starlette's default thread-pool executor,
capped at roughly 40 concurrent threads. Under load, most requests would
queue for a thread-pool slot *before ever calling `produce()`* — a
framework-level ceiling with nothing to do with Kafka. `produce()` itself is
a fast, non-blocking, O(1) local enqueue (no network I/O in-line), so
`await`ing the delivery event cooperatively in an `async def` handler lets
response time scale with real concurrency instead of an arbitrary thread
count. The callback fires on the background poll thread, not the event
loop's thread, so it sets the `asyncio.Event` via `loop.call_soon_threadsafe`
rather than directly (`asyncio.Event` isn't thread-safe on its own).

Verified with `scripts/load-test.mjs --requests 1000`: the naive
shared-producer-plus-`flush()` version pushed p50 client response to
~1.6s; switching to per-message delivery waits (still a sync `def`) brought
it to ~1.0s; switching the endpoint to `async def` on top of that brought it
down further, since requests stopped queuing on the thread pool.
