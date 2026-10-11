import asyncio
import json
import os
import threading
import time

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="producer-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.environ.get("NOTIFICATION_TOPIC", "notification-requests")

# linger.ms=0: send each message to the broker immediately instead of waiting
# up to the librdkafka default of 5 ms to fill a batch. Each request here
# blocks on its own delivery report, so that default added ~5 ms of pure
# idle wait to every response (ack median 6.2 ms -> 0.14 ms in testing).
# acks=all: the broker only acks once every in-sync replica has the record,
# and with the topic's min.insync.replicas=2 it refuses the write outright if
# fewer than 2 replicas are in sync - durability over availability. This is
# librdkafka's default already; it's written down because the demo stops
# brokers on purpose and this is the setting that decides what happens then.
#
# partitioner=murmur2_random: librdkafka's default partitioner hashes keys
# with CRC32, while the Java client uses murmur2. Matching Java means "same
# key -> same partition" holds no matter which language produced the record.
producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 0,
        "acks": "all",
        "partitioner": "murmur2_random",
        # Give up on a record before the endpoint's own 5s wait does. With the
        # librdkafka default (300s) a write refused for lack of in-sync
        # replicas keeps retrying in the background after the client has
        # already been told 502 - and may then land anyway.
        "message.timeout.ms": 4500,
    }
)

# A single background thread services delivery-report callbacks for every
# request. This matters under concurrent load: each request thread waits
# only on its own message's callback (below), not on this producer's whole
# outstanding queue — a shared producer + a bare flush() per request would
# make every request block until ALL in-flight messages are acked, which
# makes client response time scale with total concurrent load instead of
# reflecting just "did Kafka accept my event."
def _poll_loop():
    while True:
        producer.poll(1.0)


threading.Thread(target=_poll_loop, daemon=True).start()

# Kafka Connect's "JSON with schema" envelope — lets the JDBC sink connector
# build a typed UPSERT without a Schema Registry. See kafka-connect/README.md.
VALUE_SCHEMA = {
    "type": "struct",
    "optional": False,
    "name": "notification",
    "fields": [
        {"field": "request_id", "type": "string", "optional": False},
        {"field": "message", "type": "string", "optional": False},
    ],
}


class NotifyPayload(BaseModel):
    request_id: str
    message: str
    # Who the notification is for. Becomes the record key, so every
    # notification for the same recipient lands in the same partition and
    # stays in order. Optional so older clients still work: without it the
    # key falls back to request_id.
    recipient: str | None = None


def now_ms() -> float:
    return time.time() * 1000


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/notify", status_code=202)
async def notify(payload: NotifyPayload):
    received_at = now_ms()
    envelope = {"schema": VALUE_SCHEMA, "payload": {"request_id": payload.request_id, "message": payload.message}}
    key = payload.recipient or payload.request_id
    # async def, not a sync def, and asyncio.Event, not threading.Event:
    # a sync def here would run through Starlette's default thread-pool
    # executor (capped around ~40 concurrent threads), so under load most
    # requests would queue for a thread slot before ever calling produce() —
    # a framework-level ceiling that has nothing to do with Kafka. produce()
    # itself is a fast, non-blocking, O(1) enqueue, so awaiting the delivery
    # event cooperatively lets this handler scale with real concurrency
    # instead of an arbitrary thread-pool size (verified: this is what
    # closed most of the remaining gap between 10 and 1000 concurrent
    # requests in scripts/load-test.mjs).
    loop = asyncio.get_running_loop()
    delivered = asyncio.Event()
    result = {}

    def on_delivery(err, msg):
        if err is not None:
            result["error"] = str(err)
        else:
            # Where the broker put it - the record's coordinates in the log.
            result["kafka"] = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "key": key,
            }
        # Fired from the background _poll_loop thread, not this event
        # loop's thread — asyncio.Event isn't thread-safe, so it must be
        # set via call_soon_threadsafe rather than directly.
        loop.call_soon_threadsafe(delivered.set)

    producer.produce(
        TOPIC,
        key=key.encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
        headers=[("trace_id", payload.request_id.encode("utf-8"))],
        on_delivery=on_delivery,
    )
    try:
        await asyncio.wait_for(delivered.wait(), timeout=5)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=502, detail="kafka produce timed out waiting for broker ack")
    produced_at = now_ms()

    if "error" in result:
        raise HTTPException(status_code=502, detail=f"kafka produce failed: {result['error']}")

    # 202, not 200: this response means Kafka accepted the event, not that
    # the notification has been processed or delivered — that happens later,
    # off the critical path, once the sink connector / notifier-consumer
    # read it off the topic.
    return {
        "request_id": payload.request_id,
        "status": "accepted",
        "kafka": result["kafka"],
        "timeline": {
            "received_at": received_at,
            "produced_at": produced_at,
        },
    }
