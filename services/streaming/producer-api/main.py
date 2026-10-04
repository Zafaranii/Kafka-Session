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

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = os.environ.get("NOTIFICATION_TOPIC", "notification-requests")

# linger.ms=0: send each message to the broker immediately instead of waiting
# up to the librdkafka default of 5 ms to fill a batch. Each request here
# blocks on its own delivery report, so that default added ~5 ms of pure
# idle wait to every response (ack median 6.2 ms -> 0.14 ms in testing).
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "linger.ms": 0})

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


def now_ms() -> float:
    return time.time() * 1000


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/notify", status_code=202)
async def notify(payload: NotifyPayload):
    received_at = now_ms()
    envelope = {"schema": VALUE_SCHEMA, "payload": payload.model_dump()}
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

    def on_delivery(err, _msg):
        if err is not None:
            result["error"] = str(err)
        # Fired from the background _poll_loop thread, not this event
        # loop's thread — asyncio.Event isn't thread-safe, so it must be
        # set via call_soon_threadsafe rather than directly.
        loop.call_soon_threadsafe(delivered.set)

    producer.produce(
        TOPIC,
        key=payload.request_id.encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
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
        "timeline": {
            "received_at": received_at,
            "produced_at": produced_at,
        },
    }
