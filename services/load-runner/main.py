"""load-runner: the load generator behind the frontend's "Run Load Test" button.

It plays the *business service* from the deck, not a browser user:

- sync path  (deck slide 4): it calls notify-api over HTTP and is blocked
  until the whole chain (DB write, then push) answers 200.
- event path (deck slide 9): it publishes the event straight to Kafka, the
  way a backend service would, and is free as soon as Kafka acknowledges it.
  There is no producer-api in between; that HTTP layer only exists so a
  browser can send single notifications.

What keeps the comparison fair:

- Same client process, same number of messages in flight, same warm-up, and
  the same delivery measurement (a WebSocket listener) for both paths.
- The Kafka side is not allowed to cheat with fire-and-forget batching: it
  uses producer-api's exact producer settings (acks=all, linger.ms=0) and
  each message waits for its *own* broker acknowledgement, the same way
  each HTTP request waits for its own response.
- "Blocked" is measured from just before the call/produce until its own
  answer/ack, on this process's clock. Delivery is measured the same way,
  when the notification arrives over the WebSocket.

Runs inside the compose network (it reaches notify-api, ws-service,
notifier-consumer, db-service and the brokers by service name), so a browser
can start a run but is not part of the measurement.
"""

import asyncio
import json
import os
import threading
import time
import uuid
from typing import Literal

import aiohttp
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.environ.get("NOTIFICATION_TOPIC", "notification-requests")
NOTIFY_URL = os.environ.get("NOTIFY_URL", "http://notify-api:8000/notify")
SYNC_WS_URL = os.environ.get("SYNC_WS_URL", "ws://ws-service:8000/ws")
EVENT_WS_URL = os.environ.get("EVENT_WS_URL", "ws://notifier-consumer:8000/ws")
DB_SERVICE_URL = os.environ.get("DB_SERVICE_URL", "http://db-service:8000")

# Same prefix the old in-browser test used, so "Reset Load Test Data" still
# cleans these rows up.
LOAD_TEST_PREFIX = "loadtest-"
WARMUP_REQUESTS = 200  # discarded per path, so connections/pools are warm for both
DELIVERY_GRACE_S = 15  # how long to keep listening for WebSocket deliveries after the last answer
SAVE_WAIT_S = 15  # event path: how long to wait for the sink connector to persist every row
RECIPIENTS = 100  # record keys user-0..user-99, spread across all partitions

app = FastAPI(title="load-runner")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Kafka Connect's "JSON with schema" envelope - identical to producer-api's,
# so the sink connector and notifier-consumer treat these records exactly
# like any other notification.
VALUE_SCHEMA = {
    "type": "struct",
    "optional": False,
    "name": "notification",
    "fields": [
        {"field": "request_id", "type": "string", "optional": False},
        {"field": "message", "type": "string", "optional": False},
    ],
}

# Exactly producer-api's settings - see services/streaming/producer-api/main.py.
producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 0,
        "acks": "all",
        "partitioner": "murmur2_random",
        "message.timeout.ms": 4500,
    }
)


def _poll_loop():
    while True:
        producer.poll(1.0)


threading.Thread(target=_poll_loop, daemon=True).start()

run_lock = asyncio.Lock()


def now_ms() -> float:
    return time.time() * 1000


class RunRequest(BaseModel):
    path: Literal["sync", "event"]
    requests: int = Field(1000, ge=1, le=100_000)
    concurrency: int = Field(500, ge=1, le=2_000)


async def run_pool(count, concurrency, task):
    """task(i) for i in range(count), at most `concurrency` in flight."""
    results = [None] * count
    indexes = iter(range(count))

    async def worker():
        for i in indexes:
            results[i] = await task(i)

    await asyncio.gather(*(worker() for _ in range(min(concurrency, count))))
    return results


class DeliveryListener:
    """Records when each tracked request_id arrives over a WebSocket."""

    def __init__(self, session, url):
        self.session, self.url = session, url
        self.tracked = set()
        self.arrivals = {}  # request_id -> (arrived_ms, broadcast_at)
        self.ws = None
        self.task = None

    async def __aenter__(self):
        self.ws = await self.session.ws_connect(self.url, heartbeat=None)
        self.task = asyncio.create_task(self._read())
        return self

    async def _read(self):
        async for msg in self.ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            arrived = now_ms()
            try:
                data = json.loads(msg.data)
            except ValueError:
                continue
            rid = data.get("request_id")
            if rid in self.tracked and rid not in self.arrivals:
                self.arrivals[rid] = (arrived, data.get("broadcast_at"))

    async def wait_for(self, request_ids, timeout_s):
        deadline = time.time() + timeout_s
        while time.time() < deadline and not request_ids.issubset(self.arrivals):
            await asyncio.sleep(0.1)

    async def __aexit__(self, *_):
        self.task.cancel()
        await self.ws.close()


def new_request_id():
    return f"{LOAD_TEST_PREFIX}{uuid.uuid4()}"


async def sync_call(session, i, message, rid):
    """One call to notify-api, as a business service would make it."""
    body = {"request_id": rid, "message": message, "recipient": f"user-{i % RECIPIENTS}"}
    sent = now_ms()
    try:
        async with session.post(NOTIFY_URL, json=body) as resp:
            data = await resp.json(content_type=None)
            done = now_ms()
            ok = 200 <= resp.status < 300
            return {
                "request_id": rid,
                "sendTs": sent,
                "respMs": done - sent,
                "ok": ok,
                "reason": None if ok else f"HTTP {resp.status}",
                "timeline": data.get("timeline") if ok and isinstance(data, dict) else None,
            }
    except Exception as exc:
        return {"request_id": rid, "sendTs": sent, "respMs": now_ms() - sent, "ok": False, "reason": f"No response ({type(exc).__name__})"}


async def event_publish(i, message, rid):
    """Publish one event straight to Kafka and wait for its own acks=all ack."""
    key = f"user-{i % RECIPIENTS}"
    envelope = {"schema": VALUE_SCHEMA, "payload": {"request_id": rid, "message": message}}
    loop = asyncio.get_running_loop()
    acked = loop.create_future()

    def on_delivery(err, msg):
        result = (str(err), None) if err is not None else (None, (msg.partition(), msg.offset()))
        loop.call_soon_threadsafe(lambda: acked.done() or acked.set_result(result))

    sent = now_ms()
    try:
        producer.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
            headers=[("trace_id", rid.encode("utf-8"))],
            on_delivery=on_delivery,
        )
        err, coords = await asyncio.wait_for(acked, timeout=5)
    except Exception as exc:
        return {"request_id": rid, "sendTs": sent, "respMs": now_ms() - sent, "ok": False, "reason": f"Kafka: {type(exc).__name__}"}
    done = now_ms()
    if err is not None:
        return {"request_id": rid, "sendTs": sent, "respMs": done - sent, "ok": False, "reason": f"Kafka: {err}"}
    return {"request_id": rid, "sendTs": sent, "respMs": done - sent, "ok": True, "reason": None, "kafka": {"partition": coords[0], "offset": coords[1]}}


async def saved_times(session, request_ids):
    """Event path: when Postgres created each row (via db-service), waiting up to SAVE_WAIT_S."""
    deadline = time.time() + SAVE_WAIT_S
    rows = {}
    while time.time() < deadline:
        try:
            async with session.get(f"{DB_SERVICE_URL}/admin/load-test-created-at") as resp:
                rows = (await resp.json())["rows"]
        except Exception:
            pass
        if request_ids.issubset(rows):
            break
        await asyncio.sleep(0.5)
    return rows


async def run_path(req: RunRequest):
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=req.concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if req.path == "sync":
            call = lambda i, msg, rid: sync_call(session, i, msg, rid)  # noqa: E731
            ws_url = SYNC_WS_URL
        else:
            call = event_publish
            ws_url = EVENT_WS_URL

        await run_pool(WARMUP_REQUESTS, req.concurrency, lambda i: call(i, "warmup", new_request_id()))

        async with DeliveryListener(session, ws_url) as listener:
            started = now_ms()

            async def tracked(i):
                # Track the id before sending: on the sync path ws-service
                # pushes the notification before notify-api answers 200, so
                # the WebSocket message can arrive before the response does.
                rid = new_request_id()
                listener.tracked.add(rid)
                return await call(i, "load test", rid)

            results = await run_pool(req.requests, req.concurrency, tracked)
            finished = now_ms()
            ok_ids = {r["request_id"] for r in results if r["ok"]}
            await listener.wait_for(ok_ids, DELIVERY_GRACE_S)
            arrivals = dict(listener.arrivals)

        saved = await saved_times(session, ok_ids) if req.path == "event" else {}

    failure_reasons = {}
    for r in results:
        if not r["ok"]:
            failure_reasons[r["reason"]] = failure_reasons.get(r["reason"], 0) + 1

    samples = []
    for r in results:
        if not r["ok"]:
            continue
        arrived, broadcast_at = arrivals.get(r["request_id"], (None, None))
        created = saved.get(r["request_id"])
        samples.append(
            {
                "sendTs": round(r["sendTs"], 1),
                "respMs": round(r["respMs"], 2),
                "deliverMs": round(arrived - r["sendTs"], 1) if arrived is not None else None,
                "broadcastTs": broadcast_at,
                "timeline": r.get("timeline"),
                "savedMs": round(created - r["sendTs"], 1) if created is not None else None,
            }
        )

    return {
        "path": req.path,
        "requests": req.requests,
        "concurrency": req.concurrency,
        "warmup": WARMUP_REQUESTS,
        "duration_ms": round(finished - started),
        "sent": len(results),
        "succeeded": len(ok_ids),
        "failed": len(results) - len(ok_ids),
        "failureReasons": failure_reasons,
        "undelivered": len(ok_ids - set(arrivals)),
        "delivery_grace_s": DELIVERY_GRACE_S,
        "samples": samples,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
async def run(req: RunRequest):
    if run_lock.locked():
        raise HTTPException(status_code=409, detail="a load test is already running")
    async with run_lock:
        return await run_path(req)
