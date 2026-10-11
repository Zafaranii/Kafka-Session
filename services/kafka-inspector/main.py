"""kafka-inspector: a read-only window into the cluster for the demo's UI.

Everything here is what the deck's sections 03-05 talk about, read straight
from Kafka (no guessing, no local bookkeeping):

- brokers: which of the three are alive
- partitions of notification-requests: leader, replicas, in-sync replicas
  (ISR), and the log's first and next offset
- consumer groups: members, which partitions each member owns, and each
  group's committed offset and lag per partition
- the last few records of each partition: key, offset, timestamp, headers

All Kafka client calls run in a worker process (isolated.py explains the
librdkafka crash that makes this necessary); this process only merges
results and serves them, so it never goes down with the client library.

One background thread refreshes a snapshot about once a second and every
request returns the latest one, so any number of open browser tabs costs the
cluster the same. Each part of the snapshot falls back to its last good
value on its own: while a broker is stopped, some requests time out for a
few seconds, and that must not freeze the rest of the picture - least of all
the broker cards the audience is watching.
"""

import multiprocessing
import os
import socket
import threading
import time
from concurrent.futures import ProcessPoolExecutor

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import isolated

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.environ.get("NOTIFICATION_TOPIC", "notification-requests")
# broker id -> its in-network listener. Used for the liveness check below.
BROKER_ADDRESSES = {
    int(b.split("=")[0]): b.split("=")[1]
    for b in os.environ.get("BROKER_ADDRESSES", "1=kafka-1:19092,2=kafka-2:19092,3=kafka-3:19092").split(",")
}
# The groups the demo talks about, in the order the UI shows them.
GROUPS = [
    {"id": "connect-notifications-sink-connector", "label": "db-sink", "role": "JDBC sink connector → Postgres"},
    {"id": "notifier-consumer", "label": "notifier-consumer", "role": "WebSocket push to the browser"},
    {"id": "email-sender", "label": "email-sender", "role": "Simulated email sender"},
]
EMAIL_SEND_DELAY_MS = float(os.environ.get("EMAIL_SEND_DELAY_MS", 5))
MIN_INSYNC_REPLICAS = 2  # set on the topic by kafka-init in docker-compose.yml
RECENT_PER_PARTITION = 6
REFRESH_S = 0.8
TIMEOUT_S = 3

app = FastAPI(title="kafka-inspector")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

snapshot = {"ok": False, "error": "starting up"}
# Last good value of each part, reused when that part couldn't be refreshed.
last = {"partitions": {}, "start": {}, "end": {}, "members": {}, "committed": {}}
recent_cache = {}  # partition -> {"end": log end offset, "records": [...]}

# "spawn", not the Linux default "fork": a forked child of a process running
# librdkafka threads is broken. (This process doesn't load librdkafka, but
# spawn keeps that true no matter what gets imported later.)
worker_pool = None


def run_isolated(fn, *args, timeout):
    """Run fn in the worker; if it crashes or hangs, discard the worker and raise."""
    global worker_pool
    try:
        if worker_pool is None:
            worker_pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
        return worker_pool.submit(fn, *args).result(timeout=timeout)
    except Exception:
        pool, worker_pool = worker_pool, None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        raise


def reachable(address):
    host, port = address.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=0.5):
            return True
    except OSError:
        return False


def build(raw, stale):
    # Brokers. "Up" means the process accepts connections and the cluster
    # still lists it. Metadata alone isn't enough: the controller keeps
    # listing a stopped broker until its session times out (several seconds),
    # which on stage would show a broker as running right after it stopped.
    registered = set(raw["registered"]) if raw["registered"] is not None else set(BROKER_ADDRESSES)
    if raw["registered"] is None:
        stale.append("brokers")
    brokers = [{"id": b, "up": b in registered and reachable(addr)} for b, addr in sorted(BROKER_ADDRESSES.items())]

    # Partitions
    if raw["partitions"] is not None:
        last["partitions"] = {p["id"]: p for p in raw["partitions"]}
    else:
        stale.append("partitions")
    for side in ("start", "end"):
        for pid in last["partitions"]:
            if pid in raw[side]:
                last[side][pid] = raw[side][pid]
            elif raw["partitions"] is not None:
                stale.append(f"{side} offset of P{pid}")
    partitions = [
        {**p, "start_offset": last["start"].get(pid, 0), "end_offset": last["end"].get(pid, 0)}
        for pid, p in sorted(last["partitions"].items())
    ]

    # Consumer groups
    groups = []
    for g in GROUPS:
        gid = g["id"]
        if gid in raw["members"]:
            last["members"][gid] = raw["members"][gid]
        else:
            stale.append(f"members of {g['label']}")
        if gid in raw["committed"]:
            last["committed"][gid] = raw["committed"][gid]
        else:
            stale.append(f"offsets of {g['label']}")
        members = last["members"].get(gid, {"state": "UNKNOWN", "members": []})
        committed = last["committed"].get(gid, {})
        per_partition = []
        for p in partitions:
            c = committed.get(p["id"])
            per_partition.append(
                {
                    "partition": p["id"],
                    "committed": c,
                    # A group that never committed has no position yet. With
                    # auto.offset.reset=earliest it will start at the oldest
                    # retained record, so that's what it would have to read.
                    "lag": max(0, (p["end_offset"] - c) if c is not None else (p["end_offset"] - p["start_offset"])),
                }
            )
        groups.append(
            {
                **g,
                "state": members["state"],
                "members": members["members"],
                "has_committed": bool(committed),
                "partitions": per_partition,
                "total_lag": sum(x["lag"] for x in per_partition),
            }
        )
    return brokers, partitions, groups


def recent_records(partitions, stale):
    """The last few records of each partition, re-read only when it grew."""
    ranges = []
    for p in partitions:
        if recent_cache.get(p["id"], {}).get("end") == p["end_offset"]:
            continue
        first = max(p["start_offset"], p["end_offset"] - RECENT_PER_PARTITION)
        if first >= p["end_offset"]:
            recent_cache[p["id"]] = {"end": p["end_offset"], "records": []}
        else:
            ranges.append((p["id"], first, p["end_offset"] - 1))
    if ranges:
        try:
            got = run_isolated(isolated.read_tail, BOOTSTRAP_SERVERS, TOPIC, ranges, timeout=10)
            ends = {p["id"]: p["end_offset"] for p in partitions}
            for pid, records in got.items():
                recent_cache[pid] = {"end": ends[pid], "records": records[-RECENT_PER_PARTITION:]}
            if len(got) < len(ranges):
                stale.append("recent records")
        except Exception:
            stale.append("recent records")
    return {pid: c["records"] for pid, c in recent_cache.items()}


def refresh_loop():
    global snapshot
    while True:
        started = time.time()
        stale = []
        try:
            try:
                # Generous timeout: a fresh worker has to start and connect first.
                raw = run_isolated(isolated.collect, BOOTSTRAP_SERVERS, TOPIC, [g["id"] for g in GROUPS], TIMEOUT_S, timeout=20)
            except Exception:
                raw = {"registered": None, "partitions": None, "start": {}, "end": {}, "members": {}, "committed": {}}
            brokers, partitions, groups = build(raw, stale)
            recent = recent_records(partitions, stale)
            snapshot = {
                "ok": not stale,
                # What couldn't be refreshed this round and is shown from the
                # previous one (normal for a few seconds after a broker stops).
                "error": ("stale: " + ", ".join(stale)) if stale else None,
                "taken_at": int(time.time() * 1000),
                "topic": TOPIC,
                "min_insync_replicas": MIN_INSYNC_REPLICAS,
                "email_send_delay_ms": EMAIL_SEND_DELAY_MS,
                "brokers": brokers,
                "partitions": partitions,
                "groups": groups,
                "recent": recent,
            }
        except Exception as exc:
            snapshot = {**snapshot, "ok": False, "error": str(exc), "taken_at": int(time.time() * 1000)}
        time.sleep(max(0.0, REFRESH_S - (time.time() - started)))


threading.Thread(target=refresh_loop, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/snapshot")
def get_snapshot():
    return snapshot
