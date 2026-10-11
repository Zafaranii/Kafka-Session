"""Every Kafka client call the inspector makes. Runs in a worker process.

librdkafka (confluent-kafka 2.5.0 and 2.16.0 both) segfaults on some admin
responses that carry coordinator / leadership errors, which are normal for a
few seconds after a broker stops - i.e. exactly during the demo's
broker-failover act. One backtrace, taken with gdb in this container:

    rd_kafka_cgrp_op            <- SIGSEGV
    rd_kafka_handle_OffsetFetch
    rd_kafka_ListConsumerGroupOffsetsResponse_parse
    rd_kafka_admin_worker

There was more than one crashing path, so instead of avoiding individual
calls, main.py runs all of them here. A crash costs one refresh round (shown
as stale in the UI); main.py starts a new worker and the web server itself
never goes down.
"""

import json
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

_admin = None


def _admin_client(bootstrap_servers):
    global _admin
    if _admin is None:
        _admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    return _admin


def collect(bootstrap_servers, topic, group_ids, timeout_s):
    """One round of cluster state. Each part is None if it couldn't be read."""
    admin = _admin_client(bootstrap_servers)
    out = {"registered": None, "partitions": None, "start": {}, "end": {}, "members": {}, "committed": {}}

    try:
        cluster = admin.describe_cluster(request_timeout=timeout_s).result()
        out["registered"] = [node.id for node in cluster.nodes]
    except Exception:
        pass

    partition_ids = []
    try:
        desc = admin.describe_topics(TopicCollection([topic]), request_timeout=timeout_s)[topic].result()
        out["partitions"] = [
            {
                "id": p.id,
                # None while a partition has no leader (e.g. too many brokers down)
                "leader": p.leader.id if p.leader is not None else None,
                "replicas": [n.id for n in p.replicas],
                "isr": [n.id for n in p.isr],
            }
            for p in sorted(desc.partitions, key=lambda p: p.id)
        ]
        partition_ids = [p["id"] for p in out["partitions"]]
    except Exception:
        pass

    for key, spec in (("start", OffsetSpec.earliest()), ("end", OffsetSpec.latest())):
        if not partition_ids:
            break
        try:
            futures = admin.list_offsets({TopicPartition(topic, pid): spec for pid in partition_ids}, request_timeout=timeout_s)
        except Exception:
            continue
        for tp, f in futures.items():
            try:
                out[key][tp.partition] = f.result().offset
            except Exception:
                pass  # e.g. the partition's leader just moved

    try:
        described = admin.describe_consumer_groups(group_ids, request_timeout=timeout_s)
    except Exception:
        described = {}
    for group_id in group_ids:
        try:
            d = described[group_id].result()
            out["members"][group_id] = {
                "state": d.state.name if d.state is not None else "UNKNOWN",
                "members": sorted(
                    (
                        {
                            "client_id": m.client_id,
                            "host": m.host,
                            "partitions": sorted(
                                tp.partition for tp in (m.assignment.topic_partitions if m.assignment else []) if tp.topic == topic
                            ),
                        }
                        for m in d.members
                    ),
                    key=lambda m: m["client_id"],
                ),
            }
        except Exception:
            pass

        if not partition_ids:
            continue
        # One group per call is all list_consumer_group_offsets accepts.
        req = [ConsumerGroupTopicPartitions(group_id, [TopicPartition(topic, pid) for pid in partition_ids])]
        try:
            res = admin.list_consumer_group_offsets(req, request_timeout=timeout_s)[group_id].result()
            out["committed"][group_id] = {tp.partition: tp.offset for tp in res.topic_partitions if tp.offset >= 0}
        except Exception:
            pass

    return out


def _decode(msg):
    key = msg.key().decode("utf-8", "replace") if msg.key() else None
    message = None
    try:
        value = json.loads(msg.value())
        payload = value.get("payload", value)
        message = payload.get("message")
    except Exception:
        pass
    return {
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": key,
        "timestamp": msg.timestamp()[1],
        "headers": {k: (v.decode("utf-8", "replace") if v else None) for k, v in (msg.headers() or [])},
        "message": message,
    }


def read_tail(bootstrap_servers, topic, ranges, timeout_s=2.0):
    """ranges: [(partition, first_offset, last_offset)] -> {partition: [records]}.

    Partitions not read completely before the timeout are left out, so the
    caller keeps its previous (complete) view of them.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            # Required by the client; never used to subscribe or commit, so
            # this group never appears in the cluster.
            "group.id": "kafka-inspector-reader",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, pid, first) for pid, first, _last in ranges])
        wanted = {pid: last for pid, _first, last in ranges}
        got = {pid: [] for pid in wanted}
        deadline = time.time() + timeout_s
        while wanted and time.time() < deadline:
            for msg in consumer.consume(num_messages=100, timeout=0.3):
                if msg.error() or msg.partition() not in got:
                    continue
                got[msg.partition()].append(_decode(msg))
                if msg.offset() >= wanted.get(msg.partition(), -1):
                    wanted.pop(msg.partition(), None)
        return {pid: recs for pid, recs in got.items() if pid not in wanted}
    finally:
        consumer.close()
