"""email-sender: a third, independent consumer group on notification-requests.

It stands in for the "Email sender" consumer from the deck's event-driven
pipeline. It doesn't send real email: each record costs SEND_DELAY_MS of
sleep, which simulates the SMTP call. The delay is shown in the UI, so the
audience knows it is simulated.

Run several copies (email-sender-1..4 in docker-compose.yml) and they share
the one group.id, so Kafka splits the topic's partitions between them. That
is the live demo of consumer groups and of partitions as the unit of
parallelism.
"""

import os
import signal
import socket
import time

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")
TOPIC = os.environ.get("NOTIFICATION_TOPIC", "notification-requests")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "email-sender")
SEND_DELAY_MS = float(os.environ.get("SEND_DELAY_MS", 5))
INSTANCE = os.environ.get("INSTANCE_NAME", socket.gethostname())

running = True


def stop(_signum, _frame):
    global running
    running = False


# docker stop sends SIGTERM. Leaving the loop lets consumer.close() run, which
# sends a clean LeaveGroup, so the remaining instances are reassigned this
# one's partitions right away instead of after the session timeout (45s).
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def on_assign(_consumer, partitions):
    print(f"[{INSTANCE}] assigned partitions {sorted(p.partition for p in partitions)}", flush=True)


def on_revoke(_consumer, partitions):
    print(f"[{INSTANCE}] revoked partitions {sorted(p.partition for p in partitions)}", flush=True)


consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "client.id": INSTANCE,
        # A brand-new group has no committed offset yet. "earliest" makes it
        # start from the oldest record Kafka still retains, so joining late
        # means reading the whole history.
        "auto.offset.reset": "earliest",
        # Commit every second instead of every 5s (the default), so the lag
        # shown on the Inside Kafka page drains smoothly rather than in steps.
        "auto.commit.interval.ms": 1000,
    }
)
consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
print(f"[{INSTANCE}] joined group {GROUP_ID}, simulated send time {SEND_DELAY_MS} ms/email", flush=True)

sent = 0
try:
    while running:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[{INSTANCE}] consumer error: {msg.error()}", flush=True)
            continue
        time.sleep(SEND_DELAY_MS / 1000)
        sent += 1
        if sent % 500 == 0:
            key = msg.key().decode("utf-8") if msg.key() else None
            print(f"[{INSTANCE}] emailed {sent} so far (last: {key}, p{msg.partition()} @ {msg.offset()})", flush=True)
finally:
    consumer.close()
    print(f"[{INSTANCE}] left group after emailing {sent}", flush=True)
