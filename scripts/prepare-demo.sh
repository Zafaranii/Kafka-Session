#!/usr/bin/env bash
# Puts the demo in its starting state. Run this before going on stage.
#
#   1. Tears the stack down, volumes included. The Kafka log and Postgres
#      start empty, so the backlog email-sender reads in act 4 is exactly
#      what was sent during the session, not leftovers from rehearsals.
#   2. Builds and starts everything.
#   3. Creates (but does not start) email-sender-1..4, so the group has no
#      committed offsets until it is started on stage, and the control-api
#      has containers to start.
#   4. Registers the JDBC sink connector (idempotent PUT, safe to re-run).
#   5. Waits until the connector is RUNNING and every partition has all 3
#      replicas in sync.
#
# Usage: scripts/prepare-demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

CONNECT=http://localhost:8084
CONNECTOR=notifications-sink-connector

wait_for() { # wait_for <description> <seconds> <command...>
  local what=$1 secs=$2; shift 2
  printf '    waiting for %s' "$what"
  for _ in $(seq "$secs"); do
    if "$@" >/dev/null 2>&1; then echo " ✓"; return 0; fi
    printf '.'; sleep 1
  done
  echo " ✗ (gave up after ${secs}s)"; return 1
}

echo "==> Removing the old stack (fresh Kafka log and Postgres)"
docker compose --profile email down -v --remove-orphans

echo "==> Building and starting the stack"
docker compose up -d --build

echo "==> Creating email-sender-1..4 (stopped)"
docker compose --profile email create email-sender-1 email-sender-2 email-sender-3 email-sender-4

echo "==> Registering the sink connector"
wait_for "Kafka Connect REST API" 180 curl -sf "$CONNECT/connectors"
python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' \
  "connectors/$CONNECTOR.json" \
  | curl -sf -X PUT "$CONNECT/connectors/$CONNECTOR/config" -H "Content-Type: application/json" -d @- >/dev/null

connector_running() {
  curl -sf "$CONNECT/connectors/$CONNECTOR/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
ok = s["connector"]["state"] == "RUNNING" and len(s["tasks"]) == 3 and all(t["state"] == "RUNNING" for t in s["tasks"])
sys.exit(0 if ok else 1)'
}
wait_for "connector and its 3 tasks to be RUNNING" 120 connector_running

all_in_sync() {
  docker exec notification-demo-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:19092 --describe --topic notification-requests \
    | grep -c 'Isr: [0-9],[0-9],[0-9]' | grep -qx 3
}
wait_for "all 3 partitions to have 3 in-sync replicas" 60 all_in_sync

wait_for "kafka-inspector" 60 curl -sf http://localhost:8098/snapshot

echo
echo "Ready."
echo "  Demo page:    http://localhost:8090"
echo "  Inside Kafka: http://localhost:8090/kafka.html"
echo "  kafka-ui:     http://localhost:8081   (backup / Q&A)"
