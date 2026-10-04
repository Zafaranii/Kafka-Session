#!/usr/bin/env node
// Load-test script for the notification demo.
//
// Fires N concurrent requests at one or both /notify endpoints and reports
// two numbers per path: client response time (how long the HTTP call took)
// and end-to-end delivery time (when the WebSocket message actually
// arrived). No artificial delay is injected anywhere — this shows whatever
// difference the two architectures produce under real concurrent load.
//
// All requests use a request_id prefixed with LOAD_TEST_PREFIX so the
// frontend's "Reset Load Test Data" button (-> db-service's
// POST /admin/reset-load-test) can clean them up afterwards without
// touching notifications sent manually through the UI.
//
// Usage:
//   node scripts/load-test.mjs [--requests 1000] [--path both|sync|streaming] [--host localhost]

const LOAD_TEST_PREFIX = "loadtest-";
const DELIVERY_GRACE_MS = 15000; // how long to keep listening for stragglers after the last response

function parseArgs(argv) {
  const args = { requests: 1000, path: "both", host: "localhost" };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--requests") args.requests = parseInt(argv[++i], 10);
    else if (key === "--path") args.path = argv[++i];
    else if (key === "--host") args.host = argv[++i];
  }
  return args;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

function summarize(label, values) {
  if (values.length === 0) return `  ${label}: no data`;
  const sorted = [...values].sort((a, b) => a - b);
  const fmt = (n) => (n === null ? "n/a" : `${Math.round(n)}ms`);
  return (
    `  ${label} (n=${sorted.length}): ` +
    `min=${fmt(sorted[0])} p50=${fmt(percentile(sorted, 50))} ` +
    `p90=${fmt(percentile(sorted, 90))} p95=${fmt(percentile(sorted, 95))} ` +
    `p99=${fmt(percentile(sorted, 99))} max=${fmt(sorted[sorted.length - 1])}`
  );
}

async function runLoadTest({ label, notifyUrl, wsUrl, requestCount }) {
  console.log(`\n=== ${label} ===`);
  console.log(`Firing ${requestCount} concurrent requests at ${notifyUrl} ...`);

  const pendingDelivery = new Map(); // request_id -> sendTs
  const deliveryMs = [];

  const ws = new WebSocket(wsUrl);
  const wsReady = new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve);
    ws.addEventListener("error", reject);
  });
  ws.addEventListener("message", (evt) => {
    let data;
    try {
      data = JSON.parse(evt.data);
    } catch {
      return;
    }
    const sendTs = pendingDelivery.get(data.request_id);
    if (sendTs === undefined) return;
    deliveryMs.push(Date.now() - sendTs);
    pendingDelivery.delete(data.request_id);
  });

  try {
    await wsReady;
  } catch (err) {
    console.log(`  Could not connect to ${wsUrl}: ${err}`);
    return;
  }

  const results = await Promise.allSettled(
    Array.from({ length: requestCount }, async () => {
      const requestId = `${LOAD_TEST_PREFIX}${crypto.randomUUID()}`;
      const sendTs = Date.now();
      pendingDelivery.set(requestId, sendTs);
      const resp = await fetch(notifyUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ request_id: requestId, message: "load test" }),
      });
      const responseMs = Date.now() - sendTs;
      return { status: resp.status, responseMs };
    })
  );

  const succeeded = results.filter((r) => r.status === "fulfilled" && r.value.status >= 200 && r.value.status < 300);
  const failed = results.length - succeeded.length;
  const responseMs = succeeded.map((r) => r.value.responseMs);

  console.log(`  Requests: ${results.length} sent, ${succeeded.length} succeeded, ${failed} failed`);
  console.log(summarize("Client response time", responseMs));

  if (pendingDelivery.size > 0) {
    console.log(`  Waiting up to ${DELIVERY_GRACE_MS}ms for ${pendingDelivery.size} remaining deliveries...`);
    await new Promise((resolve) => setTimeout(resolve, DELIVERY_GRACE_MS));
  }
  ws.close();

  console.log(summarize("End-to-end delivery time", deliveryMs));
  if (pendingDelivery.size > 0) {
    console.log(`  ${pendingDelivery.size} request(s) never showed up over the WebSocket within the grace period.`);
  }
}

async function main() {
  const { requests, path, host } = parseArgs(process.argv.slice(2));

  if (path === "sync" || path === "both") {
    await runLoadTest({
      label: "Synchronous, Request-Driven (notify-api)",
      notifyUrl: `http://${host}:8004/notify`,
      wsUrl: `ws://${host}:8003/ws`,
      requestCount: requests,
    });
  }

  if (path === "streaming" || path === "both") {
    await runLoadTest({
      label: "Event-Driven, Asynchronous (producer-api)",
      notifyUrl: `http://${host}:8011/notify`,
      wsUrl: `ws://${host}:8013/ws`,
      requestCount: requests,
    });
  }

  console.log(
    "\nDone. Run the frontend's \"Reset Load Test Data\" button (or " +
      `POST http://${host}:8002/admin/reset-load-test) to clean up the rows this created.`
  );
}

main();
