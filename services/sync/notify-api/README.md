# notify-api

Entry point for the synchronous, request-driven path. On `POST /notify` it
writes to `db-service`, then pushes to `ws-service`, and only responds once
both have finished — the whole chain is on the client's request critical
path (see the root `README.md` / `PLAN.md` for how this contrasts with the
event-driven path).

## Retry on the push to ws-service

`push_with_retry` retries the `POST /push` call to `ws-service` up to
`PUSH_MAX_ATTEMPTS` (default 4) times, with exponential backoff starting at
`PUSH_RETRY_BASE_DELAY_S` (default 0.25s: ~0.25s, 0.5s, 1s between attempts).

This is deliberately **not** enough to make the sync path resilient the way
the event-driven path is. `ws-service` holds its WebSocket connections
entirely in memory and this retry has nothing durable to fall back on — no
queue, no offset, nothing outside this one request. So:

- If `ws-service` comes back up within the retry window, the notification
  gets through (retries genuinely helped, but only because the outage was
  short).
- If it doesn't, `/notify` returns a 502 after the last attempt and the
  notification is gone. Restarting `ws-service` afterwards does not recover
  it — there was never anywhere for it to be recovered from.

Compare with `notifier-consumer`
(`services/streaming/notifier-consumer/README.md`): Kafka's committed
offset means that side doesn't need a retry loop at all — the message is
already durably stored on the topic regardless of how long the consumer is
down.

Use the frontend's "Stop ws-service" / "Stop notifier-consumer" buttons
(wired through `services/control-api`) to see this difference directly.
