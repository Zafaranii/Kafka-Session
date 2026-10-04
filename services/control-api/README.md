# control-api

Demo-only service with the Docker socket mounted in, letting the frontend's
"Stop"/"Start" buttons actually stop and start two other containers in this
compose stack. Nothing about the architecture demo depends on this service —
it exists purely so the resilience/loss comparison can be triggered from the
browser instead of a terminal.

## Why the Docker socket, and why it's scoped down

Mounting `/var/run/docker.sock` into a container gives that container
root-equivalent control over the whole Docker host. That's fine for a local
demo but not something to expose carelessly, so this service:

- Is unauthenticated and has an open API only because it's meant to run on
  `localhost` for a local demo, never as a deployed/public service.
- Only acts on two containers, addressed by a fixed logical name
  (`notifier-consumer`, `ws-service`) mapped internally to their real
  container names — an arbitrary container name in the URL is rejected, so
  it can't be used to stop/start anything else on the host.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /containers/{name}/status` | Current Docker status (`running`, `exited`, ...) |
| `POST /containers/{name}/stop` | Stop the container (`docker stop`, 10s grace period) |
| `POST /containers/{name}/start` | Start the container back up |

`{name}` is one of `notifier-consumer` or `ws-service`.

## Why these two containers

They're the two services whose in-process state a browser WebSocket
connection depends on:

- `notifier-consumer` (event-driven path) — commits its Kafka consumer
  offset under a fixed `group.id`. Stopping it delays delivery; restarting
  it resumes from that offset, so nothing sent while it was down is lost.
- `ws-service` (sync path) — holds WebSocket connections and does no
  persistence at all. Stopping it makes `notify-api`'s push fail; once
  `notify-api`'s retries are exhausted (see
  `services/sync/notify-api/README.md`), the notification is gone for good,
  even after `ws-service` is started back up.

That contrast is the point of the frontend's stop/start buttons.
