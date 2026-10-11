import docker
from docker.errors import APIError, NotFound
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="control-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = docker.from_env()

# Demo-only service: it has the Docker socket mounted in, which is
# effectively root on the host. That's acceptable for a local demo, but the
# API surface it exposes is still deliberately narrowed to the containers the
# frontend's stop/start buttons target, by logical name - never by an
# arbitrary container name from the request.
ALLOWED_CONTAINERS = {
    "notifier-consumer": "notification-demo-notifier-consumer",
    "ws-service": "notification-demo-ws-service",
    # Inside Kafka page: stop a broker to show leader failover and the ISR.
    **{f"kafka-{i}": f"notification-demo-kafka-{i}" for i in (1, 2, 3)},
    # Inside Kafka page: scale the email-sender consumer group up and down.
    **{f"email-sender-{i}": f"notification-demo-email-sender-{i}" for i in (1, 2, 3, 4)},
}


def resolve(name: str):
    container_name = ALLOWED_CONTAINERS.get(name)
    if container_name is None:
        raise HTTPException(status_code=404, detail=f"unknown container: {name}")
    try:
        return client.containers.get(container_name)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=f"container not found: {container_name}") from exc


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/containers/{name}/status")
def status(name: str):
    container = resolve(name)
    container.reload()
    return {"name": name, "status": container.status}


@app.post("/containers/{name}/stop")
def stop(name: str):
    container = resolve(name)
    try:
        container.stop(timeout=10)
    except APIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    container.reload()
    return {"name": name, "status": container.status}


@app.post("/containers/{name}/start")
def start(name: str):
    container = resolve(name)
    try:
        container.start()
    except APIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    container.reload()
    return {"name": name, "status": container.status}
