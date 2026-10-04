import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# One HTTP client for the whole process, created at startup, so requests
# reuse keep-alive connections to db-service and ws-service instead of opening
# new TCP connections on every /notify call.
#
# A short connect timeout matters here: once ws-service's container is
# stopped (not just its process killed), Docker's embedded DNS drops its
# hostname, and the default resolver can take several seconds per lookup
# to give up - which would make each retry attempt slow instead of the
# near-instant "connection refused" a live outage demo wants.
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(5.0, connect=1.5),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
    )
    try:
        yield
    finally:
        await http_client.aclose()
        http_client = None


app = FastAPI(title="notify-api", lifespan=lifespan)


def now_ms() -> float:
    return time.time() * 1000

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_SERVICE_URL = os.environ.get("DB_SERVICE_URL", "http://db-service:8000")
WS_SERVICE_URL = os.environ.get("WS_SERVICE_URL", "http://ws-service:8000")

# Bounded retry on the push to ws-service - see README.md for why this
# still loses the message once exhausted rather than making the sync path
# resilient: there's no queue behind ws-service to retry from later, so
# this only papers over an outage shorter than PUSH_MAX_ATTEMPTS *
# PUSH_RETRY_BASE_DELAY_S (roughly, with backoff).
PUSH_MAX_ATTEMPTS = int(os.environ.get("PUSH_MAX_ATTEMPTS", 4))
PUSH_RETRY_BASE_DELAY_S = float(os.environ.get("PUSH_RETRY_BASE_DELAY_S", 0.25))

# Live retry status feed for the frontend. ws-service is the thing that's
# normally down during this demo, so the retry loop's own progress can't
# ride along on that connection - it gets a small dedicated broadcast
# channel instead, independent of whatever push_with_retry is doing.
status_connections: list[WebSocket] = []


async def broadcast_status(event: dict) -> None:
    dead = []
    for ws in status_connections:
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in status_connections:
            status_connections.remove(ws)


@app.websocket("/status-ws")
async def status_ws(websocket: WebSocket):
    await websocket.accept()
    status_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in status_connections:
            status_connections.remove(websocket)


async def push_with_retry(client: httpx.AsyncClient, body: dict, request_id: str) -> tuple[httpx.Response, int]:
    last_error: Exception | HTTPException | None = None
    for attempt in range(1, PUSH_MAX_ATTEMPTS + 1):
        await broadcast_status(
            {"type": "attempt", "request_id": request_id, "attempt": attempt, "max_attempts": PUSH_MAX_ATTEMPTS, "status": "trying"}
        )
        try:
            resp = await client.post(f"{WS_SERVICE_URL}/push", json=body)
        except httpx.RequestError as exc:
            last_error = exc
        else:
            if resp.status_code == 200:
                await broadcast_status(
                    {"type": "attempt", "request_id": request_id, "attempt": attempt, "max_attempts": PUSH_MAX_ATTEMPTS, "status": "success"}
                )
                return resp, attempt
            last_error = HTTPException(status_code=502, detail=f"ws-service failed: {resp.text}")

        await broadcast_status(
            {
                "type": "attempt",
                "request_id": request_id,
                "attempt": attempt,
                "max_attempts": PUSH_MAX_ATTEMPTS,
                "status": "failed",
                "detail": str(last_error),
            }
        )

        if attempt < PUSH_MAX_ATTEMPTS:
            delay_s = PUSH_RETRY_BASE_DELAY_S * (2 ** (attempt - 1))
            await broadcast_status(
                {"type": "waiting", "request_id": request_id, "next_attempt": attempt + 1, "max_attempts": PUSH_MAX_ATTEMPTS, "delay_ms": round(delay_s * 1000)}
            )
            await asyncio.sleep(delay_s)

    await broadcast_status({"type": "exhausted", "request_id": request_id, "max_attempts": PUSH_MAX_ATTEMPTS})

    if isinstance(last_error, HTTPException):
        raise last_error
    raise HTTPException(
        status_code=502,
        detail=(
            f"ws-service unreachable after {PUSH_MAX_ATTEMPTS} attempts - "
            f"message lost, nothing persists it to retry from later: {last_error}"
        ),
    )


class NotifyPayload(BaseModel):
    request_id: str
    message: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/notify")
async def notify(payload: NotifyPayload):
    received_at = now_ms()
    body = payload.model_dump()
    client = http_client
    try:
        write_resp = await client.post(f"{DB_SERVICE_URL}/write", json=body)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"db-service unreachable: {exc}") from exc
    if write_resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"db-service failed: {write_resp.text}")
    db_write_done_at = now_ms()

    push_resp, push_attempts = await push_with_retry(client, body, payload.request_id)
    ws_push_done_at = now_ms()

    return {
        "request_id": payload.request_id,
        "status": "delivered",
        "db_row": write_resp.json(),
        "push_result": push_resp.json(),
        "push_attempts": push_attempts,
        "timeline": {
            "received_at": received_at,
            "db_write_done_at": db_write_done_at,
            "ws_push_done_at": ws_push_done_at,
        },
    }
