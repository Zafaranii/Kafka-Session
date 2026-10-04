import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

app = FastAPI(title="ws-service")


def now_ms() -> float:
    return time.time() * 1000

active_connections: list[WebSocket] = []


class PushPayload(BaseModel):
    request_id: str
    message: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.post("/push")
async def push_notification(payload: PushPayload):
    broadcast_at = now_ms()
    message = {**payload.model_dump(), "broadcast_at": broadcast_at}
    dead = []
    for ws in active_connections:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        active_connections.remove(ws)
    return {"delivered_to": len(active_connections), "broadcast_at": broadcast_at}
