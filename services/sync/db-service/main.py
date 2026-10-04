import os

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

app = FastAPI(title="db-service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

LOAD_TEST_PREFIX = "loadtest-"

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": os.environ.get("POSTGRES_DB", "notifdb"),
    "user": os.environ.get("POSTGRES_USER", "notifuser"),
    "password": os.environ.get("POSTGRES_PASSWORD", "notifpass"),
}


class NotifyPayload(BaseModel):
    request_id: str
    message: str


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/write")
def write_notification(payload: NotifyPayload):
    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO sync_notifications (request_id, message)
                VALUES (%s, %s)
                RETURNING id, request_id, message, created_at
                """,
                (payload.request_id, payload.message),
            )
            row = cur.fetchone()
        return row
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


# Demo-only utility, not part of either path's architecture: lets the
# frontend's "Reset Load Test Data" button clean up after the load-test
# script without touching notifications sent manually through the UI.
# Scoped to request_ids the load-test script tags with LOAD_TEST_PREFIX.
@app.post("/admin/reset-load-test")
def reset_load_test():
    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sync_notifications WHERE request_id LIKE %s",
                (f"{LOAD_TEST_PREFIX}%",),
            )
            sync_deleted = cur.rowcount
            cur.execute(
                "DELETE FROM stream_notifications WHERE request_id LIKE %s",
                (f"{LOAD_TEST_PREFIX}%",),
            )
            stream_deleted = cur.rowcount
        return {"sync_notifications_deleted": sync_deleted, "stream_notifications_deleted": stream_deleted}
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()
