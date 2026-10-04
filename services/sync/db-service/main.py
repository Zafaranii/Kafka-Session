import os
import threading
from contextlib import asynccontextmanager, contextmanager

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

db_pool: pool.ThreadedConnectionPool | None = None


# The pool is opened once when the server starts and closed on shutdown, so
# requests reuse connections instead of paying a connect + auth round trip
# each time.
@asynccontextmanager
async def lifespan(_: FastAPI):
    global db_pool
    db_pool = pool.ThreadedConnectionPool(POOL_SIZE, POOL_SIZE, **DB_CONFIG)
    try:
        yield
    finally:
        db_pool.closeall()
        db_pool = None


app = FastAPI(title="db-service", lifespan=lifespan)

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

# Fixed-size pool: psycopg2 closes connections above minconn as soon as they
# are returned, so min == max keeps them open between bursts. The semaphore
# makes surplus requests wait for a free connection (psycopg2 would raise
# instead), and gives up with a 503 after POOL_WAIT_SECONDS.
POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", 10))
POOL_WAIT_SECONDS = float(os.environ.get("DB_POOL_WAIT_SECONDS", 10))
pool_slots = threading.BoundedSemaphore(POOL_SIZE)


class NotifyPayload(BaseModel):
    request_id: str
    message: str


@contextmanager
def get_connection():
    if not pool_slots.acquire(timeout=POOL_WAIT_SECONDS):
        raise HTTPException(status_code=503, detail="database connection pool exhausted")
    try:
        conn = db_pool.getconn()
    except (pool.PoolError, psycopg2.OperationalError) as exc:
        pool_slots.release()
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    # Connections that errored (e.g. postgres restarted underneath us) are
    # closed rather than returned, so the pool replaces them on a later request.
    broken = False
    try:
        yield conn
    except psycopg2.OperationalError:
        broken = True
        raise
    finally:
        db_pool.putconn(conn, close=broken or bool(conn.closed))
        pool_slots.release()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/write")
def write_notification(payload: NotifyPayload):
    try:
        with get_connection() as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO sync_notifications (request_id, message)
                VALUES (%s, %s)
                RETURNING id, request_id, message, created_at
                """,
                (payload.request_id, payload.message),
            )
            return cur.fetchone()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# Demo-only utility: lets scripts/load-test.mjs poll how many load-test rows
# have landed in stream_notifications, to time how long the sink connector
# takes to drain the topic into Postgres.
@app.get("/admin/load-test-count")
def load_test_count():
    try:
        with get_connection() as conn, conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM stream_notifications WHERE request_id LIKE %s",
                (f"{LOAD_TEST_PREFIX}%",),
            )
            return {"stream_notifications": cur.fetchone()[0]}
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# Demo-only utility: the moment Postgres created each load-test row (epoch ms),
# so the frontend can show when the sink connector actually saved each request
# instead of estimating it from polled row counts. created_at is a UTC
# `timestamp without time zone` set by now() (the insert transaction's start).
@app.get("/admin/load-test-created-at")
def load_test_created_at():
    try:
        with get_connection() as conn, conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id, (extract(epoch from created_at) * 1000)::float8
                FROM stream_notifications
                WHERE request_id LIKE %s
                """,
                (f"{LOAD_TEST_PREFIX}%",),
            )
            return {"rows": {request_id: created_ms for request_id, created_ms in cur.fetchall()}}
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# Demo-only utility, not part of either path's architecture: lets the
# frontend's "Reset Load Test Data" button clean up after the load-test
# script without touching notifications sent manually through the UI.
# Scoped to request_ids the load-test script tags with LOAD_TEST_PREFIX.
@app.post("/admin/reset-load-test")
def reset_load_test():
    try:
        with get_connection() as conn, conn, conn.cursor() as cur:
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
