import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _conn_params() -> dict:
    return dict(
        host=os.environ["ANALYTICS_DB_HOST"],
        port=int(os.environ.get("ANALYTICS_DB_PORT", 5432)),
        dbname=os.environ["ANALYTICS_DB_NAME"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
    )


@contextmanager
def get_connection():
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or None)
            return [dict(row) for row in cur.fetchall()]


def fetch_one(query: str, params: tuple = ()) -> dict | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or None)
            row = cur.fetchone()
            return dict(row) if row else None
