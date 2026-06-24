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


# ── Saved searches ────────────────────────────────────────────────────────────

def ensure_saved_searches_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mail_raw.saved_searches (
                    id         SERIAL PRIMARY KEY,
                    user_email TEXT        NOT NULL,
                    query      TEXT        NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (user_email, query)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_saved_searches_user
                ON mail_raw.saved_searches (user_email, created_at DESC)
            """)
        conn.commit()


def get_saved_searches(user_email: str) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, query, created_at
                FROM mail_raw.saved_searches
                WHERE user_email = %s
                ORDER BY created_at DESC
                LIMIT 30
                """,
                (user_email,),
            )
            rows = cur.fetchall()
            return [
                {**dict(r), "created_at": r["created_at"].isoformat()}
                for r in rows
            ]


def create_saved_search(user_email: str, query: str) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO mail_raw.saved_searches (user_email, query)
                VALUES (%s, %s)
                ON CONFLICT (user_email, query) DO UPDATE SET created_at = NOW()
                RETURNING id, query, created_at
                """,
                (user_email, query),
            )
            row = cur.fetchone()
        conn.commit()
    return {**dict(row), "created_at": row["created_at"].isoformat()}


def delete_saved_search(user_email: str, search_id: int) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM mail_raw.saved_searches WHERE id = %s AND user_email = %s",
                (search_id, user_email),
            )
            deleted = cur.rowcount
        conn.commit()
    return deleted > 0
