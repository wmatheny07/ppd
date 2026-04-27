from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras
from dagster import ConfigurableResource, EnvVar


class PostgresResource(ConfigurableResource):
    host: str = EnvVar("ANALYTICS_DB_HOST")
    port: int = 5432
    database: str = EnvVar("ANALYTICS_DB_NAME")
    user: str = EnvVar("ANALYTICS_DB_USER")
    password: str = EnvVar("ANALYTICS_DB_PASSWORD")

    @contextmanager
    def _connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_one(self, query: str, params: tuple = ()) -> dict | None:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def fetch_all(self, query: str, params: tuple = ()) -> list[dict]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]

    def execute(self, query: str, params: tuple = ()) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def execute_many(self, query: str, params_seq: list[tuple]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, query, params_seq)

    def log_pipeline_event(
        self,
        stage: str,
        status: str,
        document_id: int | None = None,
        message: str | None = None,
        dagster_run_id: str | None = None,
    ) -> None:
        self.execute(
            """
            INSERT INTO mail_raw.mail_processing_log
                (document_id, stage, status, message, dagster_run_id, logged_ts)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (document_id, stage, status, message, dagster_run_id),
        )
