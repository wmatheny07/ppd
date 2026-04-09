"""
NFL DFS ingest job: bootstrap -> dk_salaries -> depth_chart -> injuries -> player_pool

Equivalent to Airflow DAG: nfl_dfs_ingest (schedule: every 2 hours on weekends)

Required env vars:
  ANALYTICS_DB_URI
  DK_INBOX, DK_ARCHIVE, DK_ERROR, DK_SCHEMA
"""
from __future__ import annotations

import os
import subprocess

import psycopg2
from dagster import In, Nothing, Out, get_dagster_logger, job, op

JOBS_DIR = "/opt/dagster/jobs"
SQL_DIR = "/opt/dagster/sql"


def _analytics_uri() -> str:
    return os.environ.get(
        "ANALYTICS_DB_URI",
        (
            f"postgresql://{os.environ.get('ANALYTICS_DB_USER', '')}:"
            f"{os.environ.get('ANALYTICS_DB_PASSWORD', '')}@"
            f"{os.environ.get('ANALYTICS_DB_HOST', 'postgres')}:"
            f"{os.environ.get('ANALYTICS_DB_PORT', '5432')}/"
            f"{os.environ.get('ANALYTICS_DB_NAME', 'analytics')}"
        ),
    )


@op(out=Out(Nothing))
def bootstrap_nfl_dfs_ddl(context):
    logger = get_dagster_logger()
    logger.info("Running nfl_dfs bootstrap DDL")
    with open(f"{SQL_DIR}/nfl_dfs_bootstrap.sql") as f:
        sql = f.read()
    with psycopg2.connect(_analytics_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def dk_salaries_ingest(context):
    logger = get_dagster_logger()
    logger.info("Ingesting DraftKings salary CSVs")
    env = {
        **os.environ,
        "DK_SCHEMA": os.environ.get("DK_SCHEMA", "nfl_dfs"),
        "DK_INBOX": os.environ.get("DK_INBOX", "/data/inbox"),
        "DK_ARCHIVE": os.environ.get("DK_ARCHIVE", "/data/archive"),
        "DK_ERROR": os.environ.get("DK_ERROR", "/data/error"),
        "DK_POLL_SECONDS": os.environ.get("DK_POLL_SECONDS", "60"),
        "ANALYTICS_DB_URI": _analytics_uri(),
    }
    subprocess.run(
        ["python3", f"{JOBS_DIR}/dk/dk_ingest.py", "--once"],
        check=True,
        env=env,
    )


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def depth_chart_ingest(context):
    logger = get_dagster_logger()
    logger.info("Scraping Pro Football Network depth charts")
    env = {
        **os.environ,
        "DEPTH_SCHEMA": os.environ.get("DK_SCHEMA", "nfl_dfs"),
        "DEPTH_SOURCE": "pfn",
        "DEPTH_URL": "https://www.profootballnetwork.com/nfl/depth-chart/",
        "ANALYTICS_DB_URI": _analytics_uri(),
    }
    subprocess.run(
        ["python3", f"{JOBS_DIR}/pfn/pfn_depth_ingest.py"],
        check=True,
        env=env,
    )


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def espn_injury_rpt_ingest(context):
    logger = get_dagster_logger()
    logger.info("Fetching ESPN injury reports")
    env = {
        **os.environ,
        "ANALYTICS_DB_URI": _analytics_uri(),
    }
    subprocess.run(
        ["python3", f"{JOBS_DIR}/espn/espn_injury_report.py"],
        check=True,
        env=env,
    )


@op(ins={"start": In(Nothing)})
def build_active_player_pool(context):
    logger = get_dagster_logger()
    logger.info("Building active player pool from SQL")
    with open(f"{SQL_DIR}/build_active_player_pool.sql") as f:
        sql = f.read()
    with psycopg2.connect(_analytics_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


@job(tags={"domain": "nfl", "team": "dfs"})
def nfl_dfs_ingest_job():
    bootstrapped = bootstrap_nfl_dfs_ddl()
    dk_done = dk_salaries_ingest(bootstrapped)
    depth_done = depth_chart_ingest(dk_done)
    injuries_done = espn_injury_rpt_ingest(depth_done)
    build_active_player_pool(injuries_done)
