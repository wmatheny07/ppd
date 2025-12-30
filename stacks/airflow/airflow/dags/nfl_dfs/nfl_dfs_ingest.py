from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.datasets import Dataset

DEFAULT_ARGS = {
    "owner": "wes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DFS_INGESTED = Dataset("dataset://dfs/ingested")

with DAG(
    dag_id="nfl_dfs_ingest",
    description="Ingest DK + depth + injuries then build active player pool",
    template_searchpath=["/opt/airflow/sql", "/opt/airflow/dags"],
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 */2 * * 6,0",  # weekends every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=["nfl", "dfs"],
) as dag:

    bootstrap = PostgresOperator(
        task_id="bootstrap_nfl_dfs_ddl",
        postgres_conn_id="analytics_postgres",
        sql="nfl_dfs_bootstrap.sql",
    )

    depth_ingest = BashOperator(
        task_id="depth_chart_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/pfn/pfn_depth_ingest.py
        """,
        env={
            "DEPTH_SCHEMA": "{{ var.value.get('NFL_DFS_SCHEMA', 'nfl_dfs') }}",
            "DEPTH_SOURCE": "pfn",
            "DEPTH_URL": "https://www.profootballnetwork.com/nfl/depth-chart/",
        },
        append_env=True,
    )

    dk_salaries_ingest = BashOperator(
        task_id="dk_salaries_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/dk/dk_ingest.py --once
        """,
        env={
            "DK_SCHEMA": "{{ var.value.get('NFL_DFS_SCHEMA', 'nfl_dfs') }}",
            "DK_INBOX": "{{ var.value.get('DK_INBOX', '/data/inbox') }}",
            "DK_ARCHIVE": "{{ var.value.get('DK_ARCHIVE', '/data/archive') }}",
            "DK_ERROR": "{{ var.value.get('DK_ERROR', '/data/error') }}",
            "DK_POLL_SECONDS": "{{ var.value.get('DK_POLL_SECONDS', '60') }}",
            "DB_HOST": "{{ var.value.get('DB_HOST', 'postgres') }}",
            "DB_PORT": "{{ var.value.get('DB_PORT', '5432') }}",
            "DB_NAME": "{{ var.value.get('DB_NAME', 'analytics') }}",
            "DB_USER": "{{ var.value.get('DB_USER', 'analytics') }}",
            "DB_PASSWORD": "{{ var.value.get('DB_PASSWORD', '') }}",
        },
    )

    espn_injury_rpt_ingest = BashOperator(
        task_id="espn_injury_rpt_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/espn/espn_injury_report.py
        """,
    )

    build_player_pool = PostgresOperator(
        task_id="build_active_player_pool",
        postgres_conn_id="analytics_postgres",
        sql="build_active_player_pool.sql",  # <-- move your DO $$ into a .sql file
    )

    publish_dataset = BashOperator(
        task_id="publish_dfs_dataset",
        bash_command="echo dfs_ingested",
        outlets=[DFS_INGESTED],
    )

    bootstrap >> dk_salaries_ingest >> depth_ingest >> espn_injury_rpt_ingest >> build_player_pool >> publish_dataset