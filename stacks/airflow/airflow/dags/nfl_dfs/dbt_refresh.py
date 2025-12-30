from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

DEFAULT_ARGS = {
    "owner": "wes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ESPN_INGESTED = Dataset("dataset://espn/ingested")
DFS_INGESTED = Dataset("dataset://dfs/ingested")

DBT_DIR = "/opt/airflow/stacks/dbt/project"  # <-- adjust

with DAG(
    dag_id="dbt_refresh_analytics",
    description="Run dbt build when ESPN or DFS ingestion completes",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=[ESPN_INGESTED, DFS_INGESTED],  # dataset-triggered
    catchup=False,
    max_active_runs=1,
    tags=["dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_DIR}
        dbt deps --profiles-dir /opt/airflow/.dbt
        """,
        env={"ANALYTICS_DB_PASSWORD": "{{ var.value.ANALYTICS_DB_PASSWORD }}"},
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_DIR}
        dbt build --profiles-dir /opt/airflow/.dbt --select "marts.espn+ marts.dfs+"
        """,
        env={"ANALYTICS_DB_PASSWORD": "{{ var.value.ANALYTICS_DB_PASSWORD }}"},
        append_env=True,
    )

    dbt_deps >> dbt_build
