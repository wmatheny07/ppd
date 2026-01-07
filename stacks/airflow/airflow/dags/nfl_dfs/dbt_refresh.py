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
DBT_PROFILES_DIR = "/opt/airflow/.dbt"
DBT_PROJECT_DIR  = "/opt/airflow/dbt/espn"
DBT_ARTIFACTS_DIR = "/opt/airflow/dbt_artifacts"

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

    COMMON_ENV = {
        "ANALYTICS_DB_PASSWORD": "{{ var.value.ANALYTICS_DB_PASSWORD }}",
        # Add any other env vars referenced by profiles.yml
    }

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}

        mkdir -p {DBT_ARTIFACTS_DIR}/dbt_packages {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target

        if [ -f packages.yml ]; then
        dbt deps \
            --profiles-dir {DBT_PROFILES_DIR} \
            --packages-install-path {DBT_ARTIFACTS_DIR}/dbt_packages
        else
        echo "No packages.yml; skipping dbt deps"
        fi
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        set -euo pipefail
        echo "Using profiles dir: {DBT_PROFILES_DIR}"
        ls -la {DBT_PROFILES_DIR}
        cd {DBT_PROJECT_DIR}

        mkdir -p {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        test -w {DBT_ARTIFACTS_DIR} || (echo "Artifacts dir not writable" && exit 2)

        dbt build \
        --profiles-dir {DBT_PROFILES_DIR} \
        --target-path {DBT_ARTIFACTS_DIR}/target \
        --log-path {DBT_ARTIFACTS_DIR}/logs \
        --select "espn_analytics.espn+ espn_analytics.marts+ espn_analytics.staging+"
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_deps >> dbt_build
