from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
import pytz

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.email import send_email

AIRBYTE_CONNECTION_ID = "8e8d970b-4d1c-4e36-96c7-394865abace7"
DBT_PROJECT_DIR = "/opt/airflow/dbt/health"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"
DBT_ARTIFACTS_DIR = "/opt/airflow/dbt_artifacts"

DEFAULT_ARGS = {
    "owner": "wes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ET = pytz.timezone("America/New_York")

def _fmt(dt):
    return dt.astimezone(ET).strftime("%Y-%m-%d %I:%M %p ET")

def on_failure(context):
    send_email(
        to="wmatheny07@gmail.com",
        subject=f"❌ health_data_pipeline failed — {context['task_instance'].task_id}",
        html_content=f"""
        <b>DAG:</b> health_data_pipeline<br>
        <b>Task:</b> {context['task_instance'].task_id}<br>
        <b>Execution Date:</b> {_fmt(context['execution_date'])}<br>
        <b>Log URL:</b> <a href="{context['task_instance'].log_url}">View Logs</a><br>
        <b>Exception:</b> {context.get('exception', 'N/A')}
        """,
    )

def on_success(context):
    dag_run = context["dag_run"]
    stats = XCom.get_one(
        run_id=dag_run.run_id,
        task_id="trigger_airbyte_sync",
        key="airbyte_stats",
        dag_id="health_data_pipeline",
        include_prior_dates=False,
    ) or {}
    rows_synced = stats.get("rowsSynced", stats.get("recordsSynced", "N/A"))
    bytes_synced = stats.get("bytesSynced", "N/A")
    if isinstance(bytes_synced, (int, float)):
        bytes_synced = f"{bytes_synced / 1_048_576:.2f} MB" if bytes_synced > 0 else "0 MB"

    send_email(
        to="wmatheny07@gmail.com",
        subject="✅ health_data_pipeline succeeded",
        html_content=f"""
        <b>DAG:</b> health_data_pipeline<br>
        <b>Completed:</b> {_fmt(context['execution_date'])}<br>
        <br>
        <b>Airbyte Sync Results:</b><br>
        <ul>
            <li>Rows synced: {rows_synced}</li>
            <li>Bytes synced: {bytes_synced}</li>
        </ul>
        """,
    )

with DAG(
    dag_id="health_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Sync health data from MinIO via Airbyte then refresh dbt health marts",
    schedule_interval='0 8-22 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_success_callback=on_success,
    on_failure_callback=on_failure,
    tags=["health", "dbt", "airbyte"],
) as dag:

    COMMON_ENV = {
        "ANALYTICS_DB_PASSWORD": "{{ conn.analytics_postgres.password }}",
        "ANALYTICS_DB_HOST": "postgres",
        "ANALYTICS_DB_PORT": "5432",
        "ANALYTICS_DB_USER": "analytics",
        "ANALYTICS_DB_NAME": "analytics",
    }

    def trigger_and_wait_for_airbyte(**context):
        import requests

        conn = BaseHook.get_connection("airbyte_api")
        base_url = f"{conn.schema}://{conn.host}:{conn.port}" if conn.port else f"{conn.schema}://{conn.host}"

        # Get Bearer token using Client-Id / Client-Secret
        token_resp = requests.post(
            f"{base_url}/api/v1/applications/token",
            json={"client_id": conn.login, "client_secret": conn.password},
            timeout=30,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Trigger sync
        sync_resp = requests.post(
            f"{base_url}/api/public/v1/jobs",
            json={"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"},
            headers=headers,
            timeout=30,
        )
        sync_resp.raise_for_status()
        job_id = sync_resp.json()["jobId"]
        print(f"Airbyte job triggered: {job_id}")

        # Poll until complete
        job_data = {}
        while True:
            status_resp = requests.get(
                f"{base_url}/api/public/v1/jobs/{job_id}",
                headers=headers,
                timeout=30,
            )
            status_resp.raise_for_status()
            job_data = status_resp.json()
            status = job_data["status"]
            print(f"Airbyte job {job_id} status: {status}")
            if status == "succeeded":
                break
            if status in ("failed", "cancelled"):
                raise RuntimeError(f"Airbyte sync {job_id} did not succeed: status={status}")
            time.sleep(30)

        # Push stats to XCom
        context["ti"].xcom_push(key="airbyte_stats", value=job_data)

    trigger_and_wait = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_and_wait_for_airbyte,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        mkdir -p {DBT_ARTIFACTS_DIR}/dbt_packages {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        if [ -f packages.yml ]; then
            dbt deps \
                --profiles-dir {DBT_PROFILES_DIR} \
                --log-path {DBT_ARTIFACTS_DIR}/logs
        else
            echo "No packages.yml; skipping dbt deps"
        fi
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build_health_marts",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        mkdir -p {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        dbt build \
            --profiles-dir {DBT_PROFILES_DIR} \
            --target-path {DBT_ARTIFACTS_DIR}/target \
            --log-path {DBT_ARTIFACTS_DIR}/logs \
            --select "staging.health marts.health elementary" \
            --exclude "mv_espn_play_stat"
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        dbt docs generate \
            --profiles-dir {DBT_PROFILES_DIR} \
            --target-path {DBT_ARTIFACTS_DIR}/target \
            --log-path {DBT_ARTIFACTS_DIR}/logs
        cp {DBT_ARTIFACTS_DIR}/target/index.html /opt/dbt-docs/index.html
        cp {DBT_ARTIFACTS_DIR}/target/catalog.json /opt/dbt-docs/catalog.json
        cp {DBT_ARTIFACTS_DIR}/target/manifest.json /opt/dbt-docs/manifest.json
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    edr_report = BashOperator(
        task_id="edr_report",
        bash_command=f"""
        set -euo pipefail
        edr monitor report \
            --profiles-dir {DBT_PROFILES_DIR}
        cp {DBT_PROJECT_DIR}/elementary.html /opt/dbt-docs/elementary_report.html
        """,
        env=COMMON_ENV,
        append_env=True,
    )

    trigger_and_wait >> dbt_deps >> dbt_build >> [dbt_docs, edr_report]
