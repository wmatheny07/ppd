"""
Health data pipeline job: airbyte_sync -> dbt_deps -> dbt_build -> [dbt_docs, edr_report]

Equivalent to Airflow DAG: health_data_pipeline (schedule: hourly 8am-10pm ET)

Required env vars:
  AIRBYTE_HOST, AIRBYTE_PORT, AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET
  ANALYTICS_DB_HOST, ANALYTICS_DB_PORT, ANALYTICS_DB_NAME,
  ANALYTICS_DB_USER, ANALYTICS_DB_PASSWORD
  GMAIL_APP_USER, GMAIL_APP_PASSWORD  (for failure alerts)
"""
from __future__ import annotations

import os
import smtplib
import subprocess
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from dagster import (
    DagsterEventType,
    In,
    Nothing,
    Out,
    failure_hook,
    get_dagster_logger,
    job,
    op,
)

AIRBYTE_CONNECTION_ID = "8e8d970b-4d1c-4e36-96c7-394865abace7"
DBT_PROJECT_DIR = "/opt/airflow/dbt/health"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"
DBT_ARTIFACTS_DIR = "/opt/airflow/dbt_artifacts"

COMMON_ENV = {
    **os.environ,
    "ANALYTICS_DB_HOST": os.environ.get("ANALYTICS_DB_HOST", "postgres"),
    "ANALYTICS_DB_PORT": os.environ.get("ANALYTICS_DB_PORT", "5432"),
    "ANALYTICS_DB_NAME": os.environ.get("ANALYTICS_DB_NAME", "analytics"),
    "ANALYTICS_DB_USER": os.environ.get("ANALYTICS_DB_USER", ""),
    "ANALYTICS_DB_PASSWORD": os.environ.get("ANALYTICS_DB_PASSWORD", ""),
}


@failure_hook
def health_pipeline_failure_alert(context):
    """Send a Gmail alert when any op in health_pipeline_job fails."""
    smtp_user = os.environ.get("GMAIL_APP_USER", "")
    smtp_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not smtp_user or not smtp_pass:
        return

    op_name = context.op.name
    run_id = context.run_id

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"health_pipeline_job failed — {op_name}"
    msg["From"] = smtp_user
    msg["To"] = "wmatheny07@gmail.com"
    body = f"""
    <b>Job:</b> health_pipeline_job<br>
    <b>Op:</b> {op_name}<br>
    <b>Run ID:</b> {run_id}<br>
    """
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, "wmatheny07@gmail.com", msg.as_string())
    except Exception as exc:
        get_dagster_logger().warning(f"Failed to send failure email: {exc}")


@op(out=Out(Nothing))
def trigger_airbyte_sync(context):
    logger = get_dagster_logger()

    airbyte_host = os.environ.get("AIRBYTE_HOST", "localhost")
    airbyte_port = os.environ.get("AIRBYTE_PORT", "8001")
    base_url = f"http://{airbyte_host}:{airbyte_port}"
    client_id = os.environ["AIRBYTE_CLIENT_ID"]
    client_secret = os.environ["AIRBYTE_CLIENT_SECRET"]

    # Obtain bearer token
    token_resp = requests.post(
        f"{base_url}/api/v1/applications/token",
        json={"client_id": client_id, "client_secret": client_secret},
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
    logger.info(f"Airbyte job triggered: {job_id}")

    # Poll until complete
    while True:
        status_resp = requests.get(
            f"{base_url}/api/public/v1/jobs/{job_id}",
            headers=headers,
            timeout=30,
        )
        status_resp.raise_for_status()
        job_data = status_resp.json()
        status = job_data["status"]
        logger.info(f"Airbyte job {job_id} status: {status}")
        if status == "succeeded":
            break
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte sync {job_id} did not succeed: status={status}")
        time.sleep(30)


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def dbt_deps_health(context):
    logger = get_dagster_logger()
    logger.info("Installing dbt packages for health project")
    subprocess.run(
        f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        mkdir -p {DBT_ARTIFACTS_DIR}/dbt_packages {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        if [ -f packages.yml ]; then
            dbt deps --profiles-dir {DBT_PROFILES_DIR} --log-path {DBT_ARTIFACTS_DIR}/logs
        else
            echo "No packages.yml; skipping dbt deps"
        fi
        """,
        shell=True,
        check=True,
        executable="/bin/bash",
        env=COMMON_ENV,
    )


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def dbt_build_health_marts(context):
    logger = get_dagster_logger()
    logger.info("Running dbt build for health marts")
    subprocess.run(
        f"""
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
        shell=True,
        check=True,
        executable="/bin/bash",
        env=COMMON_ENV,
    )


@op(ins={"start": In(Nothing)})
def dbt_docs_generate(context):
    logger = get_dagster_logger()
    logger.info("Generating dbt docs for health project")
    subprocess.run(
        f"""
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
        shell=True,
        check=True,
        executable="/bin/bash",
        env=COMMON_ENV,
    )


@op(ins={"start": In(Nothing)})
def edr_report(context):
    logger = get_dagster_logger()
    logger.info("Running Elementary data monitoring report")
    subprocess.run(
        f"""
        set -euo pipefail
        edr monitor report --profiles-dir {DBT_PROFILES_DIR}
        cp {DBT_PROJECT_DIR}/elementary.html /opt/dbt-docs/elementary_report.html
        """,
        shell=True,
        check=True,
        executable="/bin/bash",
        env=COMMON_ENV,
    )


@job(
    tags={"domain": "health", "team": "dbt"},
    hooks={health_pipeline_failure_alert},
)
def health_pipeline_job():
    synced = trigger_airbyte_sync()
    deps_done = dbt_deps_health(synced)
    built = dbt_build_health_marts(deps_done)
    # docs and edr run in parallel after dbt build
    dbt_docs_generate(built)
    edr_report(built)
