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

import json
import os
import subprocess
import time
from datetime import timedelta

import resend

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
DBT_PROJECT_DIR = "/opt/dagster/dbt/health"
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/dbt/profiles")
DBT_ARTIFACTS_DIR = "/opt/dagster/dbt_artifacts"
DBT_STATE_DIR = f"{DBT_ARTIFACTS_DIR}/state"

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
    """Send a Resend alert when any op in health_pipeline_job fails."""
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        return

    op_name = context.op.name
    run_id = context.run_id

    try:
        resend.api_key = api_key
        resend.Emails.send({
            "from": "alerts@peakprecisiondata.com",
            "to": "wmatheny07@gmail.com",
            "subject": f"health_pipeline_job failed — {op_name}",
            "html": f"""
            <b>Job:</b> health_pipeline_job<br>
            <b>Op:</b> {op_name}<br>
            <b>Run ID:</b> {run_id}<br>
            """,
        })
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
def promote_freshness_state(context):
    """Snapshot current sources.json → state/ before running freshness.
    No-op on bootstrap when no sources.json exists yet."""
    logger = get_dagster_logger()
    logger.info("Promoting sources.json to state/")
    subprocess.run(
        f"""
        mkdir -p {DBT_STATE_DIR}
        if [ -f {DBT_ARTIFACTS_DIR}/target/sources.json ]; then
            cp {DBT_ARTIFACTS_DIR}/target/sources.json {DBT_STATE_DIR}/sources.json
            echo "Promoted sources.json to state/"
        else
            echo "No sources.json found — skipping promotion (bootstrap run)"
        fi
        """,
        shell=True,
        check=True,
        executable="/bin/bash",
    )


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def dbt_source_freshness(context):
    logger = get_dagster_logger()
    logger.info("Checking dbt source freshness")

    process = subprocess.Popen(
        f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        mkdir -p {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        dbt source freshness \
            --profiles-dir {DBT_PROFILES_DIR} \
            --target-path {DBT_ARTIFACTS_DIR}/target \
            --log-path {DBT_ARTIFACTS_DIR}/logs \
            --select "source:health source:raw_weather"
        """,
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=COMMON_ENV,
    )

    for line in process.stdout:
        logger.info(line.rstrip())

    process.wait()

    # Parse sources.json and emit a structured per-source freshness summary.
    sources_path = f"{DBT_ARTIFACTS_DIR}/target/sources.json"
    failed_sources = []
    try:
        with open(sources_path) as f:
            sources = json.load(f)

        for r in sources.get("results", []):
            source_name = r["unique_id"].split(".")[-1]
            status = r.get("status", "unknown")
            max_loaded_at = r.get("max_loaded_at", "unknown")
            age_s = r.get("max_loaded_at_time_ago_in_s")
            criteria = r.get("criteria", {})
            warn = criteria.get("warn_after") or {}
            error_thresh = criteria.get("error_after") or {}

            age_str = str(timedelta(seconds=int(age_s))) if age_s is not None else "unknown"
            warn_str = f"{warn['count']} {warn['period']}(s)" if warn.get("count") else "none"
            error_str = f"{error_thresh['count']} {error_thresh['period']}(s)" if error_thresh.get("count") else "none"

            msg = (
                f"[{status.upper()}] {source_name}: last loaded {age_str} ago "
                f"(at {max_loaded_at}) — warn after {warn_str}, error after {error_str}"
            )

            if status == "error":
                logger.error(msg)
                failed_sources.append(source_name)
            elif status == "warn":
                logger.warning(msg)
            else:
                logger.info(msg)

    except FileNotFoundError:
        logger.warning(f"sources.json not found at {sources_path} — cannot report per-source freshness details")

    if process.returncode != 0:
        detail = f"Failed sources: {', '.join(failed_sources)}" if failed_sources else "check dbt logs above for details"
        raise Exception(f"dbt source freshness check failed. {detail}")


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def dbt_build_health_marts(context):
    logger = get_dagster_logger()
    logger.info("Running dbt build for health marts")
    subprocess.run(
        f"""
        set -euo pipefail
        cd {DBT_PROJECT_DIR}
        mkdir -p {DBT_ARTIFACTS_DIR}/logs {DBT_ARTIFACTS_DIR}/target
        if [ -f {DBT_STATE_DIR}/sources.json ]; then
            echo "State found — building fresher sources only"
            dbt build \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target-path {DBT_ARTIFACTS_DIR}/target \
                --log-path {DBT_ARTIFACTS_DIR}/logs \
                --select "tag:health" \
                --state {DBT_STATE_DIR}
        else
            echo "No state found — full build (bootstrap run)"
            dbt build \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target-path {DBT_ARTIFACTS_DIR}/target \
                --log-path {DBT_ARTIFACTS_DIR}/logs \
                --select "staging.health marts.health" \
                --exclude "mv_espn_play_stat"
        fi
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


# @op(ins={"start": In(Nothing)})
# def edr_report(context):
#     logger = get_dagster_logger()
#     logger.info("Running Elementary data monitoring report")
#     subprocess.run(
#         f"""
#         set -euo pipefail
#         edr monitor report --profiles-dir {DBT_PROFILES_DIR}
#         cp {DBT_PROJECT_DIR}/elementary.html /opt/dbt-docs/elementary_report.html
#         """,
#         shell=True,
#         check=True,
#         executable="/bin/bash",
#         env=COMMON_ENV,
#     )


@job(
    tags={"domain": "health", "team": "dbt"},
    hooks={health_pipeline_failure_alert},
)
def health_pipeline_job():
    synced = trigger_airbyte_sync()
    deps_done = dbt_deps_health(synced)
    promoted = promote_freshness_state(deps_done)
    fresh = dbt_source_freshness(promoted)
    built = dbt_build_health_marts(fresh)
    dbt_docs_generate(built)
