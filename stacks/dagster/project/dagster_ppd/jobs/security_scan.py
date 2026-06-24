"""
Security scan jobs — trigger the security-scanner service and surface results in Dagster.

The security-scanner container (hostname: security-scanner, port 8000) owns the
actual Trivy/Syft/LangGraph logic. These Dagster ops just orchestrate it so scans
are visible in the Dagster UI and can be triggered manually from there.

Service URL is controlled by SECURITY_SCANNER_URL (default: http://security-scanner:8000).
"""
from __future__ import annotations

import os

import requests
from dagster import In, Nothing, Out, get_dagster_logger, job, op

_SCANNER_URL = os.environ.get("SECURITY_SCANNER_URL", "http://security-scanner:8000")
_TIMEOUT = 15  # seconds — trigger calls return immediately (async on the scanner side)


@op(out=Out(Nothing))
def trigger_full_image_scan(context):
    logger = get_dagster_logger()
    logger.info("Triggering full CVE + SBOM scan at %s", _SCANNER_URL)
    resp = requests.post(f"{_SCANNER_URL}/scan/full", timeout=_TIMEOUT)
    resp.raise_for_status()
    logger.info("Full scan accepted by scanner: %s", resp.json())


@op(out=Out(Nothing))
def trigger_version_drift_check(context):
    logger = get_dagster_logger()
    logger.info("Triggering version drift check at %s", _SCANNER_URL)
    resp = requests.post(f"{_SCANNER_URL}/scan/versions", timeout=_TIMEOUT)
    resp.raise_for_status()
    logger.info("Version check accepted by scanner: %s", resp.json())


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def log_scan_status(context):
    """Fetch the latest scan run status and surface it in the Dagster run log."""
    logger = get_dagster_logger()
    resp = requests.get(f"{_SCANNER_URL}/scan/status", timeout=_TIMEOUT)
    resp.raise_for_status()
    runs = resp.json().get("runs", [])
    if runs:
        r = runs[0]
        logger.info(
            "Latest scan — type=%s status=%s images=%s findings=%s critical=%s high=%s",
            r.get("scan_type"),
            r.get("status"),
            r.get("images_scanned"),
            r.get("findings_count"),
            r.get("critical_count"),
            r.get("high_count"),
        )
    else:
        logger.info("No scan runs recorded yet")


@job
def security_image_scan_job():
    """Full CVE + SBOM scan of all running images, followed by LangGraph/Claude analysis."""
    log_scan_status(start=trigger_full_image_scan())


@job
def security_version_check_job():
    """Version drift check — compares running image tags against latest upstream releases."""
    trigger_version_drift_check()
