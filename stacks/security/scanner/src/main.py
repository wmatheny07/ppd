"""
Security scanner service entry point.

Exposes a FastAPI app that:
- Applies the Postgres schema on startup
- Registers APScheduler jobs for autonomous scanning
- Provides HTTP endpoints for Dagster to trigger scans and read status
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Coroutine

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .db import (
    ensure_schema, get_recent_scan_runs,
    get_dashboard_summary, get_findings_paged, update_finding_status,
    get_action_plans, update_action_plan_status,
    get_version_drift_latest, get_distinct_images,
)
from .scanner import run_full_scan, run_version_check

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_scheduler = BackgroundScheduler(timezone="America/New_York")

# One lock per scan type — prevents overlapping runs triggered by Dagster retries,
# the APScheduler, or rapid manual calls arriving before a scan completes.
_full_scan_lock = threading.Lock()
_version_check_lock = threading.Lock()


def _fire_and_forget(coro: Coroutine, lock: threading.Lock, scan_label: str) -> bool:
    """
    Run an async coroutine in a dedicated thread with its own event loop.
    Returns False (and drops the request) if the same scan type is already running.
    This fully decouples heavy blocking work (Trivy/Syft subprocesses, psycopg2,
    boto3) from FastAPI's event loop so the HTTP response is never delayed.
    """
    if not lock.acquire(blocking=False):
        logger.warning("%s already in progress — ignoring duplicate trigger", scan_label)
        return False

    def _target():
        try:
            asyncio.run(coro)
        finally:
            lock.release()

    threading.Thread(target=_target, daemon=True, name=scan_label).start()
    return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Apply schema idempotently before accepting traffic
    ensure_schema()

    scan_cron    = os.environ.get("SCAN_SCHEDULE_CRON", "0 2 * * *")
    version_cron = os.environ.get("VERSION_CHECK_SCHEDULE_CRON", "0 3 * * 1")

    _scheduler.add_job(
        lambda: _fire_and_forget(run_full_scan(), _full_scan_lock, "full_scan"),
        CronTrigger.from_crontab(scan_cron),
        id="full_scan",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        lambda: _fire_and_forget(run_version_check(), _version_check_lock, "version_check"),
        CronTrigger.from_crontab(version_cron),
        id="version_check",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()

    logger.info("Security scanner ready | scan=%s | version_check=%s", scan_cron, version_cron)
    yield
    _scheduler.shutdown()


app = FastAPI(title="PPD Security Scanner", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/scan/full")
async def trigger_full_scan():
    """Trigger a full CVE + SBOM scan. Returns immediately; scan runs in a background thread.
    Returns 409 if a full scan is already in progress."""
    started = _fire_and_forget(run_full_scan(), _full_scan_lock, "full_scan")
    if not started:
        return {"status": "skipped", "reason": "full scan already in progress"}
    return {"status": "accepted", "type": "full_scan"}


@app.post("/scan/versions")
async def trigger_version_check():
    """Trigger a version drift check. Returns immediately; check runs in a background thread.
    Returns skipped if a version check is already in progress."""
    started = _fire_and_forget(run_version_check(), _version_check_lock, "version_check")
    if not started:
        return {"status": "skipped", "reason": "version check already in progress"}
    return {"status": "accepted", "type": "version_check"}


@app.get("/scan/status")
async def scan_status():
    """Return the 10 most recent scan runs."""
    runs = get_recent_scan_runs(limit=10)
    for run in runs:
        for key in ("started_at", "completed_at"):
            if run.get(key) is not None:
                run[key] = run[key].isoformat()
    return {"runs": runs}


# ─── Frontend API ─────────────────────────────────────────────────────────────

class StatusUpdate(BaseModel):
    status: str


@app.get("/api/dashboard")
async def api_dashboard():
    return get_dashboard_summary()


@app.get("/api/findings")
async def api_findings(
    severity: str | None = None,
    image: str | None = None,
    status: str = "open",
    cve_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
):
    findings, total = get_findings_paged(severity, image, status, cve_id, limit, offset)
    return {"findings": findings, "total": total, "limit": limit, "offset": offset}


@app.patch("/api/findings/{finding_id}")
async def api_update_finding(finding_id: int, body: StatusUpdate):
    valid = {"open", "resolved", "accepted", "false_positive"}
    if body.status not in valid:
        raise HTTPException(status_code=400, detail=f"status must be one of {valid}")
    update_finding_status(finding_id, body.status)
    return {"ok": True}


@app.get("/api/action-plans")
async def api_action_plans(priority: str | None = None, status: str | None = None):
    return {"plans": get_action_plans(priority, status)}


@app.patch("/api/action-plans/{plan_id}")
async def api_update_action_plan(plan_id: int, body: StatusUpdate):
    valid = {"open", "in_progress", "completed"}
    if body.status not in valid:
        raise HTTPException(status_code=400, detail=f"status must be one of {valid}")
    update_action_plan_status(plan_id, body.status)
    return {"ok": True}


@app.get("/api/version-drift")
async def api_version_drift():
    return {"drift": get_version_drift_latest()}


@app.get("/api/images")
async def api_images():
    return {"images": get_distinct_images()}
