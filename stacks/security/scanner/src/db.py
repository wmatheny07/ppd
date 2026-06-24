from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_SCHEMA_SQL = Path(__file__).parent / "schema.sql"


def _conn_params() -> dict:
    return dict(
        host=os.environ["ANALYTICS_DB_HOST"],
        port=int(os.environ.get("ANALYTICS_DB_PORT", 5432)),
        dbname=os.environ["ANALYTICS_DB_NAME"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
    )


@contextmanager
def get_conn():
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema() -> None:
    """Idempotent: create security schema + tables if they don't exist."""
    sql = _SCHEMA_SQL.read_text()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("Security schema ready")


# ─── Scan runs ────────────────────────────────────────────────────────────────

def begin_scan_run(run_id: str, scan_type: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security.scan_runs (run_id, scan_type, started_at, status)
                VALUES (%s, %s, NOW(), 'running')
                RETURNING id
                """,
                (run_id, scan_type),
            )
            return cur.fetchone()[0]


def finish_scan_run(
    scan_run_id: int,
    status: str,
    images_scanned: int = 0,
    findings_count: int = 0,
    critical_count: int = 0,
    high_count: int = 0,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE security.scan_runs
                SET status = %s,
                    completed_at = NOW(),
                    images_scanned = %s,
                    findings_count = %s,
                    critical_count = %s,
                    high_count = %s
                WHERE id = %s
                """,
                (status, images_scanned, findings_count, critical_count, high_count, scan_run_id),
            )


def get_recent_scan_runs(limit: int = 10) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, run_id, scan_type, started_at, completed_at, status,
                       images_scanned, findings_count, critical_count, high_count
                FROM security.scan_runs
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]


# ─── Findings ─────────────────────────────────────────────────────────────────

def upsert_finding(f: dict) -> None:
    """Insert or update a vulnerability finding; returns db id in f['id']."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security.findings (
                    scan_run_id, image_name, image_tag, cve_id, severity, cvss_score,
                    package_name, package_version, fixed_version, title, description,
                    published_at, vuln_references, first_seen_at, last_seen_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, NOW(), NOW()
                )
                ON CONFLICT (image_name, image_tag, cve_id, package_name) DO UPDATE SET
                    severity      = EXCLUDED.severity,
                    cvss_score    = EXCLUDED.cvss_score,
                    fixed_version = EXCLUDED.fixed_version,
                    scan_run_id   = EXCLUDED.scan_run_id,
                    last_seen_at  = NOW()
                RETURNING id
                """,
                (
                    f.get("scan_run_id"),
                    f.get("image_name"),
                    f.get("image_tag"),
                    f.get("cve_id"),
                    f.get("severity"),
                    f.get("cvss_score"),
                    f.get("package_name"),
                    f.get("package_version"),
                    f.get("fixed_version"),
                    f.get("title"),
                    f.get("description"),
                    f.get("published_at"),
                    json.dumps(f.get("references", [])),
                ),
            )
            row = cur.fetchone()
            if row:
                f["id"] = row[0]


def get_findings_for_run(scan_run_id: int) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, image_name, image_tag, cve_id, severity, cvss_score,
                       package_name, fixed_version, title, status
                FROM security.findings
                WHERE scan_run_id = %s
                ORDER BY
                    CASE severity
                        WHEN 'CRITICAL' THEN 1
                        WHEN 'HIGH'     THEN 2
                        WHEN 'MEDIUM'   THEN 3
                        WHEN 'LOW'      THEN 4
                        ELSE 5
                    END
                """,
                (scan_run_id,),
            )
            return [dict(r) for r in cur.fetchall()]


# ─── SBOMs ────────────────────────────────────────────────────────────────────

def store_sbom_record(scan_run_id: int, img: dict, sbom: dict, minio_path: str | None = None) -> None:
    component_count = len(sbom.get("artifacts", []))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security.sboms
                    (scan_run_id, image_name, image_tag, sbom_format, minio_path, component_count, generated_at)
                VALUES (%s, %s, %s, 'syft-json', %s, %s, NOW())
                """,
                (scan_run_id, img["image_name"], img["image_tag"], minio_path, component_count),
            )


# ─── Version drift ────────────────────────────────────────────────────────────

def store_version_drift(scan_run_id: int, service_name: str, img: dict, drift: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security.version_drift
                    (scan_run_id, service_name, image_name, running_tag,
                     latest_tag, versions_behind, release_notes_url, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (service_name, scan_run_id) DO UPDATE SET
                    latest_tag       = EXCLUDED.latest_tag,
                    versions_behind  = EXCLUDED.versions_behind,
                    checked_at       = NOW()
                """,
                (
                    scan_run_id,
                    service_name,
                    img["image_name"],
                    img["image_tag"],
                    drift.get("latest_tag"),
                    drift.get("versions_behind"),
                    drift.get("release_notes_url"),
                ),
            )


# ─── Action plans ─────────────────────────────────────────────────────────────

def store_action_plan(plan: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO security.action_plans
                    (scan_run_id, priority, service_name, action_type,
                     title, description, steps, estimated_effort, status, generated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open', NOW())
                """,
                (
                    plan.get("scan_run_id"),
                    plan.get("priority"),
                    plan.get("service_name"),
                    plan.get("action_type"),
                    plan.get("title", ""),
                    plan.get("description", ""),
                    json.dumps(plan.get("steps", [])),
                    plan.get("estimated_effort"),
                ),
            )


# ─── Frontend API queries ──────────────────────────────────────────────────────

def get_dashboard_summary() -> dict:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, scan_type, started_at, completed_at, status,
                       images_scanned, findings_count, critical_count, high_count
                FROM security.scan_runs WHERE status = 'completed'
                ORDER BY started_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            latest = dict(row) if row else {}
            for k in ("started_at", "completed_at"):
                if latest.get(k):
                    latest[k] = latest[k].isoformat()

            cur.execute("""
                SELECT severity, COUNT(*) AS n FROM security.findings
                WHERE status = 'open' GROUP BY severity
            """)
            severity_counts = {r["severity"]: r["n"] for r in cur.fetchall()}

            cur.execute("""
                SELECT priority, status, COUNT(*) AS n FROM security.action_plans
                WHERE status IN ('open', 'in_progress') GROUP BY priority, status
            """)
            plan_rows = cur.fetchall()
            plan_counts: dict[str, int] = {}
            for r in plan_rows:
                plan_counts[r["priority"]] = plan_counts.get(r["priority"], 0) + r["n"]
            plan_counts["in_progress"] = sum(r["n"] for r in plan_rows if r["status"] == "in_progress")

            cur.execute("""
                SELECT COUNT(*) AS n FROM security.version_drift
                WHERE checked_at = (SELECT MAX(checked_at) FROM security.version_drift)
            """)
            drift_row = cur.fetchone()
            drift_count = drift_row["n"] if drift_row else 0

            cur.execute("""
                SELECT id, scan_type, started_at, completed_at, status,
                       images_scanned, findings_count, critical_count, high_count
                FROM security.scan_runs ORDER BY started_at DESC LIMIT 10
            """)
            recent = []
            for r in cur.fetchall():
                d = dict(r)
                for k in ("started_at", "completed_at"):
                    if d.get(k):
                        d[k] = d[k].isoformat()
                recent.append(d)

            return {
                "latest_scan":        latest,
                "open_findings":      severity_counts,
                "action_plans":       plan_counts,
                "version_drift_count": drift_count,
                "recent_scans":       recent,
            }


def get_findings_paged(
    severity: str | None = None,
    image: str | None = None,
    status: str = "open",
    cve_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    conditions: list[str] = []
    params: list = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if severity:
        conditions.append("severity = %s")
        params.append(severity)
    if image:
        conditions.append("image_name = %s")
        params.append(image)
    if cve_id:
        conditions.append("cve_id ILIKE %s")
        params.append(f"%{cve_id}%")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM security.findings {where}", params)
            total = cur.fetchone()["n"]
            cur.execute(
                f"""
                SELECT id, image_name, image_tag, cve_id, severity, cvss_score,
                       package_name, package_version, fixed_version, title, status,
                       first_seen_at, last_seen_at
                FROM security.findings {where}
                ORDER BY
                    CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                                  WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4 ELSE 5 END,
                    cvss_score DESC NULLS LAST
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            findings = []
            for r in cur.fetchall():
                d = dict(r)
                for k in ("first_seen_at", "last_seen_at"):
                    if d.get(k):
                        d[k] = d[k].isoformat()
                findings.append(d)
            return findings, total


def auto_resolve_fixed_findings(scan_run_id: int, scanned_images: list[str]) -> int:
    """
    After a completed scan, mark any previously-open finding for a scanned image
    as 'resolved' if it wasn't touched by the current scan run (i.e., the CVE
    is no longer present in that image).  Skips 'accepted' and 'false_positive'.
    Returns the count of rows resolved.
    """
    if not scanned_images:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE security.findings
                SET status = 'resolved', last_seen_at = NOW()
                WHERE image_name = ANY(%s)
                  AND scan_run_id != %s
                  AND status IN ('open', 'in_progress')
                """,
                (scanned_images, scan_run_id),
            )
            return cur.rowcount


def update_finding_status(finding_id: int, status: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE security.findings SET status = %s WHERE id = %s",
                (status, finding_id),
            )


def get_action_plans(priority: str | None = None, status: str | None = None) -> list[dict]:
    conditions: list[str] = []
    params: list = []
    if priority:
        conditions.append("ap.priority = %s")
        params.append(priority)
    if status:
        conditions.append("ap.status = %s")
        params.append(status)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT ap.id, ap.scan_run_id, ap.priority, ap.service_name,
                       ap.action_type, ap.title, ap.description, ap.steps,
                       ap.estimated_effort, ap.status, ap.generated_at, ap.resolved_at,
                       ap.finding_ids, sr.started_at AS scan_date
                FROM security.action_plans ap
                JOIN security.scan_runs sr ON ap.scan_run_id = sr.id
                {where}
                ORDER BY
                    CASE ap.priority WHEN 'immediate' THEN 1 WHEN 'this_week' THEN 2 ELSE 3 END,
                    sr.started_at DESC
                """,
                params,
            )
            plans = []
            for r in cur.fetchall():
                d = dict(r)
                for k in ("generated_at", "resolved_at", "scan_date"):
                    if d.get(k):
                        d[k] = d[k].isoformat()
                if isinstance(d.get("steps"), str):
                    d["steps"] = json.loads(d["steps"])
                plans.append(d)
            return plans


def update_action_plan_status(plan_id: int, status: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE security.action_plans
                SET status = %s,
                    resolved_at = CASE WHEN %s = 'completed' THEN NOW() ELSE resolved_at END
                WHERE id = %s
                """,
                (status, status, plan_id),
            )


def get_version_drift_latest() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT vd.id, vd.service_name, vd.image_name, vd.running_tag,
                       vd.latest_tag, vd.versions_behind, vd.release_notes_url, vd.checked_at
                FROM security.version_drift vd
                WHERE vd.checked_at = (SELECT MAX(checked_at) FROM security.version_drift)
                ORDER BY vd.versions_behind DESC NULLS LAST, vd.service_name
            """)
            results = []
            for r in cur.fetchall():
                d = dict(r)
                if d.get("checked_at"):
                    d["checked_at"] = d["checked_at"].isoformat()
                results.append(d)
            return results


def get_distinct_images() -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT image_name FROM security.findings
                WHERE status = 'open' ORDER BY image_name
            """)
            return [r[0] for r in cur.fetchall()]
