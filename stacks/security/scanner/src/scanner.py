from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import uuid

import boto3
from botocore.client import Config

from .db import (
    auto_resolve_fixed_findings,
    begin_scan_run,
    finish_scan_run,
    get_findings_for_run,
    store_sbom_record,
    store_version_drift,
    upsert_finding,
)
from .inventory import get_running_images
from .version_check import check_image_version
from .agent import analyze_findings

logger = logging.getLogger(__name__)

# Packages whose findings are dropped before storage. linux-libc-dev is kernel
# headers only: containers run on the host kernel, so its CVEs aren't reachable
# inside an image and they made up ~23% of all HIGH findings.
IGNORED_PACKAGES = {
    p.strip() for p in os.environ.get("TRIVY_IGNORED_PACKAGES", "linux-libc-dev").split(",") if p.strip()
}


# ─── MinIO helper ─────────────────────────────────────────────────────────────

def _minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _store_sbom_to_minio(sbom_json: dict, image_ref: str, scan_run_id: int) -> str | None:
    bucket = os.environ.get("MINIO_SECURITY_BUCKET", "security-scans")
    safe_ref = image_ref.replace("/", "_").replace(":", "_")
    key = f"sboms/{scan_run_id}/{safe_ref}.json"
    try:
        client = _minio_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=io.BytesIO(json.dumps(sbom_json).encode()),
            ContentType="application/json",
        )
        return f"{bucket}/{key}"
    except Exception as exc:
        logger.warning("Failed to store SBOM to MinIO for %s: %s", image_ref, exc)
        return None


def _ensure_minio_bucket() -> None:
    bucket = os.environ.get("MINIO_SECURITY_BUCKET", "security-scans")
    try:
        client = _minio_client()
        existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
            logger.info("Created MinIO bucket: %s", bucket)
    except Exception as exc:
        logger.warning("Could not ensure MinIO bucket: %s", exc)


# ─── Trivy ────────────────────────────────────────────────────────────────────

def trivy_scan(image_ref: str) -> list[dict] | None:
    """
    Run Trivy against an image and return a flat list of vulnerability dicts,
    or None if the scan itself failed (so callers don't mistake it for "clean").
    Exit code 1 from Trivy means vulnerabilities were found — still valid output.
    Duplicate (cve, package) pairs — Trivy reports one per file path — are collapsed.
    """
    ignore_file = "/app/trivyignore"
    cmd = ["trivy", "image", "--format", "json", "--quiet"]
    if os.path.exists(ignore_file):
        cmd += ["--ignorefile", ignore_file]
    cmd.append(image_ref)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):
        logger.warning("Trivy unexpected exit %d for %s: %s", result.returncode, image_ref, result.stderr[:300])
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("Trivy returned non-JSON for %s", image_ref)
        return None

    findings: list[dict] = []
    seen: set[tuple] = set()
    for result_block in data.get("Results") or []:
        for vuln in result_block.get("Vulnerabilities") or []:
            key = (vuln.get("VulnerabilityID"), vuln.get("PkgName"))
            if key in seen or vuln.get("PkgName") in IGNORED_PACKAGES:
                continue
            seen.add(key)
            findings.append(
                {
                    "cve_id":          vuln.get("VulnerabilityID"),
                    "severity":        vuln.get("Severity", "UNKNOWN"),
                    "cvss_score":      _extract_cvss(vuln),
                    "package_name":    vuln.get("PkgName"),
                    "package_version": vuln.get("InstalledVersion"),
                    "fixed_version":   vuln.get("FixedVersion"),
                    "title":           vuln.get("Title", ""),
                    "description":     (vuln.get("Description") or "")[:2000],
                    "published_at":    vuln.get("PublishedDate"),
                    "references":      vuln.get("References", []),
                }
            )
    return findings


def _extract_cvss(vuln: dict) -> float | None:
    for source in ("nvd", "redhat", "ghsa"):
        score = vuln.get("CVSS", {}).get(source, {}).get("V3Score")
        if score is not None:
            try:
                return float(score)
            except (TypeError, ValueError):
                pass
    return None


# ─── Syft ─────────────────────────────────────────────────────────────────────

def syft_sbom(image_ref: str) -> dict | None:
    """Generate an SBOM for an image using Syft. Returns parsed JSON or None."""
    result = subprocess.run(
        ["syft", image_ref, "-o", "syft-json", "--quiet"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning("Syft failed for %s: %s", image_ref, result.stderr[:300])
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


# ─── Orchestration ────────────────────────────────────────────────────────────

async def run_full_scan(notify: bool = True) -> None:
    """
    Full scan workflow:
      1. Discover running images via Docker socket
      2. Trivy CVE scan per image
      3. Syft SBOM per image → MinIO
      4. LangGraph + Claude analysis → action plans
      5. Persist all findings + plans to Postgres
      6. Email the report (unless notify=False)
    """
    run_id = str(uuid.uuid4())
    logger.info("Starting full scan run %s", run_id)
    _ensure_minio_bucket()

    scan_run_id = begin_scan_run(run_id, "full")
    all_findings: list[dict] = []
    scanned_ok: list[str] = []

    try:
        images = get_running_images()

        for img in images:
            image_ref = img["image_ref"]
            logger.info("Scanning %s", image_ref)

            # CVE scan
            findings = trivy_scan(image_ref)
            if findings is None:
                logger.warning("Skipping %s — Trivy scan failed; its findings are left untouched", image_ref)
                continue
            scanned_ok.append(img["image_name"])
            for f in findings:
                f.update(
                    {
                        "image_name": img["image_name"],
                        "image_tag":  img["image_tag"],
                        "scan_run_id": scan_run_id,
                    }
                )
                upsert_finding(f)
                all_findings.append(f)

            # SBOM
            sbom = syft_sbom(image_ref)
            if sbom:
                minio_path = _store_sbom_to_minio(sbom, image_ref, scan_run_id)
                store_sbom_record(scan_run_id, img, sbom, minio_path)

        # Auto-resolve findings that disappeared in this scan, plus findings for
        # images that are no longer running at all. Images whose scan failed are kept.
        running_names = {img["image_name"] for img in images}
        failed_names = list(running_names - set(scanned_ok))
        resolved = auto_resolve_fixed_findings(scan_run_id, list(set(scanned_ok)), failed_names)
        if resolved:
            logger.info("Auto-resolved %d findings no longer present in scanned images", resolved)

        # AI analysis + action plans
        if all_findings:
            await analyze_findings(scan_run_id, all_findings, notify=notify)

        critical = sum(1 for f in all_findings if f.get("severity") == "CRITICAL")
        high     = sum(1 for f in all_findings if f.get("severity") == "HIGH")
        finish_scan_run(scan_run_id, "completed", len(images), len(all_findings), critical, high)
        logger.info(
            "Full scan complete — images=%d findings=%d critical=%d high=%d",
            len(images), len(all_findings), critical, high,
        )

    except Exception:
        logger.exception("Full scan run %s failed", run_id)
        finish_scan_run(scan_run_id, "failed")
        raise


async def run_version_check() -> None:
    """Check running image versions against latest upstream releases."""
    run_id = str(uuid.uuid4())
    logger.info("Starting version check run %s", run_id)

    scan_run_id = begin_scan_run(run_id, "version_check")

    try:
        images = get_running_images()

        for img in images:
            drift = check_image_version(img["image_name"], img["image_tag"])
            if drift:
                logger.info(
                    "Version drift — %s: running=%s latest=%s",
                    img["image_name"], img["image_tag"], drift.get("latest_tag"),
                )
                store_version_drift(scan_run_id, img["container_name"], img, drift)

        finish_scan_run(scan_run_id, "completed", len(images), 0, 0, 0)
        logger.info("Version check complete — %d images checked", len(images))

    except Exception:
        logger.exception("Version check run %s failed", run_id)
        finish_scan_run(scan_run_id, "failed")
        raise
