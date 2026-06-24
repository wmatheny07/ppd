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

def trivy_scan(image_ref: str) -> list[dict]:
    """
    Run Trivy against an image and return a flat list of vulnerability dicts.
    Exit code 1 from Trivy means vulnerabilities were found — still valid output.
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
        return []

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("Trivy returned non-JSON for %s", image_ref)
        return []

    findings: list[dict] = []
    for result_block in data.get("Results") or []:
        for vuln in result_block.get("Vulnerabilities") or []:
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

async def run_full_scan() -> None:
    """
    Full scan workflow:
      1. Discover running images via Docker socket
      2. Trivy CVE scan per image
      3. Syft SBOM per image → MinIO
      4. LangGraph + Claude analysis → action plans
      5. Persist all findings + plans to Postgres
    """
    run_id = str(uuid.uuid4())
    logger.info("Starting full scan run %s", run_id)
    _ensure_minio_bucket()

    scan_run_id = begin_scan_run(run_id, "full")
    all_findings: list[dict] = []

    try:
        images = get_running_images()

        for img in images:
            image_ref = img["image_ref"]
            logger.info("Scanning %s", image_ref)

            # CVE scan
            findings = trivy_scan(image_ref)
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

        # Auto-resolve findings that disappeared in this scan
        scanned_image_names = list({img["image_name"] for img in images})
        resolved = auto_resolve_fixed_findings(scan_run_id, scanned_image_names)
        if resolved:
            logger.info("Auto-resolved %d findings no longer present in scanned images", resolved)

        # AI analysis + action plans
        if all_findings:
            await analyze_findings(scan_run_id, all_findings)

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
