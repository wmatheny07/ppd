from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

_NVD_API_KEY = os.environ.get("NVD_API_KEY", "")
_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "ppd-security-scanner/1.0"


def enrich_cve(cve_id: str) -> dict:
    """
    Fetch additional CVE metadata. Tries OSV first (faster, no key needed),
    falls back to NVD. Returns an empty dict on failure so callers can proceed.
    """
    for fetch_fn in (_fetch_osv, _fetch_nvd):
        try:
            result = fetch_fn(cve_id)
            if result:
                return result
        except Exception as exc:
            logger.debug("CVE fetch failed for %s via %s: %s", cve_id, fetch_fn.__name__, exc)
    return {}


def _fetch_osv(cve_id: str) -> dict:
    resp = _SESSION.get(
        f"https://api.osv.dev/v1/vulns/{cve_id}",
        timeout=10,
    )
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    data = resp.json()
    return {
        "summary": data.get("summary", ""),
        "details": (data.get("details") or "")[:1000],
        "published": data.get("published"),
        "modified": data.get("modified"),
        "severity": _osv_cvss_severity(data),
        "source": "osv",
    }


def _fetch_nvd(cve_id: str) -> dict:
    headers = {"apiKey": _NVD_API_KEY} if _NVD_API_KEY else {}
    resp = _SESSION.get(
        f"https://services.nvd.nist.gov/rest/json/cves/2.0?cveId={cve_id}",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    items = resp.json().get("vulnerabilities", [])
    if not items:
        return {}
    cve = items[0]["cve"]
    desc = next(
        (d["value"] for d in cve.get("descriptions", []) if d["lang"] == "en"),
        "",
    )
    return {
        "summary": desc[:500],
        "published": cve.get("published"),
        "modified": cve.get("lastModified"),
        "source": "nvd",
    }


def _osv_cvss_severity(data: dict) -> str | None:
    for sev in data.get("severity", []):
        if sev.get("type") == "CVSS_V3":
            try:
                # CVSS vector string ends with the numeric score after the last ":"
                score = float(sev["score"].split(":")[-1])
            except (KeyError, ValueError):
                continue
            if score >= 9.0:
                return "CRITICAL"
            if score >= 7.0:
                return "HIGH"
            if score >= 4.0:
                return "MEDIUM"
            return "LOW"
    return None
