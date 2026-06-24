"""
LangGraph security analysis agent.

Graph: triage_node → plan_node → store_node → notify_node → END

- triage_node  : Claude receives a compact per-image severity summary and returns
                 an image → priority mapping ("immediate"/"this_week"/"backlog").
- plan_node    : Claude generates step-by-step remediation per (priority, image) group,
                 pulling the top CVEs for that image from the full findings list.
- store_node   : persists action plans to Postgres.
- notify_node  : sends a Resend email with PDF attachment summarising the scan results.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Annotated, TypedDict

import anthropic
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from .db import store_action_plan
from .report import build_pdf_report, build_email_html

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

_STACK_CONTEXT = """\
You are a security analyst reviewing vulnerability scan results for a self-hosted \
data analytics platform called Peak Precision Data (PPD).

Stack overview (internet-facing services listed first):
- nginx — internet-facing via Cloudflare tunnel (ports 80/443), highest exposure
- superset-with-playwright — BI dashboards, accessible externally through nginx
- mathenymanor-agent_api — Claude AI agents, accessible externally through nginx
- mathenymanor-health-api — Apple Health ingestion, internal + nginx
- pgvector/pgvector — primary data store (analytics, ESPN, Airflow, Dagster, MLflow, Superset)
- redis — Celery broker/cache, internal only
- minio/minio — object storage with health + mail data, internal only
- metabase/metabase — BI dashboards, internal only
- ghcr.io/mlflow/mlflow — ML tracking, internal only
- qdrant/qdrant — vector DB, internal only
- custom-airflow-ml-dbt — orchestration, internal only
- ppd-dagster — orchestration, internal only
- bitnami/kafka — streaming, internal only
- jupyter-gpu — notebooks, internal only
- linuxserver/plex — media server, internal only
- homebridge/homebridge — HomeKit bridge, internal only

Prioritization rules:
1. immediate  — CVSS ≥ 9.0, OR service is internet-facing, OR active in-the-wild exploitation
2. this_week  — CVSS 7.0–8.9, OR affects postgres/minio (data-at-rest), OR privilege escalation
3. backlog    — CVSS < 7.0 on internal-only service

Return only valid JSON with no markdown code fences.\
"""


def _parse_json(text: str, context: str) -> dict | None:
    """
    Parse JSON from a Claude response, stripping markdown fences if present.
    Logs the raw response on failure so we can diagnose prompt issues.
    """
    # Strip ```json ... ``` or ``` ... ``` fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned.strip(), flags=re.MULTILINE)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Unparseable JSON from %s. Raw response:\n%s", context, text[:500])
        return None


def _build_image_summary(findings: list[dict]) -> dict:
    """
    Collapse findings into a compact per-image summary for the triage prompt.
    Sending counts + top CVEs is far more token-efficient than sending raw finding objects.
    """
    summary: dict[str, dict] = {}
    for f in findings:
        img = f.get("image_name", "unknown")
        if img not in summary:
            summary[img] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "top_cves": []}
        sev = f.get("severity", "UNKNOWN")
        summary[img][sev] = summary[img].get(sev, 0) + 1
        if sev in ("CRITICAL", "HIGH") and len(summary[img]["top_cves"]) < 5:
            summary[img]["top_cves"].append({
                "cve":     f.get("cve_id"),
                "cvss":    f.get("cvss_score"),
                "package": f.get("package_name"),
                "fixed":   bool(f.get("fixed_version")),
                "title":   (f.get("title") or "")[:80],
            })
    return summary


# ─── State ────────────────────────────────────────────────────────────────────

class SecurityState(TypedDict):
    scan_run_id: int
    findings: list[dict]
    image_priorities: dict[str, str]   # image_name → "immediate"|"this_week"|"backlog"
    top_concerns: list[str]
    action_plans: list[dict]
    messages: Annotated[list, add_messages]


# ─── Nodes ────────────────────────────────────────────────────────────────────

def triage_node(state: SecurityState) -> dict:
    """
    Send a compact per-image severity summary to Claude.
    Claude returns a priority assignment per image — no finding objects in the response,
    keeping output small and reliable.
    """
    summary = _build_image_summary(state["findings"])

    prompt = f"""Assign a security priority to each Docker image based on its vulnerability counts, \
top CVEs, and its role in the stack (internet-facing vs. internal).

Per-image summary:
{json.dumps(summary, indent=2, default=str)}

Return JSON only, no markdown:
{{
  "image_priorities": {{
    "<image_name>": "immediate|this_week|backlog"
  }},
  "top_concerns": ["<one-line concern>", ...]
}}"""

    response = _client.messages.create(
        model="claude-opus-4-7",
        max_tokens=1024,
        system=_STACK_CONTEXT,
        messages=[{"role": "user", "content": prompt}],
    )

    parsed = _parse_json(response.content[0].text, "triage_node")
    if not parsed:
        return {"image_priorities": {}, "top_concerns": []}

    priorities = parsed.get("image_priorities", {})
    concerns   = parsed.get("top_concerns", [])
    logger.info(
        "Triage complete — immediate=%d this_week=%d backlog=%d",
        sum(1 for v in priorities.values() if v == "immediate"),
        sum(1 for v in priorities.values() if v == "this_week"),
        sum(1 for v in priorities.values() if v == "backlog"),
    )
    return {"image_priorities": priorities, "top_concerns": concerns}


def plan_node(state: SecurityState) -> dict:
    """
    For each (priority, image) pair, pull that image's worst findings from the
    full findings list and ask Claude for a concrete remediation plan.
    """
    priorities = state["image_priorities"]
    if not priorities:
        logger.warning("No image priorities from triage — skipping plan generation")
        return {"action_plans": []}

    # Pre-index findings by image for fast lookup
    by_image: dict[str, list[dict]] = {}
    for f in state["findings"]:
        by_image.setdefault(f.get("image_name", "unknown"), []).append(f)

    action_plans: list[dict] = []

    for priority_label in ("immediate", "this_week", "backlog"):
        images_at_priority = [img for img, p in priorities.items() if p == priority_label]
        for image in images_at_priority:
            image_findings = by_image.get(image, [])
            if not image_findings:
                continue

            # Send top 5 worst CVEs (by CVSS) to keep the plan prompt tight
            top = sorted(
                [f for f in image_findings if f.get("cvss_score")],
                key=lambda f: f["cvss_score"],
                reverse=True,
            )[:5]
            top_compact = [
                {
                    "cve":     f.get("cve_id"),
                    "cvss":    f.get("cvss_score"),
                    "package": f.get("package_name"),
                    "fixed_in": f.get("fixed_version"),
                    "title":   (f.get("title") or "")[:100],
                }
                for f in top
            ]

            prompt = f"""Generate a remediation plan for {priority_label.upper()} priority findings \
in the Docker image "{image}".

Top CVEs by CVSS score:
{json.dumps(top_compact, indent=2, default=str)}

Total findings for this image: \
{sum(1 for f in image_findings if f.get('severity') == 'CRITICAL')} critical, \
{sum(1 for f in image_findings if f.get('severity') == 'HIGH')} high, \
{sum(1 for f in image_findings if f.get('severity') == 'MEDIUM')} medium

Return JSON only, no markdown:
{{
  "title": "<short imperative title, e.g. Patch critical OpenSSL CVEs in nginx>",
  "action_type": "patch|upgrade|config_change|monitor",
  "description": "<2-3 sentences: what the risk is and what action to take>",
  "steps": ["<step 1>", "<step 2>", "..."],
  "estimated_effort": "30min|2hrs|1day|1week"
}}"""

            response = _client.messages.create(
                model="claude-opus-4-7",
                max_tokens=1024,
                system=_STACK_CONTEXT,
                messages=[{"role": "user", "content": prompt}],
            )

            parsed = _parse_json(response.content[0].text, f"plan_node/{priority_label}/{image}")
            if parsed:
                parsed.update({
                    "priority":     priority_label,
                    "service_name": image,
                    "finding_ids":  [f.get("id") for f in image_findings if f.get("id")],
                    "scan_run_id":  state["scan_run_id"],
                })
                action_plans.append(parsed)

    logger.info("Generated %d action plans", len(action_plans))
    return {"action_plans": action_plans}


def store_node(state: SecurityState) -> dict:
    """Persist all action plans to Postgres."""
    for plan in state["action_plans"]:
        store_action_plan(plan)
    logger.info("Stored %d action plans for scan_run_id=%d", len(state["action_plans"]), state["scan_run_id"])
    return {}


def notify_node(state: SecurityState) -> dict:
    """Send Resend email with PDF report. Failures are logged but never raise."""
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        logger.info("RESEND_API_KEY not set — skipping scan report email")
        return {}

    try:
        import base64
        import resend

        scan_run_id = state["scan_run_id"]
        findings    = state["findings"]
        plans       = state["action_plans"]
        concerns    = state["top_concerns"]

        from collections import Counter
        severity_counts = dict(Counter(f.get("severity", "UNKNOWN") for f in findings))

        from datetime import datetime, timezone
        scan_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        pdf_bytes = build_pdf_report(scan_run_id, severity_counts, plans, concerns, scan_date)
        html_body = build_email_html(scan_run_id, severity_counts, plans, concerns, scan_date)

        critical = severity_counts.get("CRITICAL", 0)
        high     = severity_counts.get("HIGH", 0)
        immediate_count = sum(1 for p in plans if p.get("priority") == "immediate")
        subject = (
            f"PPD Security Report — {scan_date} — "
            f"{critical}C / {high}H open  |  {immediate_count} immediate action plans"
        )

        to_addr   = os.environ.get("SECURITY_REPORT_EMAIL", "wmatheny07@gmail.com")
        from_addr = os.environ.get("SECURITY_REPORT_FROM",  "security@peakprecisiondata.com")

        params: dict = {
            "from":    from_addr,
            "to":      [to_addr],
            "subject": subject,
            "html":    html_body,
        }
        if pdf_bytes:
            params["attachments"] = [{
                "filename": f"ppd-security-report-{scan_date}.pdf",
                "content":  list(base64.b64encode(pdf_bytes)),
            }]

        resend.api_key = api_key
        resend.Emails.send(params)
        logger.info("Security report email sent to %s (scan_run_id=%d)", to_addr, scan_run_id)

    except Exception as exc:
        logger.warning("Failed to send security report email: %s", exc)

    return {}


# ─── Graph ────────────────────────────────────────────────────────────────────

def _build_graph():
    g = StateGraph(SecurityState)
    g.add_node("triage", triage_node)
    g.add_node("plan",   plan_node)
    g.add_node("store",  store_node)
    g.add_node("notify", notify_node)
    g.add_edge(START,    "triage")
    g.add_edge("triage", "plan")
    g.add_edge("plan",   "store")
    g.add_edge("store",  "notify")
    g.add_edge("notify", END)
    return g.compile()


_graph = None


async def analyze_findings(scan_run_id: int, findings: list[dict]) -> None:
    """Entry point called by scanner.py after all findings have been upserted."""
    global _graph
    if _graph is None:
        _graph = _build_graph()

    logger.info(
        "Running LangGraph security analysis for scan_run_id=%d (%d findings across %d images)",
        scan_run_id,
        len(findings),
        len({f.get("image_name") for f in findings}),
    )
    await _graph.ainvoke({
        "scan_run_id":      scan_run_id,
        "findings":         findings,
        "image_priorities": {},
        "top_concerns":     [],
        "action_plans":     [],
        "messages":         [],
    })
