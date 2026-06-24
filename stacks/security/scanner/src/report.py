"""
Generate PDF + HTML email content for security scan reports.
Uses fpdf2 (pure-Python, no system deps) for PDF generation.
"""
from __future__ import annotations

from datetime import datetime, timezone


_PRIORITY_LABEL = {"immediate": "IMMEDIATE", "this_week": "THIS WEEK", "backlog": "BACKLOG"}
_PRIORITY_COLOR = {
    "immediate": (239, 68, 68),
    "this_week": (249, 115, 22),
    "backlog": (59, 130, 246),
}
_SEVERITY_COLOR = {
    "CRITICAL": (239, 68, 68),
    "HIGH": (249, 115, 22),
    "MEDIUM": (234, 179, 8),
    "LOW": (34, 197, 94),
    "UNKNOWN": (100, 116, 139),
}


def build_pdf_report(
    scan_run_id: int,
    severity_counts: dict,
    action_plans: list[dict],
    top_concerns: list[str],
    scan_date: str = "",
) -> bytes:
    try:
        from fpdf import FPDF
    except ImportError:
        return b""

    accent      = (37, 99, 235)
    light_blue  = (96, 165, 250)
    card_bg     = (13, 26, 53)
    border_rgb  = (30, 51, 88)
    body_text   = (226, 232, 240)
    muted_rgb   = (100, 116, 139)
    dark        = (20, 20, 30)
    mid_gray    = (60, 60, 80)
    white       = (255, 255, 255)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.set_margins(15, 15, 15)
    pdf.add_page()

    # ── Header bar ────────────────────────────────────────────────────────────
    pdf.set_fill_color(*card_bg)
    pdf.rect(0, 0, 210, 38, "F")

    pdf.set_xy(15, 8)
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(*light_blue)
    pdf.cell(0, 9, "Peak Precision Data", ln=True)

    pdf.set_xy(15, 21)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*body_text)
    label = f"Security Vulnerability Report  |  Scan #{scan_run_id}"
    if scan_date:
        label += f"  |  {scan_date}"
    pdf.cell(0, 6, label, ln=True)

    pdf.ln(14)

    # ── Executive Summary ─────────────────────────────────────────────────────
    def section_header(title: str) -> None:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(*accent)
        pdf.cell(0, 8, title, ln=True)
        pdf.set_draw_color(*accent)
        pdf.set_line_width(0.4)
        pdf.line(15, pdf.get_y(), 195, pdf.get_y())
        pdf.ln(4)

    section_header("Executive Summary")

    # Severity count boxes
    col_w = 43
    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        r, g, b = _SEVERITY_COLOR[sev]
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*muted_rgb)
        pdf.cell(col_w, 5, sev, align="C")
    pdf.ln()

    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        r, g, b = _SEVERITY_COLOR[sev]
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_text_color(r, g, b)
        pdf.cell(col_w, 12, str(severity_counts.get(sev, 0)), align="C")
    pdf.ln(16)

    # Priority plan counts
    priority_counts: dict[str, int] = {}
    for plan in action_plans:
        k = plan.get("priority", "backlog")
        priority_counts[k] = priority_counts.get(k, 0) + 1

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(*muted_rgb)
    pdf.cell(0, 5, "ACTION PLANS BY PRIORITY", ln=True)
    pdf.ln(1)

    for prio in ("immediate", "this_week", "backlog"):
        r, g, b = _PRIORITY_COLOR[prio]
        pdf.set_fill_color(r, g, b)
        pdf.set_text_color(*white)
        pdf.set_font("Helvetica", "B", 9)
        cnt = priority_counts.get(prio, 0)
        pdf.cell(58, 7, f"  {_PRIORITY_LABEL[prio]}: {cnt}", fill=True)
        pdf.cell(3, 7, "")
    pdf.ln(12)

    # ── Top Concerns ──────────────────────────────────────────────────────────
    if top_concerns:
        section_header("Top Concerns")
        for concern in top_concerns[:8]:
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*mid_gray)
            pdf.cell(6, 5, "\x95")  # bullet char in Latin-1
            pdf.multi_cell(0, 5, concern[:200])
        pdf.ln(4)

    # ── Action Plans ──────────────────────────────────────────────────────────
    for prio_label in ("immediate", "this_week", "backlog"):
        plans_here = [p for p in action_plans if p.get("priority") == prio_label]
        if not plans_here:
            continue

        section_header(f"{_PRIORITY_LABEL[prio_label]} Action Plans")

        for plan in plans_here:
            # Title
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*dark)
            pdf.multi_cell(0, 6, plan.get("title", "Untitled"))

            # Meta line
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(*muted_rgb)
            svc    = plan.get("service_name", "")
            effort = plan.get("estimated_effort", "")
            atype  = plan.get("action_type", "")
            pdf.cell(0, 5, f"Service: {svc}  |  Type: {atype}  |  Effort: {effort}", ln=True)
            pdf.ln(1)

            # Description
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*mid_gray)
            pdf.multi_cell(0, 5, plan.get("description", ""))
            pdf.ln(1)

            # Steps
            steps = plan.get("steps", [])
            if steps:
                pdf.set_font("Helvetica", "B", 8)
                pdf.set_text_color(*dark)
                pdf.cell(0, 5, "Remediation Steps:", ln=True)
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(*mid_gray)
                for i, step in enumerate(steps, 1):
                    pdf.multi_cell(0, 5, f"  {i}. {step[:300]}")

            pdf.set_draw_color(*border_rgb)
            pdf.set_line_width(0.2)
            pdf.line(15, pdf.get_y() + 3, 195, pdf.get_y() + 3)
            pdf.ln(7)

    # ── Footer ────────────────────────────────────────────────────────────────
    pdf.set_y(-15)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(*muted_rgb)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pdf.cell(0, 6, f"Generated {generated}  |  Peak Precision Data Security Scanner", align="C")

    return bytes(pdf.output())


def build_email_html(
    scan_run_id: int,
    severity_counts: dict,
    action_plans: list[dict],
    top_concerns: list[str],
    scan_date: str = "",
) -> str:
    priority_counts: dict[str, int] = {}
    for plan in action_plans:
        k = plan.get("priority", "backlog")
        priority_counts[k] = priority_counts.get(k, 0) + 1

    immediate_plans = [p for p in action_plans if p.get("priority") == "immediate"]

    def sev_badge(sev: str, count: int) -> str:
        colors = {
            "CRITICAL": ("#fee2e2", "#b91c1c"),
            "HIGH":     ("#ffedd5", "#c2410c"),
            "MEDIUM":   ("#fef9c3", "#a16207"),
            "LOW":      ("#dcfce7", "#15803d"),
        }
        bg, fg = colors.get(sev, ("#f1f5f9", "#475569"))
        return (
            f'<td style="padding:12px 20px; text-align:center;">'
            f'<div style="background:{bg};border-radius:8px;padding:8px 16px;">'
            f'<div style="color:{fg};font-size:24px;font-weight:700;">{count}</div>'
            f'<div style="color:{fg};font-size:11px;font-weight:600;margin-top:2px;">{sev}</div>'
            f'</div></td>'
        )

    severity_row = "".join(
        sev_badge(s, severity_counts.get(s, 0))
        for s in ("CRITICAL", "HIGH", "MEDIUM", "LOW")
    )

    immediate_html = ""
    for plan in immediate_plans[:5]:
        steps_html = "".join(
            f'<li style="margin:4px 0;color:#374151;">{step}</li>'
            for step in (plan.get("steps") or [])[:5]
        )
        immediate_html += f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;margin:12px 0;">
          <div style="font-weight:700;font-size:14px;color:#111827;">{plan.get("title","")}</div>
          <div style="font-size:12px;color:#6b7280;margin:4px 0;">
            {plan.get("service_name","")} &nbsp;·&nbsp; {plan.get("action_type","")} &nbsp;·&nbsp; {plan.get("estimated_effort","")}
          </div>
          <div style="font-size:13px;color:#374151;margin:8px 0;">{plan.get("description","")}</div>
          {"<ol style='margin:8px 0 0 16px;padding:0;font-size:12px;'>" + steps_html + "</ol>" if steps_html else ""}
        </div>"""

    concerns_html = "".join(
        f'<li style="margin:4px 0;color:#374151;">{c}</li>'
        for c in (top_concerns or [])[:6]
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
<div style="max-width:640px;margin:24px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 6px rgba(0,0,0,0.07);">

  <!-- Header -->
  <div style="background:#0d1a35;padding:28px 32px;">
    <div style="color:#60a5fa;font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;">Peak Precision Data</div>
    <div style="color:#e2e8f0;font-size:20px;font-weight:700;margin-top:4px;">Security Report</div>
    <div style="color:#94a3b8;font-size:12px;margin-top:4px;">Scan #{scan_run_id}{' — ' + scan_date if scan_date else ''}</div>
  </div>

  <!-- Severity counts -->
  <div style="padding:24px 32px 8px;">
    <div style="font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:12px;">Open Vulnerabilities</div>
    <table style="width:100%;border-collapse:collapse;">{severity_row}</table>
  </div>

  <!-- Priority summary -->
  <div style="padding:16px 32px 8px;">
    <div style="font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:10px;">Action Plans</div>
    <table style="border-collapse:collapse;">
      <tr>
        <td style="padding:6px 14px;background:#fee2e2;border-radius:6px;color:#b91c1c;font-weight:700;font-size:13px;margin-right:8px;">
          Immediate: {priority_counts.get("immediate", 0)}
        </td>
        <td style="width:8px;"></td>
        <td style="padding:6px 14px;background:#ffedd5;border-radius:6px;color:#c2410c;font-weight:700;font-size:13px;">
          This Week: {priority_counts.get("this_week", 0)}
        </td>
        <td style="width:8px;"></td>
        <td style="padding:6px 14px;background:#dbeafe;border-radius:6px;color:#1d4ed8;font-weight:700;font-size:13px;">
          Backlog: {priority_counts.get("backlog", 0)}
        </td>
      </tr>
    </table>
  </div>

  {"<!-- Top Concerns --><div style='padding:16px 32px 8px;'><div style='font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;'>Top Concerns</div><ul style='margin:0;padding-left:20px;'>" + concerns_html + "</ul></div>" if concerns_html else ""}

  <!-- Immediate plans -->
  {"<div style='padding:16px 32px 8px;'><div style='font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px;'>Immediate Action Required</div>" + immediate_html + "</div>" if immediate_html else ""}

  <!-- Footer -->
  <div style="padding:20px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;">
    <div style="font-size:11px;color:#9ca3af;">
      Full report attached as PDF. View and manage findings at
      <a href="https://security.peakprecisiondata.com" style="color:#2563eb;">security.peakprecisiondata.com</a>
    </div>
  </div>
</div>
</body>
</html>"""
