"""
Generate PDF + HTML email content for security scan reports.
Uses fpdf2 (pure-Python, no system deps) for PDF generation.
"""
from __future__ import annotations

import html
from datetime import datetime, timezone


_PRIORITY_LABEL = {"immediate": "IMMEDIATE", "this_week": "THIS WEEK", "backlog": "BACKLOG"}
_PRIORITY_COLOR = {
    "immediate": (239, 68, 68),
    "this_week": (249, 115, 22),
    "backlog": (59, 130, 246),
}
_OUTCOME_LABEL = {
    "deployed":    ("Fixed",       "#dcfce7", "#15803d", (21, 128, 61)),
    "rolled_back": ("Rolled back", "#fee2e2", "#b91c1c", (185, 28, 28)),
    "rejected":    ("Not deployed", "#ffedd5", "#c2410c", (194, 65, 12)),
    "no_update":   ("No update",   "#f1f5f9", "#475569", (71, 85, 105)),
    "failed":      ("Error",       "#fee2e2", "#b91c1c", (185, 28, 28)),
    "skipped":     ("Skipped",     "#f1f5f9", "#475569", (71, 85, 105)),
}


def _latin1(text) -> str:
    """fpdf2 core fonts are Latin-1 only; map common Unicode punctuation and drop the rest."""
    text = str(text or "")
    for a, b in (("\u2014", "-"), ("\u2013", "-"), ("\u2192", "->"), ("\u2018", "'"),
                 ("\u2019", "'"), ("\u201c", '"'), ("\u201d", '"'), ("\u2026", "..."),
                 ("\u2265", ">="), ("\u2264", "<="), ("\u00b7", "|")):
        text = text.replace(a, b)
    return text.encode("latin-1", "replace").decode("latin-1")


def _remaining_rows(remaining: dict | None, action_plans: list[dict]) -> list[tuple[str, dict, dict | None]]:
    """(image, {"CRITICAL","HIGH"} counts, best plan) for each image still carrying CRITICAL/HIGH."""
    prio_rank = {"immediate": 0, "this_week": 1, "backlog": 2}
    plan_by_image: dict[str, dict] = {}
    for p in action_plans:
        img = p.get("service_name")
        cur = plan_by_image.get(img)
        if cur is None or prio_rank.get(p.get("priority"), 3) < prio_rank.get(cur.get("priority"), 3):
            plan_by_image[img] = p
    rows = [(img, c, plan_by_image.get(img)) for img, c in (remaining or {}).items()
            if c.get("CRITICAL") or c.get("HIGH")]
    rows.sort(key=lambda r: (-r[1].get("CRITICAL", 0), -r[1].get("HIGH", 0)))
    return rows


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
    remediation: dict | None = None,
    remaining: dict | None = None,
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
            pdf.multi_cell(0, 5, _latin1(concern[:200]), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    # ── Automated Remediation ─────────────────────────────────────────────────
    section_header("Automated Remediation")
    if not remediation:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*muted_rgb)
        pdf.multi_cell(0, 5, "No automated remediation run in the last 12 hours.", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)
    else:
        tiles = (
            ("CRITICAL FIXED", remediation.get("critical_fixed", 0), _SEVERITY_COLOR["CRITICAL"]),
            ("HIGH FIXED",     remediation.get("high_fixed", 0),     _SEVERITY_COLOR["HIGH"]),
            ("IMAGES UPDATED", remediation.get("images_deployed", 0), (21, 128, 61)),
            ("ROLLED BACK",    remediation.get("images_rolled_back", 0), muted_rgb),
        )
        for label, _, _ in tiles:
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_text_color(*muted_rgb)
            pdf.cell(col_w, 5, label, align="C")
        pdf.ln()
        for _, val, rgb in tiles:
            pdf.set_font("Helvetica", "B", 20)
            pdf.set_text_color(*rgb)
            pdf.cell(col_w, 11, str(val), align="C")
        pdf.ln(14)

        for a in remediation.get("actions") or []:
            if a.get("outcome") == "skipped":
                continue
            label, _, _, rgb = _OUTCOME_LABEL.get(a.get("outcome"), (a.get("outcome"), "", "", muted_rgb))
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*dark)
            pdf.cell(95, 5, _latin1(a.get("image_ref", ""))[:60])
            pdf.set_text_color(*rgb)
            pdf.cell(30, 5, _latin1(label))
            pdf.set_text_color(*mid_gray)
            pdf.set_font("Helvetica", "", 9)
            fixed = (f"-{a.get('critical_fixed', 0)}C / -{a.get('high_fixed', 0)}H"
                     if a.get("outcome") == "deployed" else "")
            pdf.cell(0, 5, fixed, ln=True)
            if a.get("reason"):
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(*muted_rgb)
                pdf.multi_cell(0, 4, _latin1(a["reason"])[:300], new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

        sweep = remediation.get("health_sweep") or []
        if sweep:
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*_SEVERITY_COLOR["CRITICAL"])
            pdf.multi_cell(0, 5, _latin1(
                "Health sweep: not healthy after the run - "
                + ", ".join(f"{c.get('name')} ({c.get('state')})" for c in sweep)
            ), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    # ── Remaining Critical & High: plan per image ─────────────────────────────
    rows = _remaining_rows(remaining, action_plans)
    if rows:
        section_header("Remaining Critical & High - Plan")
        for image, counts, plan in rows:
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*dark)
            pdf.multi_cell(0, 6, _latin1(
                f"{image}  -  {counts.get('CRITICAL', 0)} critical / {counts.get('HIGH', 0)} high"
            ), new_x="LMARGIN", new_y="NEXT")
            if not plan:
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(*muted_rgb)
                pdf.multi_cell(0, 5, "No plan generated for this image.", new_x="LMARGIN", new_y="NEXT")
                pdf.ln(3)
                continue

            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*accent)
            pdf.multi_cell(0, 5, _latin1(plan.get("title", "")), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "I", 8)
            pdf.set_text_color(*muted_rgb)
            meta = (f"Priority: {_PRIORITY_LABEL.get(plan.get('priority'), plan.get('priority'))}  |  "
                    f"Type: {plan.get('action_type', '')}  |  Effort: {plan.get('estimated_effort', '')}")
            pdf.cell(0, 5, _latin1(meta), ln=True)
            if plan.get("blocker"):
                pdf.multi_cell(0, 5, _latin1("Why not automatic: " + plan["blocker"]), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(*mid_gray)
            pdf.multi_cell(0, 5, _latin1(plan.get("description", "")), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

            steps = plan.get("steps", [])
            if steps:
                pdf.set_font("Helvetica", "B", 8)
                pdf.set_text_color(*dark)
                pdf.cell(0, 5, "Remediation Steps:", ln=True)
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(*mid_gray)
                for i, step in enumerate(steps, 1):
                    pdf.multi_cell(0, 5, _latin1(f"  {i}. {step[:300]}"), new_x="LMARGIN", new_y="NEXT")

            pdf.set_draw_color(*border_rgb)
            pdf.set_line_width(0.2)
            pdf.line(15, pdf.get_y() + 3, 195, pdf.get_y() + 3)
            pdf.ln(7)

    # ── Footer ────────────────────────────────────────────────────────────────
    pdf.set_auto_page_break(auto=False)
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
    remediation: dict | None = None,
    remaining: dict | None = None,
) -> str:
    esc = html.escape
    priority_counts: dict[str, int] = {}
    for plan in action_plans:
        k = plan.get("priority", "backlog")
        priority_counts[k] = priority_counts.get(k, 0) + 1

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

    section_label = (
        'font-size:12px;font-weight:600;color:#6b7280;text-transform:uppercase;'
        'letter-spacing:0.05em;margin-bottom:10px;'
    )

    # ── Automated remediation ────────────────────────────────────────────────
    if remediation:
        def tile(label: str, value, bg: str, fg: str) -> str:
            return (
                f'<td style="padding:6px;text-align:center;">'
                f'<div style="background:{bg};border-radius:8px;padding:8px 10px;">'
                f'<div style="color:{fg};font-size:22px;font-weight:700;">{value}</div>'
                f'<div style="color:{fg};font-size:10px;font-weight:600;margin-top:2px;">{label}</div>'
                f'</div></td>'
            )
        tiles = (
            tile("CRITICAL FIXED", remediation.get("critical_fixed", 0), "#fee2e2", "#b91c1c")
            + tile("HIGH FIXED", remediation.get("high_fixed", 0), "#ffedd5", "#c2410c")
            + tile("IMAGES UPDATED", remediation.get("images_deployed", 0), "#dcfce7", "#15803d")
            + tile("ROLLED BACK", remediation.get("images_rolled_back", 0), "#f1f5f9", "#475569")
        )
        action_rows = ""
        for a in remediation.get("actions") or []:
            if a.get("outcome") == "skipped":
                continue
            label, bg, fg, _ = _OUTCOME_LABEL.get(a.get("outcome"), (a.get("outcome"), "#f1f5f9", "#475569", None))
            fixed = (f'-{a.get("critical_fixed", 0)}C / -{a.get("high_fixed", 0)}H'
                     if a.get("outcome") == "deployed" else "")
            action_rows += f"""
            <tr>
              <td style="padding:8px 6px;border-top:1px solid #f1f5f9;font-size:12px;color:#111827;font-family:monospace;">{esc(a.get("image_ref", ""))}</td>
              <td style="padding:8px 6px;border-top:1px solid #f1f5f9;"><span style="background:{bg};color:{fg};border-radius:4px;padding:2px 8px;font-size:11px;font-weight:700;white-space:nowrap;">{esc(label)}</span></td>
              <td style="padding:8px 6px;border-top:1px solid #f1f5f9;font-size:12px;color:#374151;white-space:nowrap;">{fixed}</td>
            </tr>
            <tr><td colspan="3" style="padding:0 6px 8px;font-size:11px;color:#6b7280;">{esc(a.get("reason") or "")}</td></tr>"""
        sweep = remediation.get("health_sweep") or []
        sweep_html = ""
        if sweep:
            names = ", ".join(f'{esc(c.get("name", ""))} ({esc(c.get("state", ""))})' for c in sweep)
            sweep_html = (
                '<div style="margin-top:12px;padding:10px 12px;background:#fee2e2;border-radius:6px;'
                f'color:#b91c1c;font-size:12px;font-weight:600;">Not healthy after the run: {names}</div>'
            )
        remediation_html = f"""
  <div style="padding:16px 32px 8px;">
    <div style="{section_label}">Automated Remediation</div>
    <table style="width:100%;border-collapse:collapse;"><tr>{tiles}</tr></table>
    {"<table style='width:100%;border-collapse:collapse;margin-top:12px;'>" + action_rows + "</table>" if action_rows else ""}
    {sweep_html}
  </div>"""
    else:
        remediation_html = f"""
  <div style="padding:16px 32px 8px;">
    <div style="{section_label}">Automated Remediation</div>
    <div style="font-size:13px;color:#6b7280;">No automated remediation run in the last 12 hours.</div>
  </div>"""

    # ── Remaining CRITICAL/HIGH: one plan per image ──────────────────────────
    remaining_html = ""
    for image, counts, plan in _remaining_rows(remaining, action_plans):
        head = (
            f'<div style="font-family:monospace;font-size:12px;color:#6b7280;">{esc(image)} &nbsp;·&nbsp; '
            f'<span style="color:#b91c1c;font-weight:700;">{counts.get("CRITICAL", 0)} critical</span> / '
            f'<span style="color:#c2410c;font-weight:700;">{counts.get("HIGH", 0)} high</span></div>'
        )
        if not plan:
            remaining_html += (
                '<div style="border:1px solid #e5e7eb;border-radius:8px;padding:12px 16px;margin:12px 0;">'
                f'{head}<div style="font-size:12px;color:#6b7280;margin-top:6px;">No plan generated for this image.</div></div>'
            )
            continue
        steps_html = "".join(
            f'<li style="margin:4px 0;color:#374151;">{esc(str(step))}</li>'
            for step in (plan.get("steps") or [])[:6]
        )
        blocker = (
            f'<div style="font-size:12px;color:#92400e;background:#fef3c7;border-radius:4px;padding:6px 8px;margin:8px 0;">'
            f'<b>Why not automatic:</b> {esc(plan["blocker"])}</div>'
            if plan.get("blocker") else ""
        )
        remaining_html += f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;margin:12px 0;">
          {head}
          <div style="font-weight:700;font-size:14px;color:#111827;margin-top:6px;">{esc(plan.get("title", ""))}</div>
          <div style="font-size:12px;color:#6b7280;margin:4px 0;">
            {esc(_PRIORITY_LABEL.get(plan.get("priority"), plan.get("priority") or ""))} &nbsp;·&nbsp; {esc(plan.get("action_type", ""))} &nbsp;·&nbsp; {esc(plan.get("estimated_effort", ""))}
          </div>
          {blocker}
          <div style="font-size:13px;color:#374151;margin:8px 0;">{esc(plan.get("description", ""))}</div>
          {"<ol style='margin:8px 0 0 16px;padding:0;font-size:12px;'>" + steps_html + "</ol>" if steps_html else ""}
        </div>"""

    concerns_html = "".join(
        f'<li style="margin:4px 0;color:#374151;">{html.escape(str(c))}</li>'
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

  <!-- Automated remediation -->
  {remediation_html}

  <!-- Remaining critical/high plans -->
  {"<div style='padding:16px 32px 8px;'><div style='" + section_label + "margin-bottom:4px;'>Remaining Critical &amp; High &mdash; Plan</div>" + remaining_html + "</div>" if remaining_html else ""}

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
