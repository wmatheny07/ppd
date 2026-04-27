# superset_config.py
import os

# ===== Database =====
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://analytics:sausage5poem6moveGLADLY@postgres:5432/superset"
)

# ===== Custom Units for Big Number Charts =====
# Commenting out as this was unsuccessful and caused issues on the front end
#CURRENCIES = ["$", "€", "£", "bpm", "lbs", "steps", "hrs", "mi", "cal", "oz", "hrs", "kWh", "miles", "km", "gallons", "liters", "units", "items", "points"]

THEME_DEFAULT = {
  "token": {
    "fontFamily": "'Inter', ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial",
    "colorPrimary": "#4F46E5",     # <-- set to your Agent portal accent
    "colorSuccess": "#22C55E",
    "colorWarning": "#F59E0B",
    "colorError":   "#EF4444",

    "borderRadius": 12,
    "wireframe": False
  }
}

DASHBOARD_CSS = """
/* ===== Page Background ===== */
body,
.dashboard-container {
  background-color: #F7F7F8 !important;
}

/* ===== Dashboard Title ===== */
.dashboard-header .editable-title {
  color: #111827 !important;
  font-weight: 600;
}

/* ===== Chart Cards ===== */
.dashboard-component-chart-holder {
  background: white !important;
  border-radius: 24px !important;
  box-shadow: 0 8px 28px rgba(15, 23, 42, 0.06) !important;
  border: none !important;
  padding: 16px !important;
}

/* ===== Filter Bar ===== */
.dashboard-filter-bar {
  background: white !important;
  border-radius: 16px !important;
  box-shadow: 0 8px 28px rgba(15, 23, 42, 0.06) !important;
  border: none !important;
}

/* ===== Primary Buttons ===== */
.ant-btn-primary {
  background-color: #FF385C !important;
  border-color: #FF385C !important;
}

.ant-btn-primary:hover {
  background-color: #e03150 !important;
  border-color: #e03150 !important;
}

/* ===== Tabs ===== */
.ant-tabs-tab.ant-tabs-tab-active .ant-tabs-tab-btn {
  color: #FF385C !important;
}

/* ===== Chart Titles ===== */
.slice-container h4 {
  color: #111827 !important;
  font-weight: 600;
}

/* ===== Secondary Text ===== */
.ant-typography,
.ant-table {
  color: #6B7280 !important;
}

/* ===== Remove harsh borders ===== */
.ant-card,
.ant-table-container {
  border: none !important;
}

/* ===== Screenshot / report rendering =====
   Superset's Selenium screenshotter captures the .standalone element by
   bounding box. Without this, .standalone clips at 100vh regardless of
   how tall WEBDRIVER_WINDOW_SIZE is set, cutting off the bottom of the
   dashboard. These rules only affect the report standalone view. */
.standalone {
  height: unset !important;
  max-height: unset !important;
  overflow: visible !important;
}
.standalone .dashboard-content,
.standalone .grid-content,
.standalone .dragdroppable,
.standalone .dragdroppable-content {
  height: unset !important;
  max-height: unset !important;
  overflow: visible !important;
}
"""

# Optional: manage themes in the UI too
ENABLE_UI_THEME_ADMINISTRATION = True

# ===== Alerts & Reports =====
FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "PLAYWRIGHT_REPORTS_AND_THUMBNAILS": True,
}
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False

# ===== Celery (required for scheduled reports) =====
from celery.schedules import crontab

class CeleryConfig:
    broker_url = "redis://redis:6379/0"
    result_backend = "redis://redis:6379/0"
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    worker_prefetch_multiplier = 10
    task_acks_late = True
    # beat_schedule must be explicitly defined here — overriding CeleryConfig
    # in superset_config.py replaces the entire default class, so the schedule
    # that Superset defines in its own config.py is otherwise lost.
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
            "options": {"expires": 604800},  # 1 week
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=0, hour=0),
        },
    }

CELERY_CONFIG = CeleryConfig

# ===== SMTP via Resend relay =====
SMTP_HOST = "smtp.resend.com"
SMTP_PORT = 465
SMTP_SSL = True
SMTP_STARTTLS = False
SMTP_USER = "resend"
SMTP_PASSWORD = os.environ.get("RESEND_API_KEY", "")
SMTP_MAIL_FROM = "reports@peakprecisiondata.com"

# ===== Screenshot rendering =====
WEBDRIVER_TYPE = "chrome"
WEBDRIVER_OPTION_ARGS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
]
# Chrome runs inside the worker container — it must reach Superset via the
# internal Docker hostname, not the external domain (which may not be
# reachable from within the network or may fail SSL from inside).
# Width matches a standard widescreen viewport. Height is set tall enough to
# capture a full multi-section dashboard without clipping — increase further
# if a dashboard grows and starts getting cut off again.
WEBDRIVER_WINDOW_SIZE = (1600, 4000)
WEBDRIVER_BASEURL = "http://superset:8088"
# Links embedded in the report email still point to the public-facing URL.
WEBDRIVER_BASEURL_USER_FRIENDLY = os.environ.get(
    "SUPERSET_WEBSERVER_BASEURL", "https://superset.peakprecisiondata.com"
)
