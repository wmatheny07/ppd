# superset_config.py

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
"""

# Optional: manage themes in the UI too
ENABLE_UI_THEME_ADMINISTRATION = True
