{% docs __overview__ %}

# Analytics Project — Data Catalog Overview

This project transforms raw data from three domains — **Weather & Air Quality**, **Personal Health**, and **ESPN Play-by-Play** — into analysis-ready tables. Data flows from raw API and export sources through standardized staging views, enrichment intermediates, and materialized mart tables used for dashboards, alerts, and personal health monitoring.

---

## Domains

### Weather & Air Quality

Raw weather and air quality readings are ingested every 15 minutes (HRRR high-resolution model) and hourly via the Open-Meteo Forecast and Air Quality APIs, streamed through Kafka into a `raw_weather` schema. Five locations are monitored:

| Location | Role |
|---|---|
| Summerville, SC | Home base (used as comparison baseline) |
| La Plata, MD | Secondary location |
| Ooltewah, TN | Secondary location |
| Fairfax, VA | Secondary location |
| Clifton Forge, VA | Secondary location |

Staging models deduplicate raw records and standardize column naming and units (e.g., `temperature_2m` → `temperature_f`, `pm2_5` → `pm2_5_ugm3`). The intermediate layer aggregates 15-minute observations to hourly, joins weather with air quality, and derives a suite of **health-oriented computed metrics**:

- **Heat / Cold Risk Category** — classifies conditions from `normal` through `extreme_danger` / `extreme_cold`
- **Humidity Comfort Level** — `very_dry` → `comfortable` → `oppressive`
- **UV Risk Category** — `low` → `extreme`
- **Pressure Change** — 1-hour and 3-hour deltas (relevant for migraine and joint pain tracking)
- **Exercise Suitability Score** (0–100) — penalizes extreme temperatures, high wind, precipitation, poor visibility, and elevated UV
- **Outdoor Health Score** (0–100) — exercise suitability further adjusted for AQI and pollen load
- **AQI Health Category** — `good` → `hazardous` based on US AQI composite

Mart models roll these hourly enriched records into daily location summaries, cross-location comparisons with rankings, and an alert table that fires on threshold crossings (extreme heat/cold, rapid pressure swings, dangerous air quality, high pollen, dangerous wind gusts, etc.) with a weighted **alert severity score**.

**Data freshness SLA**: warn if source is older than 30 minutes; error after 2 hours.

---

### Personal Health (Apple Health)

Health data is extracted from Apple Health JSON exports for tracked individuals. Seventeen staging views cover the full breadth of Apple Health metric types:

- **Workouts** — type, duration, calories, intensity, GPS routes
- **Heart Rate** — raw readings, daily min/max/avg, resting HR, heart rate variability (HRV/SDNN), and post-workout recovery HR
- **Sleep** — REM, core, deep, and awake stage durations; in-bed and sleep window timestamps
- **Activity** — active energy burned, basal metabolic energy, step count, walking/running distance, walking speed, walking step length, physical effort scores

Mart models build analytical facts on top of these views:

- **`fct_workout_summary`** — deduplicated, source-prioritized workout records with reclassification logic (e.g., low-intensity runs recategorized as hiking) and iFIT calorie override
- **`fct_active_energy_summary`** — per-session active calorie totals with start/end timestamps
- **`fct_workout_time_adj`** — corrects workout end times for sessions with anomalous calorie burn curves
- **`fct_sleep_metrics`** — nightly sleep stage breakdown per person
- **`fct_dim_sleep`** — unpivoted sleep stages (one row per stage per night) for dimensional analysis
- **`fct_sleep_calories`** — joins nightly sleep data with same-day calorie burn
- **`fct_hr_zones`** — maps heart rate readings to workout windows and classifies time spent in Recovery, Fat Burn, Cardio, Threshold, and Peak zones using age-adjusted max HR
- **`fct_resting_hr`** — daily resting heart rate record per person

---

### ESPN Play-by-Play

ESPN play-by-play data is sourced from a foreign data wrapper (`espn_raw` schema) and staged for downstream analysis. Three entity types are tracked:

- **Plays** — sequence, type, clock, scoring, yardage, drive, penalty, possession, and team context
- **Participant Stats** — per-athlete stat payloads attached to each play (flexible JSONB structure)
- **Team Stats** — per-team (offense/defense) stat payloads per play

> **Note**: ESPN models are currently disabled in the project configuration and are not actively built.

---

## Model Relationship Diagram

![Model Relationship Diagram](dag.svg)

---

## Layer Summary

| Layer | Materialization | Purpose |
|---|---|---|
| **Sources** | External tables | Raw API / export data; freshness monitored |
| **Staging** | View | Deduplicate, rename, cast, unit-standardize |
| **Intermediate** | View | Aggregate, join domains, derive health metrics |
| **Marts** | Table / Incremental | Analysis-ready facts; dashboards and alerts |

## Key Computed Metrics

| Metric | Domain | Range | Description |
|---|---|---|---|
| Exercise Suitability Score | Weather | 0–100 | Composite outdoor exercise safety score |
| Outdoor Health Score | Weather | 0–100 | Exercise score adjusted for AQI and pollen |
| Alert Severity Score | Weather | 0–10 | Weighted count of active health-condition alerts |
| Pressure Change (1h / 3h) | Weather | hPa delta | Used for migraine / joint pain correlation |
| HR Zone Time | Health | minutes | Time in Recovery / Fat Burn / Cardio / Threshold / Peak per workout |
| Age-Adjusted Max HR | Health | BPM | `220 − age` used for HR zone classification |

{% enddocs %}
