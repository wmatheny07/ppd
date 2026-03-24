# Peak Precision Data

A personal sports analytics and health data platform — data engineering, ML infrastructure, and visualization all self-hosted on a single machine.

---

## What's Here

| Domain | What it does |
|---|---|
| **Health** | Ingests Apple Health exports, transforms them into analytics-ready marts, and surfaces cardiovascular, sleep, and activity metrics |
| **Sports** | Collects NFL data from ESPN and Pro Football Network — play-by-play, injuries, depth charts, athlete profiles |
| **Fantasy** | Ingests DraftKings DFS contest data and runs dbt models to support lineup analysis |
| **Infrastructure** | Postgres, MinIO, MLflow, Qdrant, Superset, Metabase, Airflow — all containerized and networked together |

---

## Stack Layout

```
stacks/
├── core/        Postgres, MinIO, Superset, Metabase, MLflow, Qdrant, Jupyter, DK ingest
├── airflow/     Orchestration — Celery-backed Airflow with dbt + ML packages baked in
├── dbt/         Data transformation (staging views → analytics marts)
├── espn/        Django API service for ESPN data
├── superset/    Superset configuration and theming
├── airbyte/     (Airbyte — sync from MinIO to Postgres)
```

All services share a `core_data_net` Docker bridge network and are managed via [`/opt/util/stack.sh`](../util/README.md).

---

## Core Services

### Postgres
Single analytics database server hosting all schemas:

| Database | Purpose |
|---|---|
| `analytics` | dbt staging views and marts (health, sports, fantasy) |
| `espn` | ESPN API Django app models |
| `mlflow` | MLflow experiment and run metadata |
| `airflow` | Airflow job state, XCom, connections |

### MinIO
S3-compatible object storage. Primary buckets:

| Bucket | Contents |
|---|---|
| `health-data` | Apple Health JSON exports (per-person, per-category) |
| `mlflow` | MLflow model artifacts |

### Airflow
Orchestrates all pipelines on `core_data_net`. DAGs:

| DAG | Schedule | What it does |
|---|---|---|
| `health_data_pipeline` | Hourly (8am–10pm ET) | Airbyte sync → dbt build → docs → Elementary report |
| `espn_core_ingest` | Scheduled | ESPN athletes, events, injury reports |
| `nfl_dfs/*` | Scheduled | DraftKings contest ingest + dbt refresh |

### dbt
Transforms raw source data into analytics-ready tables in Postgres.

```
models/
├── staging/health/    Views over raw Apple Health JSON (heart rate, sleep, workouts, energy)
├── staging/espn/      Views over raw ESPN data
└── marts/health/      Incremental fact tables (fct_workout_summary, fct_hr_zones, fct_resting_hr, ...)
```

Targets the `analytics` Postgres database. Data quality monitored via [Elementary](https://docs.elementary-data.com/).

### Superset / Metabase
BI and dashboarding. Superset is the primary tool with custom PPD theming. Both connect to the `analytics` Postgres database.

### MLflow + Qdrant + Jupyter
ML infrastructure: experiment tracking (MLflow), vector storage (Qdrant), and a GPU-enabled Jupyter Lab for model development.

---

## Data Flow

```
Source Data
    │
    ├── Apple Health JSON ──► MinIO (health-data bucket)
    │                              │
    │                         Airbyte sync
    │                              │
    │                         Postgres (raw)
    │                              │
    │                           dbt build
    │                              │
    │                      analytics marts ──► Superset / Metabase
    │
    ├── ESPN API ──► airflow jobs ──► Postgres (espn DB + analytics staging)
    │
    └── DraftKings ──► inbox/dk/ ──► dk-ingest container ──► Postgres (nfl_dfs schema)
```

---

## Data Ingest Folders

```
data/
├── inbox/    Raw files waiting to be processed (subdirs by source: dk/, espn/, ...)
├── archive/  Successfully processed files
└── error/    Failed files awaiting manual review
```

---

## Apps

### `apps/Public-ESPN-API`
Django REST API that serves ESPN data out of Postgres. Backed by Celery + Celery Beat for async and scheduled tasks. Exposed on port `18090`.

---

## Utilities

Day-to-day operations are handled by scripts in [`/opt/util/`](../util/README.md):

- `stack.sh` — bring stacks up/down, restart services, tail logs
- `dbt-run.sh` — ad-hoc dbt builds, tests, full refreshes
- `dbt-dry-run.sh` — preview and compile models without executing
- `inject-env.sh` — pull secrets from 1Password and generate runtime env files
- `ensure-*.sh` — idempotent setup scripts for MinIO buckets, MLflow, Superset

---

## Environment

Runtime secrets are managed via 1Password and injected into `/opt/config/runtime/` env files before any stack starts. The resolved file used at runtime is `/opt/config/runtime/.env.all`.
