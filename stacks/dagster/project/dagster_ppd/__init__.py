from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.health_dbt import health_dbt_assets
from .assets.weather_dbt import weather_dbt_assets
from .jobs.espn_ingest import espn_ingest_job
from .jobs.health_pipeline import health_pipeline_job
from .jobs.nfl_dfs_ingest import nfl_dfs_ingest_job
from .jobs.weather_pipeline import weather_pipeline_job
from .schedules import (
    espn_ingest_schedule,
    health_pipeline_schedule,
    nfl_dfs_ingest_schedule,
    weather_pipeline_schedule,
)

DBT_PROFILES_DIR = "/opt/dbt/profiles"

# Shared DbtCliResource — both health and weather models live in the same
# analytics project (/opt/dagster/dbt/health), so they share one resource
# and one manifest. Weather assets are distinguished by tag:weather selection.
_health_dbt = DbtCliResource(
    project_dir="/opt/dagster/dbt/health",
    profiles_dir=DBT_PROFILES_DIR,
    target_path="/opt/dagster/dbt_artifacts/health/target",
)

defs = Definitions(
    assets=[health_dbt_assets, weather_dbt_assets],
    jobs=[
        espn_ingest_job,
        health_pipeline_job,
        nfl_dfs_ingest_job,
        weather_pipeline_job,
    ],
    schedules=[
        espn_ingest_schedule,
        health_pipeline_schedule,
        nfl_dfs_ingest_schedule,
        weather_pipeline_schedule,
    ],
    resources={
        "health_dbt": _health_dbt,
    },
)
