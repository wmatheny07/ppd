from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.health_dbt import health_dbt_assets
from .assets.weather_dbt import weather_dbt_assets
from .assets.mail import raw_mail_documents, enriched_mail_documents, mail_dbt_assets, mail_scan_sensor
from .jobs.espn_ingest import espn_ingest_job
from .jobs.health_pipeline import health_pipeline_job
from .jobs.mail_pipeline import mail_dbt_job, mail_pipeline_job
from .jobs.nfl_dfs_ingest import nfl_dfs_ingest_job
from .jobs.weather_pipeline import weather_pipeline_job
from .resources import AnthropicResource, MinIOResource, PostgresResource
from .schedules import (
    espn_ingest_schedule,
    health_pipeline_schedule,
    mail_dbt_schedule,
    nfl_dfs_ingest_schedule,
    weather_pipeline_schedule,
)

DBT_PROFILES_DIR = "/opt/dbt/profiles"

# Shared DbtCliResource — health, weather, and mail models all live in the same
# analytics project, so they share one resource and one manifest.
_health_dbt = DbtCliResource(
    project_dir="/opt/dagster/dbt/health",
    profiles_dir=DBT_PROFILES_DIR,
    target_path="/opt/dagster/dbt_artifacts/health/target",
)

defs = Definitions(
    assets=[
        health_dbt_assets,
        weather_dbt_assets,
        raw_mail_documents,
        enriched_mail_documents,
        mail_dbt_assets,
    ],
    jobs=[
        espn_ingest_job,
        health_pipeline_job,
        mail_pipeline_job,
        mail_dbt_job,
        nfl_dfs_ingest_job,
        weather_pipeline_job,
    ],
    schedules=[
        espn_ingest_schedule,
        health_pipeline_schedule,
        mail_dbt_schedule,
        nfl_dfs_ingest_schedule,
        weather_pipeline_schedule,
    ],
    sensors=[
        mail_scan_sensor,
    ],
    resources={
        "health_dbt": _health_dbt,
        "minio": MinIOResource(),
        "postgres": PostgresResource(),
        "anthropic": AnthropicResource(),
    },
)
