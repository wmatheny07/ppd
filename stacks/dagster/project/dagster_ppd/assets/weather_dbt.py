"""
Weather dbt models as Dagster software-defined assets.

Selects all models tagged with 'weather' from the shared analytics project:
  staging/weather/      → stg_weather_observations, stg_air_quality_observations
  intermediate/weather/ → int_weather_hourly_conditions, int_weather_air_quality_combined
  marts/weather/        → mart_daily_weather_summary, mart_location_comparison,
                          mart_health_weather_alerts

The manifest is generated at container startup by entrypoint.sh via `dbt parse`
for the health project — weather models now live in that same project so no
additional parse step is needed.
"""
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

HEALTH_MANIFEST = Path("/opt/airflow/dbt_artifacts/health/target/manifest.json")


@dbt_assets(
    manifest=HEALTH_MANIFEST,
    select="tag:weather",
)
def weather_dbt_assets(context: AssetExecutionContext, health_dbt: DbtCliResource):
    yield from health_dbt.cli(["build"], context=context).stream()
