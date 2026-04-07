"""
Weather dbt pipeline job.

Materializes all weather dbt assets (staging → intermediate → marts) on a
30-minute schedule. The Kafka producer polls Open-Meteo every 15 minutes,
so 30-minute dbt runs keep marts fresh while avoiding redundant back-to-back
builds during the consumer's flush window.

No Airbyte sync step — data arrives continuously via the Kafka consumer
writing directly to raw_weather.weather_observations and
raw_weather.air_quality_observations.
"""
from dagster import define_asset_job

from ..assets.weather_dbt import weather_dbt_assets

weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=[weather_dbt_assets],
    tags={"domain": "weather", "team": "dbt"},
)
