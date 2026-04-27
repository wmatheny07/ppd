"""
Schedules for the PPD Dagster instance.

Schedule mapping from Airflow:
  espn_core_ingest        → espn_ingest_schedule      "0 3 * * 6"    (Saturdays 3am ET)
  health_data_pipeline    → health_pipeline_schedule  "0 8-22 * * *" (hourly 8am-10pm ET)
  nfl_dfs_ingest          → nfl_dfs_ingest_schedule   "0 */2 * * 6,0" (every 2h weekends)

Kafka-sourced:
  weather_pipeline        → weather_pipeline_schedule "*/30 * * * *" (every 30 min, 24/7)
"""
from __future__ import annotations

from dagster import ScheduleDefinition

from .jobs.espn_ingest import espn_ingest_job
from .jobs.health_pipeline import health_pipeline_job
from .jobs.mail_pipeline import mail_dbt_job
from .jobs.nfl_dfs_ingest import nfl_dfs_ingest_job
from .jobs.weather_pipeline import weather_pipeline_job

espn_ingest_schedule = ScheduleDefinition(
    name="espn_ingest_schedule",
    job=espn_ingest_job,
    cron_schedule="0 3 * * 6",       # Saturdays at 3am
    execution_timezone="America/New_York",
)

health_pipeline_schedule = ScheduleDefinition(
    name="health_pipeline_schedule",
    job=health_pipeline_job,
    cron_schedule="0 8-22 * * *",    # Hourly 8am-10pm ET
    execution_timezone="America/New_York",
)

nfl_dfs_ingest_schedule = ScheduleDefinition(
    name="nfl_dfs_ingest_schedule",
    job=nfl_dfs_ingest_job,
    cron_schedule="0 */2 * * 6,0",  # Every 2 hours on weekends
    execution_timezone="America/New_York",
)

weather_pipeline_schedule = ScheduleDefinition(
    name="weather_pipeline_schedule",
    job=weather_pipeline_job,
    cron_schedule="*/30 * * * *",   # Every 30 minutes, 24/7
    execution_timezone="America/New_York",
)

mail_dbt_schedule = ScheduleDefinition(
    name="mail_dbt_schedule",
    job=mail_dbt_job,
    cron_schedule="*/30 * * * *",   # Every 30 minutes — builds after sensor-driven runs
    execution_timezone="America/New_York",
)
