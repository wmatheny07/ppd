"""
ESPN core ingest job: pull_events -> pull_athletes -> pull_play_by_play

Equivalent to Airflow DAG: espn_core_ingest (schedule: Saturdays at 3am ET)

Season/week range is controlled by env vars:
  ESPN_SEASON      (default: 2025)
  ESPN_SEASON_TYPE (default: 2  = regular season)
  ESPN_WEEK_START  (default: 1)
  ESPN_WEEK_END    (default: 18)
  PLAY_BATCH_SIZE        (default: 250)
  PARTICIPANT_BATCH_SIZE (default: 2000)
  PBP_WORKERS            (default: 2)
"""
from __future__ import annotations

import os
import subprocess

from dagster import In, Nothing, Out, get_dagster_logger, job, op

JOBS_DIR = "/opt/airflow/jobs"


@op(out=Out(Nothing))
def pull_events(context):
    logger = get_dagster_logger()
    season = os.environ.get("ESPN_SEASON", "2025")
    week_start = os.environ.get("ESPN_WEEK_START", "1")
    week_end = os.environ.get("ESPN_WEEK_END", "18")

    logger.info(f"Pulling ESPN events: season={season} weeks={week_start}-{week_end}")
    subprocess.run(
        [
            "python3",
            f"{JOBS_DIR}/espn/pull_nfl_events.py",
            "--season", season,
            "--week-start", week_start,
            "--week-end", week_end,
        ],
        check=True,
    )


@op(ins={"start": In(Nothing)}, out=Out(Nothing))
def pull_athletes(context):
    logger = get_dagster_logger()
    season = os.environ.get("ESPN_SEASON", "2025")

    logger.info(f"Pulling NFL athletes for season {season}")
    subprocess.run(
        [
            "python3", "-u",
            f"{JOBS_DIR}/espn/pull_nfl_athletes.py",
            "--season", season,
        ],
        check=True,
    )


@op(ins={"start": In(Nothing)})
def pull_play_by_play(context):
    logger = get_dagster_logger()
    season = os.environ.get("ESPN_SEASON", "2025")
    season_type = os.environ.get("ESPN_SEASON_TYPE", "2")
    week_start = os.environ.get("ESPN_WEEK_START", "1")
    week_end = os.environ.get("ESPN_WEEK_END", "18")
    play_batch = os.environ.get("PLAY_BATCH_SIZE", "250")
    participant_batch = os.environ.get("PARTICIPANT_BATCH_SIZE", "2000")
    workers = os.environ.get("PBP_WORKERS", "2")

    events_sql = (
        f"select espn_id from public.espn_event "
        f"where season_year = '{season}' "
        f"and season_type = '{season_type}' "
        f"and week BETWEEN '{week_start}' AND '{week_end}' "
        f"order by week, espn_id"
    )

    logger.info(f"Pulling play-by-play: season={season} weeks={week_start}-{week_end}")
    subprocess.run(
        [
            "python3",
            f"{JOBS_DIR}/espn/pull_nfl_pbp.py",
            "--events-sql", events_sql,
            "--play-batch-size", play_batch,
            "--participant-batch-size", participant_batch,
            "--workers", workers,
        ],
        check=True,
    )


@job(tags={"domain": "espn", "team": "nfl"})
def espn_ingest_job():
    pull_play_by_play(pull_athletes(pull_events()))
