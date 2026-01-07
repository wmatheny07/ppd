from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "wes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Airflow Dataset markers (logical “this data is fresh” signals)
ESPN_INGESTED = Dataset("dataset://espn/ingested")

with DAG(
    dag_id="espn_core_ingest",
    description="ESPN core ingest: events -> athletes -> play-by-play",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * 6",  # daily 3am (tune; could be hourly during season)
    catchup=False,
    max_active_runs=1,
    tags=["espn", "nfl"],
) as dag:

    with TaskGroup(group_id="ingest") as ingest:

        pull_events = BashOperator(
            task_id="pull_events",
            bash_command="""
            set -euo pipefail
            python3 /opt/airflow/jobs/espn/pull_nfl_events.py \
              --season {{ var.value.get('ESPN_SEASON', '2025') }} \
              --week-start {{ var.value.get('ESPN_WEEK_START', '1') }} \
              --week-end {{ var.value.get('ESPN_WEEK_END', '18') }}
            """,
            append_env=True,
        )

        pull_athletes = BashOperator(
            task_id="pull_athletes",
            bash_command="""
            set -euo pipefail
            python3 -u /opt/airflow/jobs/espn/pull_nfl_athletes.py \
            --season {{ var.value.get('ESPN_SEASON', '2025') }}
            """,
            append_env=True,
        )

        pull_pbp = BashOperator(
            task_id="pull_play_by_play",
            bash_command="""
            set -euo pipefail

            python3 /opt/airflow/jobs/espn/pull_nfl_pbp.py \
            --events-sql "
                select espn_id
                from public.espn_event
                where season_year = {{ var.value.get('ESPN_SEASON', '2025') }}
                and season_type = {{ var.value.get('ESPN_SEASON_TYPE', '2') }}
                and week between {{ var.value.get('ESPN_WEEK_START', '1') }} and {{ var.value.get('ESPN_WEEK_END', '18') }}
                order by week, espn_id
            " \
            --play-batch-size 250 \
            --participant-batch-size 2000
            """,
            append_env=True,
        )

        pull_events >> pull_athletes >> pull_pbp

    # Publish a dataset update once the ESPN ingest is complete
    publish_dataset = BashOperator(
        task_id="publish_espn_dataset",
        bash_command="echo espn_ingested",
        outlets=[ESPN_INGESTED],
    )

    ingest >> publish_dataset
