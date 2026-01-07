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

with DAG(
    dag_id="espn_quick_pbp_test",
    description="ESPN core ingest: play-by-play",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    #schedule="0 3 * * 6",  # daily 3am (tune; could be hourly during season)
    catchup=False,
    max_active_runs=1,
    tags=["espn", "nfl"],
) as dag:

    with TaskGroup(group_id="test_pbp_ingest") as ingest:

        pull_pbp = BashOperator(
            task_id="pull_play_by_play",
            bash_command="""
            set -euo pipefail

            python3 /opt/airflow/jobs/espn/pull_nfl_pbp.py \
            --events-sql "
                select espn_id
                from public.espn_event
                where season_year = '2025'
                and season_type = '2'
                and week = '1'
                order by week, espn_id
            " \
            --play-batch-size 250 \
            --participant-batch-size 2000 \
            --stat-flags-csv /opt/airflow/jobs/espn/stat_key_whitelist.csv
            """,
            #append_env=True,
        )

        pull_pbp
