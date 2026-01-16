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
              where season_year = '{{ var.value.espn_season_year | default("2025") }}'
                and season_type = '{{ var.value.espn_season_type | default("2") }}'
                and week BETWEEN '{{ var.value.espn_start_week | default("1") }}' AND '{{ var.value.espn_end_week | default("1") }}'
              order by week, espn_id
          " \
            " \
            --play-batch-size '{{ var.value.play_batch_size | default("250") }}' \
            --participant-batch-size '{{ var.value.participant_batch_size | default("2000") }}' \
            --stat-flags-csv '{{ var.value.stats_flag_csv | default("/opt/airflow/jobs/espn/stat_key_whitelist.csv") }}' \
            --workers {{ var.value.pbp_workers | default("1") }}      
            """,
            #append_env=True,
        )

        pull_pbp
