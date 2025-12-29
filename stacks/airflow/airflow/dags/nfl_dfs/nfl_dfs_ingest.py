from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# If you're on Airflow 2.6+, you can also use:
# from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "wes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# You can set these in Airflow Variables if you want:
# - NFL_DFS_SCHEMA (e.g. nfl_dfs)
# - DK_INBOX, DK_ARCHIVE, DK_ERROR (paths inside the worker)
# - DK_POLL_SECONDS (if you want dk_ingest to do one pass, set small or implement --once flag)
# - DEPTH_URL / DEPTH_SOURCE

with DAG(
    dag_id="nfl_dfs_ingest",
    description="Ingest DK salaries + PFN depth charts, then build active player pool",
    template_searchpath=["/opt/airflow/sql", "/opt/airflow/dags"],
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 */2 * * 6,0",  # every 2 hours (tune; on Sundays you might do */30 mins)
    catchup=False,
    max_active_runs=1,
    tags=["nfl", "dfs"],
) as dag:

    # ---- Bootstrap ensuring data structure is in place in PG ----
    bootstrap = PostgresOperator(
        task_id="bootstrap_nfl_dfs_ddl",
        postgres_conn_id="analytics_postgres",
        sql="nfl_dfs_bootstrap.sql",
    )

    # ---- PFN Depth Charts ----
    depth_ingest = BashOperator(
        task_id="depth_chart_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/pfn/pfn_depth_ingest.py
        """,
        env={
          "DEPTH_SCHEMA": "{{ var.value.get('NFL_DFS_SCHEMA', 'nfl_dfs') }}",
          "DEPTH_SOURCE": "pfn",
          "DEPTH_URL": "https://www.profootballnetwork.com/nfl/depth-chart/",
      },
        append_env=True,   # <-- THIS is the key
    )

    # ---- DraftKings Salaries ----
    # Best practice: adjust dk_ingest.py to support an "--once" mode.
    # If you don't yet, this DAG can still run it in a "one-shot" wrapper by:
    # - setting DK_POLL_SECONDS low
    # - and having script exit after processing current files (recommended change)
    dk_salaries_ingest = BashOperator(
        task_id="dk_salaries_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/dk/dk_ingest.py --once
        """,
        env={
            "DK_SCHEMA": "{{ var.value.get('NFL_DFS_SCHEMA', 'nfl_dfs') }}",
            "DK_INBOX": "{{ var.value.get('DK_INBOX', '/data/inbox') }}",
            "DK_ARCHIVE": "{{ var.value.get('DK_ARCHIVE', '/data/archive') }}",
            "DK_ERROR": "{{ var.value.get('DK_ERROR', '/data/error') }}",
            "DK_POLL_SECONDS": "{{ var.value.get('DK_POLL_SECONDS', '60') }}",
            "DB_HOST": "{{ var.value.get('DB_HOST', 'postgres') }}",
            "DB_PORT": "{{ var.value.get('DB_PORT', '5432') }}",
            "DB_NAME": "{{ var.value.get('DB_NAME', 'analytics') }}",
            "DB_USER": "{{ var.value.get('DB_USER', 'analytics') }}",
            "DB_PASSWORD": "{{ var.value.get('DB_PASSWORD', '') }}",
        },
    )

    # ---- ESPN Injury Report ----
    # Pulling injury report from ESPN since depth chart and be misleading at times
    espn_injury_rpt_ingest = BashOperator(
        task_id="espn_injury_rpt_ingest",
        bash_command="""
        set -euo pipefail
        python3 /opt/airflow/jobs/espn/espn_injury_report.py
        """,
    )

    # ---- Build Active Player Pool ----
    # Uses a Postgres connection defined in Airflow UI (Admin -> Connections).
    # Create one called "analytics_postgres" (or change postgres_conn_id below).
    build_player_pool = PostgresOperator(
        task_id="build_active_player_pool",
        postgres_conn_id="analytics_postgres",
        sql="""
        DO $$
        DECLARE
          s text := '{{ var.value.get("NFL_DFS_SCHEMA", "nfl_dfs") }}';
        BEGIN
          EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I;', s);

          -- Example materialized table (you can make it a VIEW instead)
        EXECUTE format($fmt$
          CREATE TABLE IF NOT EXISTS %I.active_player_pool (
            slate_date date NOT NULL,
            dk_player_id bigint,
            name text,
            team_abbrev text,
            position text,
            roster_position text,
            salary int,
            game_info text,
            avg_points_per_game numeric,
            depth_position_group text,
            depth_rank int,
            player_slug text,
            updated_ts timestamptz NOT NULL DEFAULT now(),
            player_key text GENERATED ALWAYS AS (COALESCE(dk_player_id::text, name)) STORED,
            PRIMARY KEY (slate_date, roster_position, player_key)
          );
        $fmt$, s);

          -- Refresh logic (simple: rebuild for latest slate_date in dk_salaries)
          EXECUTE format($fmt$
            WITH latest AS (
              SELECT max(slate_date) AS slate_date
              FROM %I.dk_salaries
            )
            DELETE FROM %I.active_player_pool p
            USING latest
            WHERE p.slate_date = latest.slate_date;
          $fmt$, s, s);

          EXECUTE format($fmt$
            WITH latest AS (
              SELECT max(slate_date) AS slate_date
              FROM %I.dk_salaries
            )
            INSERT INTO %I.active_player_pool (
              slate_date, dk_player_id, name, team_abbrev, position, roster_position,
              salary, game_info, avg_points_per_game, depth_position_group, depth_rank, player_slug
            )
            SELECT
              sal.slate_date,
              sal.dk_player_id,
              sal.name,
              sal.team_abbrev,
              sal.position,
              sal.roster_position,
              sal.salary,
              sal.game_info,
              sal.avg_points_per_game,
              dc.position_group,
              dc.depth_rank,
              dc.player_slug
            FROM %I.dk_salaries sal
            JOIN latest ON latest.slate_date = sal.slate_date
            LEFT JOIN %I.depth_chart_current dc
              ON dc.slate_date = sal.slate_date
            AND dc.team_abbrev = sal.team_abbrev
            AND dc.player_name_norm = lower(sal.name)
            ;
          $fmt$, s, s, s, s);

        END $$;
        """,
    )

    bootstrap >> dk_salaries_ingest >> depth_ingest >> espn_injury_rpt_ingest >> build_player_pool
