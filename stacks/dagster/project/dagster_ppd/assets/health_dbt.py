"""
Health dbt models as Dagster software-defined assets.

Manifest is generated at container startup by entrypoint.sh via `dbt parse`.
Each dbt model in the selection becomes a Dagster asset with full lineage.
"""
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

HEALTH_MANIFEST = Path("/opt/dagster/dbt_artifacts/health/target/manifest.json")


@dbt_assets(
    manifest=HEALTH_MANIFEST,
    select="staging.health marts.health",
    exclude="mv_espn_play_stat",
)
def health_dbt_assets(context: AssetExecutionContext, health_dbt: DbtCliResource):
    yield from health_dbt.cli(["build"], context=context).stream()
