from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

HEALTH_MANIFEST = Path("/opt/dagster/dbt_artifacts/health/target/manifest.json")


@dbt_assets(
    manifest=HEALTH_MANIFEST,
    select="staging.mail marts.mail",
)
def mail_dbt_assets(context: AssetExecutionContext, health_dbt: DbtCliResource):
    yield from health_dbt.cli(["build"], context=context).stream()
