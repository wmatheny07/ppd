from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import extraction, enrichment
from .assets.dbt_assets import mail_dbt_assets
from .resources.minio import MinIOResource
from .resources.postgres import PostgresResource
from .resources.anthropic import AnthropicResource
from .sensors.minio_sensor import mail_scan_sensor

all_assets = [
    *load_assets_from_modules([extraction, enrichment]),
    mail_dbt_assets,
]

defs = Definitions(
    assets=all_assets,
    sensors=[mail_scan_sensor],
    resources={
        "minio": MinIOResource(
            endpoint_url="${MINIO_ENDPOINT}",
            access_key="${MINIO_ACCESS_KEY}",
            secret_key="${MINIO_SECRET_KEY}",
            bucket="mail-raw",
        ),
        "postgres": PostgresResource(
            host="${POSTGRES_HOST}",
            port=5432,
            database="${POSTGRES_DB}",
            user="${POSTGRES_USER}",
            password="${POSTGRES_PASSWORD}",
        ),
        "anthropic": AnthropicResource(
            api_key="${ANTHROPIC_API_KEY}",
        ),
        "dbt": DbtCliResource(
            project_dir="mail_intelligence/dbt_project",
            profiles_dir="mail_intelligence/dbt_project",
        ),
    },
)
