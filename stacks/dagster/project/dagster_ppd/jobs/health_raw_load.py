"""
Load Apple Health exports from MinIO into the raw health tables.

Replaces the Airbyte health-data connection. Each export file is one row, keyed
on its object key. The table and column names are the ones Airbyte created,
because the dbt health models read data, _ab_source_file_url,
_ab_source_file_last_modified and _airbyte_extracted_at.

Required resources:
  postgres      PostgresResource
  health_minio  MinIOResource(bucket=HEALTH_BUCKET)
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone

from dagster import Nothing, Out, get_dagster_logger, op

from ..resources import MinIOResource, PostgresResource

HEALTH_BUCKET = "health-data"
RAW_SCHEMA = "health"

# table -> (list prefix, key pattern). Skips anything else under the prefix.
STREAMS = {
    "metrics": ("metrics/json/", re.compile(r"^metrics/json/[^/]+/[^/]+\.json$")),
    "Workouts": ("workouts/json/", re.compile(r"^workouts/json/[^/]+/[^/]+\.json$")),
}

LOADED_FILES_SQL = """
    SELECT _ab_source_file_url AS url, _ab_source_file_last_modified AS last_modified
    FROM {table}
"""

ENSURE_UNIQUE_KEY_SQL = """
    CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {table} (_ab_source_file_url)
"""

UPSERT_FILE_SQL = """
    INSERT INTO {table} (
        data, _ab_source_file_url, _ab_source_file_last_modified,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    ) VALUES (%s::jsonb, %s, %s, %s, NOW(), %s::jsonb)
    ON CONFLICT (_ab_source_file_url) DO UPDATE SET
        data = EXCLUDED.data,
        _ab_source_file_last_modified = EXCLUDED._ab_source_file_last_modified,
        _airbyte_raw_id = EXCLUDED._airbyte_raw_id,
        _airbyte_extracted_at = EXCLUDED._airbyte_extracted_at,
        _airbyte_meta = EXCLUDED._airbyte_meta
"""

LOADER_META = json.dumps({"changes": [], "loader": "dagster"})


def qualified_table(table: str) -> str:
    return f'{RAW_SCHEMA}."{table}"'


def format_last_modified(value: datetime) -> str:
    """Match Airbyte's stored format, e.g. 2026-11-01T21:14:27.732000Z."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def parse_export(raw: bytes) -> str:
    """Return the JSON for the data column: the file's top-level "data" value."""
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object, got {type(payload).__name__}")
    return json.dumps(payload.get("data"))


def files_to_load(
    objects: list[dict], pattern: re.Pattern, loaded: dict[str, str]
) -> list[tuple[str, str]]:
    """(key, last_modified) for each export that is new or changed since it was loaded."""
    pending = []
    for obj in objects:
        key = obj["Key"]
        if not pattern.match(key):
            continue
        last_modified = format_last_modified(obj["LastModified"])
        if loaded.get(key) != last_modified:
            pending.append((key, last_modified))
    return sorted(pending)


@op(out=Out(Nothing))
def load_health_raw_files(
    postgres: PostgresResource,
    health_minio: MinIOResource,
):
    logger = get_dagster_logger()
    failures: list[str] = []

    for table, (prefix, pattern) in STREAMS.items():
        target = qualified_table(table)
        postgres.execute(
            ENSURE_UNIQUE_KEY_SQL.format(
                index=f'"{table.lower()}_source_file_url_key"', table=target
            )
        )
        rows = postgres.fetch_all(LOADED_FILES_SQL.format(table=target))
        loaded = {row["url"]: row["last_modified"] for row in rows}
        pending = files_to_load(health_minio.list_objects(prefix), pattern, loaded)

        count = 0
        for key, last_modified in pending:
            # One bad export shouldn't hold back the others; the op still fails below.
            try:
                data = parse_export(health_minio.download_bytes(key))
                postgres.execute(
                    UPSERT_FILE_SQL.format(table=target),
                    (data, key, last_modified, str(uuid.uuid4()), LOADER_META),
                )
                count += 1
            except Exception as exc:
                logger.error(f"Failed to load {key}: {exc}")
                failures.append(key)
        logger.info(f"{target}: loaded {count} of {len(pending)} new or changed files")

    if failures:
        raise RuntimeError(f"{len(failures)} health export(s) failed to load: {failures[:10]}")
