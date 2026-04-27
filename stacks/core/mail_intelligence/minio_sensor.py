from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from ..resources.minio import MinIOResource
from ..resources.postgres import PostgresResource


@sensor(
    job_name="mail_pipeline_job",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def mail_scan_sensor(context: SensorEvaluationContext):
    """
    Polls the MinIO mail-raw/ bucket every 30 seconds.
    Compares object keys against already-processed keys in Postgres.
    Yields a RunRequest for each unprocessed file.
    """
    minio: MinIOResource = context.resources.minio
    postgres: PostgresResource = context.resources.postgres

    # Fetch all object keys currently in the bucket
    objects = minio.list_objects(prefix="inbox/")
    bucket_keys = {obj["Key"] for obj in objects}

    if not bucket_keys:
        return

    # Fetch keys already processed (stored in Postgres on successful extraction)
    processed = postgres.fetch_all(
        "SELECT minio_key FROM mail_documents WHERE extraction_status = 'complete'"
    )
    processed_keys = {row["minio_key"] for row in processed}

    new_keys = bucket_keys - processed_keys

    for key in new_keys:
        context.log.info(f"New scan detected: {key} — triggering pipeline run")
        yield RunRequest(
            run_key=key,                    # idempotent: same key never runs twice
            run_config={
                "ops": {
                    "raw_mail_documents": {
                        "config": {"minio_key": key}
                    }
                }
            },
        )
