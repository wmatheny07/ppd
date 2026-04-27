from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from ...resources.minio import MinIOResource
from ...resources.postgres import PostgresResource


@sensor(
    job_name="mail_pipeline_job",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def mail_scan_sensor(
    context: SensorEvaluationContext,
    minio: MinIOResource,
    postgres: PostgresResource,
):
    objects = minio.list_objects(prefix="inbox/")
    bucket_keys = {obj["Key"] for obj in objects}

    if not bucket_keys:
        return

    processed = postgres.fetch_all(
        "SELECT minio_key FROM mail_raw.mail_documents WHERE extraction_status = 'complete'"
    )
    processed_keys = {row["minio_key"] for row in processed}

    new_keys = bucket_keys - processed_keys

    for key in new_keys:
        context.log.info(f"New scan detected: {key} — triggering pipeline run")
        yield RunRequest(
            run_key=key,
            run_config={
                "ops": {
                    "raw_mail_documents": {
                        "config": {"minio_key": key}
                    }
                }
            },
        )
