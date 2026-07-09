from datetime import datetime, timezone

from dagster import sensor, RunRequest, SensorEvaluationContext, DefaultSensorStatus

from ...resources.minio import MinIOResource
from ...resources.postgres import PostgresResource


def _pipeline_run_request(key: str, run_key: str) -> RunRequest:
    return RunRequest(
        run_key=run_key,
        run_config={
            "ops": {
                "raw_mail_documents": {
                    "config": {"minio_key": key}
                }
            }
        },
    )


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
        yield _pipeline_run_request(key, run_key=key)

    retry_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H")

    # Documents that finished extraction but never got an enrichment row --
    # e.g. a prior run crashed on a Claude API error (rate limit, low credit
    # balance). These keys already have a fulfilled run_key from their first
    # attempt, so a plain retry would be a no-op; suffixing run_key with an
    # hourly bucket forces a fresh run without re-hitting the API every 30s
    # if the underlying outage (e.g. billing) hasn't cleared yet.
    unenriched = postgres.fetch_all(
        """
        SELECT d.minio_key
        FROM mail_raw.mail_documents d
        LEFT JOIN mail_raw.mail_enrichments e ON d.id = e.document_id
        WHERE d.extraction_status = 'complete' AND e.document_id IS NULL
        """
    )
    unenriched_keys = {row["minio_key"] for row in unenriched} & bucket_keys

    for key in unenriched_keys:
        context.log.info(f"Unenriched document detected: {key} — retrying pipeline run")
        yield _pipeline_run_request(key, run_key=f"{key}::enrich-retry::{retry_bucket}")

    # Statements that were enriched but never got a successful bank_transactions
    # run -- same crash-with-no-marker failure mode as enrichment. We can't use
    # row existence in mail_raw.bank_transactions the way we do for enrichment,
    # since a statement can legitimately have zero transactions; instead check
    # mail_processing_log for a recorded success on the bank_transactions stage.
    unextracted_statements = postgres.fetch_all(
        """
        SELECT d.minio_key
        FROM mail_raw.mail_documents d
        JOIN mail_raw.mail_enrichments e ON d.id = e.document_id
        WHERE d.extraction_status = 'complete'
          AND e.document_type = 'statement'
          AND NOT EXISTS (
              SELECT 1 FROM mail_raw.mail_processing_log l
              WHERE l.document_id = d.id
                AND l.stage = 'bank_transactions'
                AND l.status = 'success'
          )
        """
    )
    unextracted_statement_keys = {row["minio_key"] for row in unextracted_statements} & bucket_keys

    for key in unextracted_statement_keys:
        context.log.info(f"Statement missing transaction extraction: {key} — retrying pipeline run")
        yield _pipeline_run_request(key, run_key=f"{key}::txn-retry::{retry_bucket}")
