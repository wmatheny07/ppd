from dagster import define_asset_job

from ..assets.mail.extraction import raw_mail_documents
from ..assets.mail.enrichment import enriched_mail_documents
from ..assets.mail.bank_transactions import bank_statement_transactions
from ..assets.mail.mail_dbt import mail_dbt_assets

# Triggered per-document by mail_scan_sensor. Runs extraction → enrichment →
# transaction extraction (statements only) for a single MinIO key.
mail_pipeline_job = define_asset_job(
    name="mail_pipeline_job",
    selection=[raw_mail_documents, enriched_mail_documents, bank_statement_transactions],
    tags={"domain": "mail", "team": "mail_intelligence"},
)

# Runs on a schedule to build staging.mail and marts.mail after documents
# have been extracted and enriched. Kept separate from mail_pipeline_job
# so a single dbt run covers all documents processed since the last build.
mail_dbt_job = define_asset_job(
    name="mail_dbt_job",
    selection=[mail_dbt_assets],
    tags={"domain": "mail", "team": "dbt"},
)
