-- models/staging/stg_mail_documents.sql
-- Cleans and types the raw extraction + enrichment tables.
-- This is the foundation every downstream model builds on.

with raw as (
    select
        d.id                                        as document_id,
        d.minio_key,
        d.file_hash,
        d.page_count,
        d.extraction_method,
        d.ingest_ts,
        d.extracted_ts,

        -- Enrichment fields (left join — may not exist yet)
        e.document_type,
        e.sender_normalized                         as sender,
        e.document_date::date                       as document_date,
        e.dollar_amounts,
        e.action_required,
        e.action_description,
        e.summary,
        e.addressee_name,
        e.mail_owner,
        e.tokens_used,
        e.enriched_ts

    from {{ source('mail_raw', 'mail_documents') }} d
    left join {{ source('mail_raw', 'mail_enrichments') }} e
        on d.id = e.document_id
    where d.extraction_status = 'complete'
),

typed as (
    select
        document_id,
        minio_key,
        file_hash,
        coalesce(page_count, 0)                             as page_count,
        extraction_method,
        ingest_ts at time zone 'UTC'                        as ingest_ts_utc,
        document_date,
        coalesce(document_type, 'unknown')                  as document_type,
        coalesce(sender, 'Unknown Sender')                  as sender,
        coalesce(action_required, false)                    as action_required,
        action_description,
        summary,
        dollar_amounts,
        addressee_name,
        coalesce(mail_owner, 'unknown')                     as mail_owner,
        tokens_used,

        -- Derived flags
        document_date >= current_date - interval '30 days'  as is_recent,
        extract(year from document_date)::int               as document_year,
        extract(month from document_date)::int              as document_month

    from raw
)

select * from typed
