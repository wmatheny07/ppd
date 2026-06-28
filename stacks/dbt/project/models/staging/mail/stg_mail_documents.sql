-- models/staging/stg_mail_documents.sql
-- Cleans and types the raw extraction + enrichment tables.
-- This is the foundation every downstream model builds on.

WITH raw AS (
    SELECT
        d.id                                        AS document_id
        , d.minio_key
        , d.file_hash
        , d.page_count
        , d.extraction_method
        , d.ingest_ts
        , d.extracted_ts

        -- Enrichment fields (left join -- may not exist yet)
        , e.document_type
        , e.sender_normalized                       AS sender
        , e.document_date::date                     AS document_date
        , e.dollar_amounts
        , e.action_required
        , e.action_description
        , e.summary
        , e.addressee_name
        , e.mail_owner
        , e.tokens_used
        , e.enriched_ts

    FROM {{ source('mail_raw', 'mail_documents') }} AS d
    LEFT JOIN {{ source('mail_raw', 'mail_enrichments') }} AS e
        ON d.id = e.document_id
    WHERE d.extraction_status = 'complete'
),

typed AS (
    SELECT
        document_id
        , minio_key
        , file_hash
        , COALESCE(page_count, 0)                   AS page_count
        , extraction_method
        , ingest_ts AT TIME ZONE 'UTC'              AS ingest_ts_utc
        , document_date
        , COALESCE(document_type, 'unknown')         AS document_type
        , COALESCE(sender, 'Unknown Sender')         AS sender
        , COALESCE(action_required, FALSE)           AS action_required
        , action_description
        , summary
        , dollar_amounts
        , addressee_name
        , COALESCE(mail_owner, 'unknown')            AS mail_owner
        , tokens_used

        -- Derived flags
        , document_date >= CURRENT_DATE - INTERVAL '30 days'
            AS is_recent
        , EXTRACT(YEAR FROM document_date)::int      AS document_year
        , EXTRACT(MONTH FROM document_date)::int     AS document_month

    FROM raw
)

SELECT * FROM typed
