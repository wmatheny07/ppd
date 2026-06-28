-- models/marts/mart_mail_inbox.sql
-- Actionable inbox: documents requiring follow-up, sorted by recency.
-- Powers the primary Superset dashboard view.

WITH classified AS (
    SELECT * FROM {{ ref('stg_mail_documents') }}
),

inbox AS (
    SELECT
        document_id
        , sender
        , document_type
        , document_date
        , summary
        , action_description
        , minio_key

        -- Age buckets for dashboard filtering
        , CASE
            WHEN document_date >= CURRENT_DATE - INTERVAL '7 days'
                THEN 'This Week'
            WHEN document_date >= CURRENT_DATE - INTERVAL '30 days'
                THEN 'This Month'
            WHEN document_date >= CURRENT_DATE - INTERVAL '90 days'
                THEN 'Last 90 Days'
            ELSE 'Older'
        END                                         AS age_bucket

        -- Dollar summary (unnested from JSONB)
        , (
            SELECT SUM((amt->>'value')::numeric)
            FROM jsonb_array_elements(dollar_amounts) AS amt
        )                                           AS total_amount

        , CURRENT_DATE - document_date              AS days_old

    FROM classified
    WHERE action_required = TRUE
        AND document_date IS NOT NULL
),

ranked AS (
    SELECT
        *
        , ROW_NUMBER() OVER (ORDER BY document_date DESC)
            AS inbox_rank
    FROM inbox
)

SELECT * FROM ranked
ORDER BY document_date DESC
