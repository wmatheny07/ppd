-- models/marts/mart_mail_by_sender.sql
-- Sender-level rollup: frequency, recency, total spend from financial mail.
-- Useful for spotting subscription creep, insurance EOB patterns,
-- VA benefit correspondence frequency, etc.

WITH base AS (
    SELECT * FROM {{ ref('stg_mail_documents') }}
),

sender_agg AS (
    SELECT
        sender
        , document_type
        , COUNT(*)                                  AS document_count
        , MIN(document_date)
            FILTER (WHERE document_date IS NOT NULL)
            AS first_seen
        , MAX(document_date)
            FILTER (WHERE document_date IS NOT NULL)
            AS last_seen
        , MAX(document_date)
            FILTER (WHERE document_date IS NOT NULL)
            = MAX(MAX(document_date)
                FILTER (WHERE document_date IS NOT NULL))
            OVER (PARTITION BY sender)              AS is_most_recent
        , COUNT(*)
            FILTER (WHERE action_required = TRUE)   AS action_count

        -- Spend rollup (financial document types only)
        , SUM(
            CASE
                WHEN document_type IN ('statement', 'utility', 'insurance')
                THEN (
                    SELECT SUM((amt->>'value')::numeric)
                    FROM jsonb_array_elements(dollar_amounts) AS amt
                )
                ELSE 0
            END
        )                                           AS total_billed

        -- Recency score for sorting
        , CURRENT_DATE
            - MAX(document_date)
                FILTER (WHERE document_date IS NOT NULL)
            AS days_since_last

    FROM base
    GROUP BY sender, document_type
)

SELECT
    sender
    , document_type
    , document_count
    , first_seen
    , last_seen
    , days_since_last
    , action_count
    , ROUND(total_billed, 2)                        AS total_billed

    -- Engagement flag: heard from them in last 90 days
    , days_since_last <= 90                         AS is_active_sender

FROM sender_agg
ORDER BY document_count DESC, last_seen DESC
