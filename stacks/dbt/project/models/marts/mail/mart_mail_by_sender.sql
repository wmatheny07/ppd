-- models/marts/mart_mail_by_sender.sql
-- Sender-level rollup: frequency, recency, total spend from financial mail.
-- Useful for spotting subscription creep, insurance EOB patterns,
-- VA benefit correspondence frequency, etc.

with base as (
    select * from {{ ref('stg_mail_documents') }}
),

sender_agg as (
    select
        sender,
        document_type,
        count(*)                                            as document_count,
        min(document_date) filter (where document_date is not null) as first_seen,
        max(document_date) filter (where document_date is not null) as last_seen,
        max(document_date) filter (where document_date is not null)
            = max(max(document_date) filter (where document_date is not null))
            over (partition by sender)                      as is_most_recent,
        count(*) filter (where action_required = true)      as action_count,

        -- Spend rollup (financial document types only)
        sum(
            case when document_type in ('statement', 'utility', 'insurance')
            then (
                select sum((amt->>'value')::numeric)
                from jsonb_array_elements(dollar_amounts) as amt
            )
            else 0 end
        )                                                   as total_billed,

        -- Recency score for sorting (null when no dated documents exist for this sender)
        current_date - max(document_date) filter (where document_date is not null) as days_since_last

    from base
    group by sender, document_type
)

select
    sender,
    document_type,
    document_count,
    first_seen,
    last_seen,
    days_since_last,
    action_count,
    round(total_billed, 2)                                  as total_billed,

    -- Engagement flag: heard from them in last 90 days
    days_since_last <= 90                                   as is_active_sender

from sender_agg
order by document_count desc, last_seen desc
