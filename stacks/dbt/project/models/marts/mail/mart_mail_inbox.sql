-- models/marts/mart_mail_inbox.sql
-- Actionable inbox: documents requiring follow-up, sorted by recency.
-- Powers the primary Superset dashboard view.

with classified as (
    select * from {{ ref('stg_mail_documents') }}
),

inbox as (
    select
        document_id,
        sender,
        document_type,
        document_date,
        summary,
        action_description,
        minio_key,

        -- Age buckets for dashboard filtering
        case
            when document_date >= current_date - interval '7 days'  then 'This Week'
            when document_date >= current_date - interval '30 days' then 'This Month'
            when document_date >= current_date - interval '90 days' then 'Last 90 Days'
            else 'Older'
        end                                         as age_bucket,

        -- Dollar summary (unnested from JSONB)
        (
            select sum((amt->>'value')::numeric)
            from jsonb_array_elements(dollar_amounts) as amt
        )                                           as total_amount,

        current_date - document_date                as days_old

    from classified
    where action_required = true
      and document_date is not null
),

ranked as (
    select
        *,
        row_number() over (order by document_date desc) as inbox_rank
    from inbox
)

select * from ranked
order by document_date desc
