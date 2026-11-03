{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    tags=['health']
) }}

WITH raw_metrics AS (

    SELECT
        reading ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , reading ->> 'qty' AS qty
        , reading ->> 'source' AS data_source
        , metric ->> 'name' AS metric_name
        , _ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'metrics') }},
        LATERAL JSONB_ARRAY_ELEMENTS(data -> 'metrics') AS metric,
        LATERAL JSONB_ARRAY_ELEMENTS(metric -> 'data') AS reading
    WHERE
        metric ->> 'name' = 'step_count'

)

, latest_per_minute AS (

    -- Exports hold one row per minute. Re-exports of the same minute can shift the
    -- seconds or blank the source, so keep the row from the newest file. Truncating
    -- with the offset keeps both 1am hours on the night the clocks go back.
    SELECT DISTINCT ON (person, DATE_TRUNC('minute', "date"::TIMESTAMPTZ))
        DATE_TRUNC('minute', "date"::TIMESTAMP) AS record_date
        , person
        , qty
        , data_source
        , metric_name
    FROM
        raw_metrics
    ORDER BY
        person
        , DATE_TRUNC('minute', "date"::TIMESTAMPTZ)
        , file_last_modified DESC

)

SELECT
    latest_per_minute.record_date
    , latest_per_minute.person
    , latest_per_minute.qty::FLOAT AS step_count
    , latest_per_minute.data_source
    , latest_per_minute.metric_name
FROM
    latest_per_minute
{%- if is_incremental() %}
LEFT JOIN (
    SELECT person, MAX(record_date) AS max_record_date
    FROM {{ this }}
    GROUP BY person
) AS watermark
    ON watermark.person = latest_per_minute.person
WHERE
    -- Per person, because exports arrive at different times. The lookback picks up late files.
    watermark.max_record_date IS NULL
    OR latest_per_minute.record_date > watermark.max_record_date - INTERVAL '7 days'
{% endif %}
