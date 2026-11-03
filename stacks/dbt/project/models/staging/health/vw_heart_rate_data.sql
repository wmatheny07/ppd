{{ config(
    materialized='incremental',
    unique_key=['person', 'record_minute'],
    incremental_strategy='delete+insert'
) }}

-- MATERIALIZED pulls these fields out first. Otherwise the sort below can carry each
-- row's whole JSON array, which used over 350 GB of temp space in one rebuild.
WITH raw_metrics AS MATERIALIZED (

    SELECT
        reading ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , reading ->> 'Min' AS min
        , reading ->> 'Max' AS max
        , reading ->> 'Avg' AS avg
        , reading ->> 'source' AS data_source
        , metric ->> 'name' AS metric_name
        , _ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'metrics') }},
        LATERAL JSONB_ARRAY_ELEMENTS(data -> 'metrics') AS metric,
        LATERAL JSONB_ARRAY_ELEMENTS(metric -> 'data') AS reading
    WHERE
        metric ->> 'name' = 'heart_rate'

)

, ranked_by_file AS (

    -- Heart rate has several real readings per minute, and a re-export can shift their
    -- seconds. So each minute comes only from the newest file that contains it.
    SELECT
        *
        , DATE_TRUNC('minute', "date"::TIMESTAMPTZ) AS record_minute
        , RANK() OVER (
            PARTITION BY person, DATE_TRUNC('minute', "date"::TIMESTAMPTZ)
            ORDER BY file_last_modified DESC
        ) AS file_rank
    FROM
        raw_metrics

)

, latest_per_reading AS (

    SELECT DISTINCT ON (person, "date")
        "date"
        , record_minute
        , person
        , min
        , max
        , avg
        , data_source
        , metric_name
    FROM
        ranked_by_file
    WHERE
        file_rank = 1
    ORDER BY
        person
        , "date"

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'latest_per_reading.date',
        'latest_per_reading.person'
    ]) }} AS id
    , latest_per_reading."date"
    , latest_per_reading.record_minute
    , latest_per_reading.person
    , latest_per_reading.min
    , latest_per_reading.max
    , latest_per_reading.avg
    , latest_per_reading.data_source
    , latest_per_reading.metric_name
FROM
    latest_per_reading
{%- if is_incremental() %}
LEFT JOIN (
    SELECT person, MAX(record_minute) AS max_minute
    FROM {{ this }}
    GROUP BY person
) AS watermark
    ON watermark.person = latest_per_reading.person
WHERE
    -- Per person, because exports arrive at different times. The lookback picks up late files.
    watermark.max_minute IS NULL
    OR latest_per_reading.record_minute > watermark.max_minute - INTERVAL '7 days'
{% endif %}
