WITH raw_metrics AS (

    SELECT DISTINCT
        JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'qty' AS qty
        , JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' AS metric_name
    FROM
        {{ source('health', 'metrics') }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , qty::FLOAT AS resting_heart_rate
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'resting_heart_rate'
