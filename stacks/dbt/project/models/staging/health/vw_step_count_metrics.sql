{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    tags=['health']
) }}

WITH raw_metrics AS (

    SELECT DISTINCT
        JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'qty' AS qty
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'source' AS data_source
        , JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' AS metric_name
    FROM
        {{ source('health', 'metrics') }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , qty::FLOAT AS step_count
    , data_source
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'step_count'
    {%- if is_incremental() %}
        AND "date"::TIMESTAMP > (SELECT MAX(record_date::TIMESTAMP) FROM {{ this }})
    {% endif %}
