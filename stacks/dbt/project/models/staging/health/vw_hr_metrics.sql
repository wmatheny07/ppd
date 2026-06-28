WITH raw_metrics AS (

    SELECT DISTINCT
        JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Min' AS min
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Max' AS max
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Avg' AS avg
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'source' AS data_source
        , JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' AS metric_name
    FROM
        {{ source('health', 'metrics') }}

)

SELECT
    "date"::TIMESTAMP AS record_date
    , person
    , min::FLOAT AS min
    , max::FLOAT AS max
    , avg::FLOAT AS avg
    , data_source
    , metric_name
FROM
    raw_metrics
WHERE
    metric_name = 'heart_rate'
