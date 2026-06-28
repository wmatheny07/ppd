{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person']
) }}

WITH raw_metrics AS (

    SELECT DISTINCT
        JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' AS "date"
        , SPLIT_PART(_ab_source_file_url, '/', 3) AS person
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'rem' AS rem
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'core' AS core
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'deep' AS deep
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'awake' AS awake
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'inBedStart' AS in_bed_start
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'sleepStart' AS sleep_start
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'inBedEnd' AS in_bed_end
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'sleepEnd' AS sleep_end
        , JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' AS metric_name
    FROM
        {{ source('health', 'metrics') }}

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'date',
        'person'
    ]) }} AS id
    , "date"::DATE AS record_date
    , person
    , rem::FLOAT AS rem
    , core::FLOAT AS core
    , deep::FLOAT AS deep
    , awake::FLOAT AS awake
    , in_bed_start::TIMESTAMP AS in_bed_start
    , sleep_start::TIMESTAMP AS sleep_start
    , in_bed_end::TIMESTAMP AS in_bed_end
    , sleep_end::TIMESTAMP AS sleep_end
    , COALESCE(rem::FLOAT, 0)
        + COALESCE(core::FLOAT, 0)
        + COALESCE(deep::FLOAT, 0)
        + COALESCE(awake::FLOAT, 0) AS total_sleep
FROM
    raw_metrics
WHERE
    metric_name = 'sleep_analysis'
    {% if is_incremental() %}
        AND {{ dbt_utils.generate_surrogate_key([
            'date',
            'person'
        ]) }} NOT IN (SELECT id FROM {{ this }})
    {% endif %}
ORDER BY
    "date"
