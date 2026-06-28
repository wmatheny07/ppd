{{ config(
    materialized='incremental',
    unique_key=['id', 'person', 'data_source'],
    incremental_strategy='delete+insert'
) }}

SELECT
    id
    , workout_id
    , person
    , MIN(date::timestamp) AS start_time
    , MAX(date::timestamp) AS end_time
    , data_source
    , ROUND(SUM(qty::decimal(10, 3)), 2) AS total_calories
FROM {{ ref('vw_active_energy') }}
{% if is_incremental() %}
WHERE date::timestamp > (SELECT MAX(end_time::timestamp) FROM {{ this }})
    OR date::date > current_date - INTERVAL '30 day'
    -- reprocess last 30 days to capture updates
{% endif %}
GROUP BY
    id
    , workout_id
    , person
    , data_source
