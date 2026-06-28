{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert'
) }}

WITH cals_burned AS (

    SELECT
        DATE(workout_start) AS record_date
        , SUM(total_calories) AS calories_burned
    FROM {{ ref('fct_workout_summary') }}
    GROUP BY DATE(workout_start)

)

SELECT
    vsm.record_date
    , person
    , total_sleep
    , awake
    , calories_burned
FROM {{ ref('vw_sleep_metrics') }} AS vsm
JOIN cals_burned
    ON vsm.record_date = cals_burned.record_date
{%- if is_incremental() %}
WHERE vsm.record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
    OR vsm.record_date::date > current_date - INTERVAL '30 day'
    -- reprocess last 30 days to capture updates
{%- endif %}
