{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert'
) }}

WITH cals_burned AS (

    SELECT
        DATE(workout_start) AS record_date
        , person
        , SUM(total_calories) AS calories_burned
    FROM {{ ref('fct_workout_summary') }}
    GROUP BY DATE(workout_start), person

)

SELECT
    vsm.record_date
    , vsm.person
    , total_sleep
    , awake
    , coalesce(calories_burned, 0) AS calories_burned
FROM {{ ref('vw_sleep_metrics') }} AS vsm
LEFT JOIN cals_burned
    ON vsm.record_date = cals_burned.record_date
    AND vsm.person = cals_burned.person
{%- if is_incremental() %}
WHERE vsm.record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
    OR vsm.record_date::date > current_date - INTERVAL '30 day'
    -- reprocess last 30 days to capture updates
{%- endif %}
