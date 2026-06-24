{{ config(
    materialized='incremental', 
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert'
)}}

SELECT
    vsm.record_date,
    person,
    total_sleep,
    awake,
    calories_burned
FROM
    {{ ref('vw_sleep_metrics') }} vsm
    JOIN (
        SELECT
            date (workout_start) record_date,
            SUM(total_calories) calories_burned
        FROM
            {{ ref('fct_workout_summary') }}
        GROUP BY
            date (workout_start)
    ) cals_burned ON vsm.record_date = cals_burned.record_date
{%- if is_incremental() %}
WHERE vsm.record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
or vsm.record_date::date > current_date - interval '30 day' -- reprocess last 30 days to capture updates
{%- endif %}