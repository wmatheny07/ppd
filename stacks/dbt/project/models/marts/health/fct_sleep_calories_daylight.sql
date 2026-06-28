{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT
    fsc.*
    , ftids.time_in_daylight
FROM {{ ref('fct_sleep_calories') }} AS fsc
JOIN {{ ref('fct_time_in_daylight_summary') }} AS ftids
    ON fsc.record_date = ftids.record_date
    AND fsc.person = ftids.person
