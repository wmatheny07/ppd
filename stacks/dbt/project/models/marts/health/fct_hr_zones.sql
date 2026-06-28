{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person', 'workout_type', 'hr_zone'],
    incremental_strategy='delete+insert'
) }}

WITH constants AS (

    SELECT
        DATE('12/29/1983') AS birth_date

)

, base_data AS (

    SELECT
        date
        , vhrd.person
        , vws.workout_type
        , ROUND(avg::numeric, 2) AS hr
        , EXTRACT(
            YEAR FROM AGE(DATE("date"), birth_date)
        ) AS age_at_reading
    FROM {{ ref('vw_heart_rate_data') }} AS vhrd
    JOIN {{ ref('fct_workout_summary') }} AS vws
        ON vhrd."date"::timestamp
            BETWEEN DATE_TRUNC('minute', workout_start::timestamp)
            AND workout_end::timestamp
    CROSS JOIN constants
    {% if is_incremental() %}
    WHERE vhrd."date"::date > (SELECT MAX(record_date::date) FROM {{ this }})
        OR vhrd."date"::date > current_date - INTERVAL '30 day'
        -- reprocess last 30 days to capture updates
    {% endif %}

)

, zones AS (

    SELECT
        date
        , hr
        , workout_type
        , age_at_reading
        , person
        , CASE
            WHEN hr < (220 - age_at_reading) * 0.6 THEN 'Recovery'
            WHEN hr < (220 - age_at_reading) * 0.7 THEN 'Fat Burn'
            WHEN hr < (220 - age_at_reading) * 0.8 THEN 'Cardio'
            WHEN hr < (220 - age_at_reading) * 0.9 THEN 'Threshold'
            WHEN hr >= (220 - age_at_reading) * 0.9 THEN 'Peak'
            ELSE 'Unknown'
        END AS hr_zone
        , DATE_PART(
            'minute',
            LEAD("date"::timestamp) OVER (
                ORDER BY "date"
            ) - "date"::timestamp
        )::decimal(10, 3) + DATE_PART(
            'seconds',
            LEAD("date"::timestamp) OVER (
                ORDER BY "date"
            ) - "date"::timestamp
        )::decimal(10, 3) / 60 AS duration_at_reading
    FROM base_data

)

SELECT
    DATE("date") AS record_date
    , person
    , workout_type
    , hr_zone
    , ROUND(SUM(duration_at_reading), 2) AS time_in_zone
FROM zones
GROUP BY
    record_date
    , workout_type
    , hr_zone
    , person
ORDER BY
    record_date
    , workout_type
