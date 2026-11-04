{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person', 'workout_type', 'hr_zone'],
    incremental_strategy='delete+insert'
) }}

WITH birth_dates AS (

    SELECT *
    FROM (
        VALUES
            ('wes', DATE '1983-12-29')
            , ('amanda', DATE '1989-03-12')
    ) AS people (person, birth_date)

)

, base_data AS (

    SELECT
        date
        , vhrd."date"::timestamp AS reading_ts
        , vhrd.person
        , vws.id AS workout_id
        , vws.workout_end
        , vws.workout_type
        , ROUND(avg::numeric, 2) AS hr
        , EXTRACT(
            YEAR FROM AGE(DATE("date"), birth_date)
        ) AS age_at_reading
    FROM {{ ref('vw_heart_rate_data') }} AS vhrd
    JOIN {{ ref('fct_workout_summary') }} AS vws
        ON vhrd.person = vws.person
        AND vhrd."date"::timestamp
            BETWEEN DATE_TRUNC('minute', workout_start::timestamp)
            AND workout_end::timestamp
    JOIN birth_dates
        ON vhrd.person = birth_dates.person
    {% if is_incremental() %}
    WHERE vhrd."date"::date > {{ earliest_person_watermark('record_date::date') }}
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
        -- Each reading lasts until the next one in the same workout; the last until it ends.
        , (
            EXTRACT(EPOCH FROM COALESCE(
                LEAD(reading_ts) OVER (
                    PARTITION BY person, workout_id
                    ORDER BY reading_ts
                ),
                workout_end
            ) - reading_ts) / 60
        )::decimal(10, 3) AS duration_at_reading
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
