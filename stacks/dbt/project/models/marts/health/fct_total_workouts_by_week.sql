{{ config(
    materialized='view'
) }}

WITH deduped AS (

    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY
                person
                , DATE_TRUNC('week', workout_start)
                , start_nearest_30min
            ORDER BY workout_start, id
        ) AS rn
    FROM {{ ref('fct_workout_summary') }} AS vws
    WHERE LOWER(COALESCE(data_source, '')) <> 'ifit'
        OR (
            LOWER(COALESCE(data_source, '')) = 'ifit'
            AND NOT EXISTS (
                SELECT 1
                FROM {{ ref('fct_workout_summary') }} AS vws2
                WHERE vws2.start_nearest_30min = vws.start_nearest_30min
                    AND vws2.person = vws.person
                    AND LOWER(COALESCE(vws2.data_source, '')) <> 'ifit'
            )
        )

)

SELECT
    person
    , DATE_TRUNC('week', workout_start) AS workout_week
    , COUNT(DISTINCT id) AS total_workouts
FROM deduped
WHERE rn = 1
GROUP BY person, DATE_TRUNC('week', workout_start)
ORDER BY workout_week DESC, person
