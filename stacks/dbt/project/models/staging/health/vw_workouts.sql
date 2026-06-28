{{ config(
    materialized='table',
    tags=['health', 'staging']
) }}

SELECT DISTINCT
    workout ->> 'id' AS id
    , SPLIT_PART(w._ab_source_file_url, '/', 3) AS person
    , (workout ->> 'start')::TIMESTAMP AS start
    , (workout ->> 'end')::TIMESTAMP AS "end"
    , ROUND((workout ->> 'duration')::DECIMAL(10, 3) / 60, 2) AS duration_mins
    , ROUND(
        {{ convert_energy(
            "workout -> 'activeEnergyBurned' ->> 'qty'",
            "workout -> 'activeEnergyBurned' ->> 'units'"
        ) }},
        2
    ) AS total_calories
    , workout ->> 'name' AS workout_type
    , (workout -> 'intensity' ->> 'qty')::DECIMAL(10, 3) AS intensity
    , workout -> 'intensity' ->> 'units' AS intensity_units
FROM
    {{ source('health', 'Workouts') }} AS w,
    LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout
