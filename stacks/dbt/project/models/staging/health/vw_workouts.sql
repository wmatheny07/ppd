{{ config(
    materialized='table',
    tags=['health', 'staging']
) }}

WITH raw_workouts AS MATERIALIZED (

    SELECT
        workout ->> 'id' AS id
        , SPLIT_PART(w._ab_source_file_url, '/', 3) AS person
        , workout ->> 'start' AS start_text
        , workout ->> 'end' AS end_text
        , workout ->> 'duration' AS duration
        , workout -> 'activeEnergyBurned' ->> 'qty' AS energy_qty
        , workout -> 'activeEnergyBurned' ->> 'units' AS energy_units
        , workout ->> 'name' AS workout_type
        , workout -> 'intensity' ->> 'qty' AS intensity
        , workout -> 'intensity' ->> 'units' AS intensity_units
        -- Only newer exports name the app that recorded the workout.
        , workout -> 'source' ->> 'name' AS source_app
        , w._ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'Workouts') }} AS w,
        LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout

)

-- A workout can appear in several exports, so keep the one from the newest file.
SELECT DISTINCT ON (id)
    id
    , person
    , start_text::TIMESTAMP AS start
    , end_text::TIMESTAMP AS "end"
    , ROUND(duration::DECIMAL(10, 3) / 60, 2) AS duration_mins
    , ROUND(
        {{ convert_energy("energy_qty", "energy_units") }},
        2
    ) AS total_calories
    , workout_type
    , intensity::DECIMAL(10, 3) AS intensity
    , intensity_units
    , source_app
FROM
    raw_workouts
ORDER BY
    id
    , file_last_modified DESC
