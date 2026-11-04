{{ config(
    materialized='table',
    tags=['health']
) }}

-- Built as a table because a workout can stop being the kept copy when a later export
-- arrives, and an incremental run would leave that older row behind.

WITH energy_by_workout AS (

    -- The summary has one row per reading, so total them per workout.
    SELECT
        workout_id
        , SUM(total_calories) AS total_calories
        , COALESCE(
            MAX(data_source) FILTER (WHERE LOWER(data_source) LIKE '%ifit%')
            , MAX(NULLIF(data_source, ''))
            , ''
        ) AS data_source
    FROM {{ ref('fct_active_energy_summary') }}
    GROUP BY workout_id

)

, ranked_workouts AS (

    -- One treadmill or strength session is often recorded by both iFIT and the Apple
    -- Watch. Lower ranks are preferred. Older exports don't name the app and iFIT merges
    -- the energy sources, so those are identified another way: where the app is known,
    -- only the Watch records intensity, and only iFIT logs these sessions as 'Run'.
    SELECT
        vw.*
        , ves.total_calories AS energy_calories
        , ves.data_source AS energy_source
        , CASE
            WHEN vw.source_app LIKE '%Watch%' THEN 1
            WHEN vw.source_app ILIKE '%ifit%' OR vw.workout_type = 'Run' THEN 4
            WHEN vw.source_app IS NOT NULL THEN 2
            WHEN vw.intensity IS NOT NULL THEN 1
            WHEN ves.data_source LIKE '%Watch%' AND ves.data_source NOT ILIKE '%ifit%' THEN 1
            ELSE 3
        END AS source_rank
    FROM {{ ref('vw_workouts') }} AS vw
    LEFT JOIN energy_by_workout AS ves
        ON vw.id = ves.workout_id

)

, kept_workouts AS (

    -- Drop a Run/iFIT or unidentified record when a preferred record covers the same
    -- session, meaning they overlap for at least half of the shorter one. Two records of
    -- the same rank only count as one session if they also start within 5 minutes.
    SELECT r.*
    FROM ranked_workouts AS r
    WHERE NOT EXISTS (
        SELECT 1
        FROM ranked_workouts AS other
        WHERE other.person = r.person
            AND other.id <> r.id
            AND r.source_rank >= 3
            AND other.start < r."end"
            AND r.start < other."end"
            AND EXTRACT(EPOCH FROM LEAST(r."end", other."end") - GREATEST(r.start, other.start))
                >= 0.5 * LEAST(
                    EXTRACT(EPOCH FROM r."end" - r.start)
                    , EXTRACT(EPOCH FROM other."end" - other.start)
                )
            AND (
                other.source_rank < r.source_rank
                OR (
                    other.source_rank = r.source_rank
                    AND ABS(EXTRACT(EPOCH FROM other.start - r.start)) <= 300
                    AND (other.duration_mins, other.id) > (r.duration_mins, r.id)
                )
            )
    )

)

, deduped_workouts AS (

    SELECT DISTINCT ON (
        {{ dbt_utils.generate_surrogate_key([
            'vw.person',
            'vw.workout_type',
            'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')'
        ]) }}
    )
        {{ dbt_utils.generate_surrogate_key([
            'vw.person',
            'vw.workout_type',
            'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')'
        ]) }} AS id
        , vw.person
        , vw.workout_type
        , vw.start AS start_time
        , vw."end" AS end_time
        , CASE
            WHEN vw.source_rank >= 3 AND LOWER(vw.energy_source) LIKE '%ifit%'
                THEN vw.energy_calories
            ELSE vw.total_calories
        END AS total_calories
        , vw.duration_mins
        , vw.intensity
        , CASE
            WHEN vw.source_rank = 1 THEN 'Apple Watch'
            WHEN vw.source_rank = 4 OR LOWER(vw.energy_source) LIKE '%ifit%' THEN 'iFIT'
            ELSE COALESCE(vw.source_app, vw.energy_source)
        END AS data_source
    FROM kept_workouts AS vw
    ORDER BY
        {{ dbt_utils.generate_surrogate_key([
            'vw.person',
            'vw.workout_type',
            'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')'
        ]) }}
        , vw.source_rank
        , vw.id

)

, workout_calcs AS (

    SELECT
        id
        , person
        , workout_type
        , data_source
        , start_time AS workout_start
        , (
            DATE_TRUNC('hour', start_time)
            + INTERVAL '30 min' * ROUND(
                EXTRACT(MINUTE FROM start_time)::integer / 30.0
            )
        ) AS start_nearest_30min
        , end_time AS workout_end
        , total_calories
        , duration_mins
        , total_calories / NULLIF(duration_mins, 0) AS calories_per_minute
        , intensity
    FROM deduped_workouts

)

SELECT
    id
    , person
    , CASE
        WHEN workout_type = 'Run'
            AND calories_per_minute < 13
            THEN 'Hiking'
        ELSE workout_type
    END AS workout_type
    , data_source
    , workout_start
    , start_nearest_30min
    , workout_end
    , total_calories
    , duration_mins
    , calories_per_minute
    , intensity
FROM workout_calcs
ORDER BY workout_start DESC
