{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH raw_energy AS (

    SELECT
        workout ->> 'id' AS id
        , SPLIT_PART(w._ab_source_file_url, '/', 3) AS person
        , {{ convert_energy(
            "energy_entry ->> 'qty'",
            "energy_entry ->> 'units'"
        ) }} AS qty
        , energy_entry ->> 'date' AS date
        , energy_entry ->> 'units' AS units
        , energy_entry ->> 'source' AS data_source
    FROM
        {{ source('health', 'Workouts') }} AS w,
        LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout,
        LATERAL JSONB_ARRAY_ELEMENTS(workout -> 'activeEnergy') AS energy_entry

)

, with_moving_avg AS (

    SELECT
        id
        , person
        , data_source
        , qty
        , date
        , CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY date
            ) > 3 THEN AVG(qty::FLOAT) OVER (
                PARTITION BY id
                ORDER BY date
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS mv_avg
    FROM
        raw_energy

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'id',
        'person',
        'date'
    ]) }} AS id
    , with_moving_avg.id AS workout_id
    , person
    , data_source
    , qty
    , date
    , CASE
        WHEN LAG(mv_avg) OVER (
            PARTITION BY id
            ORDER BY date
        ) IS NOT NULL THEN (
            LAG(mv_avg) OVER (
                PARTITION BY id
                ORDER BY date
            ) - mv_avg
        ) / NULLIF(LAG(mv_avg) OVER (
            PARTITION BY id
            ORDER BY date
        ), 0)
        ELSE NULL
    END AS perc_diff_prev
FROM
    with_moving_avg
{%- if is_incremental() %}
WHERE
    date::TIMESTAMP > (SELECT MAX(date::TIMESTAMP) FROM {{ this }})
{% endif %}
