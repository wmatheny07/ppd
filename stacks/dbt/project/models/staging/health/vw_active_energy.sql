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
        , w._ab_source_file_last_modified AS file_last_modified
    FROM
        {{ source('health', 'Workouts') }} AS w,
        LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout,
        LATERAL JSONB_ARRAY_ELEMENTS(workout -> 'activeEnergy') AS energy_entry

)

, latest_per_reading AS (

    -- The same workout can appear in several exports. Keep the row from the newest file,
    -- preferring one with a source because newer exports can leave it blank.
    SELECT DISTINCT ON (id, person, date)
        id
        , person
        , qty
        , date
        , units
        , data_source
    FROM
        raw_energy
    ORDER BY
        id
        , person
        , date
        , (COALESCE(data_source, '') <> '') DESC
        , file_last_modified DESC

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
        latest_per_reading

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'with_moving_avg.id',
        'with_moving_avg.person',
        'with_moving_avg.date'
    ]) }} AS id
    , with_moving_avg.id AS workout_id
    , with_moving_avg.person
    , with_moving_avg.data_source
    , with_moving_avg.qty
    , with_moving_avg.date
    , CASE
        WHEN LAG(mv_avg) OVER (
            PARTITION BY with_moving_avg.id
            ORDER BY with_moving_avg.date
        ) IS NOT NULL THEN (
            LAG(mv_avg) OVER (
                PARTITION BY with_moving_avg.id
                ORDER BY with_moving_avg.date
            ) - mv_avg
        ) / NULLIF(LAG(mv_avg) OVER (
            PARTITION BY with_moving_avg.id
            ORDER BY with_moving_avg.date
        ), 0)
        ELSE NULL
    END AS perc_diff_prev
FROM
    with_moving_avg
{%- if is_incremental() %}
LEFT JOIN (
    SELECT person, MAX(date::TIMESTAMP) AS max_date
    FROM {{ this }}
    GROUP BY person
) AS watermark
    ON watermark.person = with_moving_avg.person
WHERE
    -- Per person, because exports arrive at different times. The lookback picks up late files.
    watermark.max_date IS NULL
    OR with_moving_avg.date::TIMESTAMP > watermark.max_date - INTERVAL '7 days'
{% endif %}
