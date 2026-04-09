{{ config(materialized='incremental', unique_key=['person','workout_type','workout_start','workout_end']) }}

SELECT
  id,
  person,
  CASE
    WHEN workout_type = 'Run'
    AND calories_per_minute < 13 THEN 'Hiking'
    ELSE workout_type
  END AS workout_type,
  data_source,
  workout_start,
  start_nearest_30min,
  workout_end,
  total_calories,
  duration_mins,
  calories_per_minute,
  intensity
FROM
  (
    SELECT
      id,
      person,
      workout_type,
      data_source,
      start_time workout_start,
      (
        DATE_TRUNC('hour', start_time) + interval '30 min' * ROUND(
          EXTRACT(
            MINUTE
            FROM
              start_time
          )::integer / 30.0
        )
      ) AS start_nearest_30min,
      end_time workout_end,
      total_calories,
      duration_mins,
      total_calories / duration_mins calories_per_minute,
      intensity
    FROM
      (
        SELECT DISTINCT
          ON 
          ( {{ dbt_utils.generate_surrogate_key(['vw.person', 'vw.workout_type', 'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')']) }} )
          {{ dbt_utils.generate_surrogate_key(['vw.person', 'vw.workout_type', 'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')']) }} id,
          vw.person,
          vw.workout_type,
          vw.start AS start_time,
          CASE
            WHEN wta.updated_end_date IS NOT NULL THEN wta.updated_end_date::timestamp
            ELSE vw.end
          END AS end_time,
          CASE
            WHEN LOWER(data_source) LIKE '%ifit%' THEN ves.total_calories
            ELSE vw.total_calories
          END AS total_calories,
          CASE
            WHEN wta.updated_end_date IS NOT NULL THEN EXTRACT(
              EPOCH
              FROM
                (wta.updated_end_date::timestamp - vw.start)
            ) / 60
            ELSE vw.duration_mins
          END AS duration_mins,
          vw.intensity AS intensity,
          CASE
            WHEN LOWER(ves.data_source) LIKE '%ifit%' THEN 'iFIT'
            WHEN ves.data_source LIKE '%Apple Watch%' THEN 'Apple Watch'
            ELSE ves.data_source
          END AS data_source
        FROM
          {{ ref('vw_workouts') }} vw
          LEFT JOIN {{ ref('fct_workout_time_adj') }} wta ON vw.id = wta.id
          AND wta.updated_end_date IS NOT NULL
          LEFT JOIN {{ ref('fct_active_energy_summary') }} ves ON vw.id = ves.id
        WHERE
          {% if is_incremental() %}
          vw.start::timestamp > (SELECT MAX(workout_start::timestamp) FROM {{ this }})
          {% else %}
          1 = 1
          {% endif %}
        ORDER BY
          {{ dbt_utils.generate_surrogate_key(['vw.person', 'vw.workout_type', 'to_char(vw.start, \'MM/DD/YYYY HH:MI:SS\')']) }}
      ) summary
  )
ORDER BY
  workout_start DESC
