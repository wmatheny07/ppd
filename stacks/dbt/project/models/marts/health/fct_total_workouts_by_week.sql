{{ config (
    materialized='view'
)}}

SELECT
  person,
  date_trunc('week', workout_start) workout_week,
  count(distinct id) total_workouts
FROM
(
SELECT
  *,
  row_number() over (partition by person, date_trunc('week', workout_start) order by start_nearest_30min desc, workout_start, id) rn
FROM
  {{ ref('fct_workout_summary') }} vws
WHERE
    lower(data_source) <> 'ifit'
    OR (
      lower(data_source) = 'ifit'
      AND NOT EXISTS(
        SELECT
          1
        FROM {{ ref('fct_workout_summary') }} AS vws2
        WHERE
          start_nearest_30min = vws.start_nearest_30min
          and vws2.person = vws.person
          and lower(vws2.data_source) <> 'ifit'
      )
    )
)
where rn = 1
group by person, date_trunc('week', workout_start)
order by workout_week desc, person