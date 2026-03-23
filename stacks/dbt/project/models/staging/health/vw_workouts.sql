SELECT
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS id,
  split_part(_ab_source_file_url, '/', 3) person,
  (
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'start'
  )::timestamp AS start,
  (
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'end'
  )::timestamp AS END,
  ROUND(
    (
      JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'duration'
    )::decimal(10, 3) / 60,
    2
  ) AS duration_mins,
  ROUND(
    (
      JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergyBurned' ->> 'qty'
    )::decimal(10, 3),
    2
  ) AS total_calories,
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'name' AS workout_type,
  (
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'intensity' ->> 'qty'
  )::decimal(10, 3) AS intensity,
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'intensity' ->> 'units' AS intensity_units
FROM
  {{ source('health', 'Workouts') }} w
