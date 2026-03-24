SELECT
  workout ->> 'id' AS id,
  split_part(w._ab_source_file_url, '/', 3) AS person,
  (workout ->> 'start')::timestamp AS start,
  (workout ->> 'end')::timestamp AS END,
  ROUND((workout ->> 'duration')::decimal(10, 3) / 60, 2) AS duration_mins,
  ROUND(
    {{ convert_energy(
      "workout -> 'activeEnergyBurned' ->> 'qty'",
      "workout -> 'activeEnergyBurned' ->> 'units'"
    ) }},
    2
  ) AS total_calories,
  workout ->> 'name' AS workout_type,
  (workout -> 'intensity' ->> 'qty')::decimal(10, 3) AS intensity,
  workout -> 'intensity' ->> 'units' AS intensity_units
FROM
  {{ source('health', 'Workouts') }} w,
  LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout
