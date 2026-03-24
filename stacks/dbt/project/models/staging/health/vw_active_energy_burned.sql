SELECT
  workout ->> 'id' AS id,
  split_part(w._ab_source_file_url, '/', 3) AS person,
  {{ convert_energy(
    "workout -> 'activeEnergyBurned' ->> 'qty'",
    "workout -> 'activeEnergyBurned' ->> 'units'"
  ) }} AS qty,
  workout -> 'activeEnergyBurned' ->> 'units' AS units
FROM
  {{ source('health', 'Workouts') }} w,
  LATERAL JSONB_ARRAY_ELEMENTS(w.data -> 'workouts') AS workout
