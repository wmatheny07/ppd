SELECT
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS id,
  split_part(_ab_source_file_url, '/', 3) person,
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergyBurned' ->> 'qty' AS qty,
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'activeEnergyBurned' ->> 'units' AS units
FROM
  {{ source('health', 'Workouts') }} w
