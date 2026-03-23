SELECT
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS id,
  split_part(_ab_source_file_url, '/', 3) person,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'heartRateRecovery'
  ) ->> 'Avg' AS avg,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'heartRateRecovery'
  ) ->> 'Max' AS max,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'heartRateRecovery'
  ) ->> 'Min' AS min,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'heartRateRecovery'
  ) ->> 'date' AS date,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'heartRateRecovery'
  ) ->> 'units' AS units
FROM
  {{ source('health', 'Workouts') }} w
