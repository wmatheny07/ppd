SELECT
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS id,
  split_part(_ab_source_file_url, '/', 3) person,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'walkingAndRunningDistance'
  ) ->> 'qty' AS qty,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'walkingAndRunningDistance'
  ) ->> 'date' AS date,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'walkingAndRunningDistance'
  ) ->> 'units' AS units
FROM
  {{ source('health', 'Workouts') }} w
