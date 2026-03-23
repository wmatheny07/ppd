SELECT
  JSONB_ARRAY_ELEMENTS(data -> 'workouts') ->> 'id' AS rt_id,
  split_part(_ab_source_file_url, '/', 3) person,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'route'
  ) ->> 'speed' AS rt_speed,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'route'
  ) ->> 'altitude' AS rt_altitude,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'route'
  ) ->> 'latitude' AS rt_latitude,
  JSONB_ARRAY_ELEMENTS(
    JSONB_ARRAY_ELEMENTS(data -> 'workouts') -> 'route'
  ) ->> 'longitude' AS rt_longitude
FROM
  {{ source('health', 'Workouts') }} w
