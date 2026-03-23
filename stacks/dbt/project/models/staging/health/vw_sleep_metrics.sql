SELECT
  "date"::date AS record_date,
  person,
  rem::float AS rem,
  core::float AS core,
  deep::float AS deep,
  awake::float AS awake,
  in_bed_start::timestamp AS in_bed_start,
  sleep_start::timestamp AS sleep_start,
  in_bed_end::timestamp AS in_bed_end,
  sleep_end::timestamp AS sleep_end,
  COALESCE(rem::float, 0) + COALESCE(core::float, 0) + COALESCE(deep::float, 0) + COALESCE(awake::float, 0) AS total_sleep
FROM
  (
    SELECT DISTINCT
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' AS date,
      split_part(_ab_source_file_url, '/', 3) person,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'rem' AS rem,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'core' AS core,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'deep' AS deep,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'awake' AS awake,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'inBedStart' AS in_bed_start,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'sleepStart' AS sleep_start,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'inBedEnd' AS in_bed_end,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'sleepEnd' AS sleep_end,
      JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' AS metric_name
    FROM
      {{ source('health', 'metrics') }}
  )
WHERE
  metric_name = 'sleep_analysis'
ORDER BY
  "date"
