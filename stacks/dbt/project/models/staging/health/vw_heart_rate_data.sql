SELECT
  *
FROM
  (
    SELECT DISTINCT
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' "date",
      split_part(_ab_source_file_url, '/', 3) person,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Min' min,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Max' max,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'Avg' avg,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'source' data_source,
      JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' metric_name
    FROM
      {{ source('health', 'metrics') }}
  )
WHERE
  metric_name = 'heart_rate'
