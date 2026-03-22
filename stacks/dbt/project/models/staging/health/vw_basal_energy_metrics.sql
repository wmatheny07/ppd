SELECT
  "date"::timestamp AS record_date,
  person,
  qty::float AS basal_energy_burned,
  data_source,
  metric_name
FROM
  (
    SELECT DISTINCT
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'date' "date",
      split_part(_ab_source_file_url, '/', 3) person,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'qty' qty,
      JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(data -> 'metrics') -> 'data') ->> 'source' data_source,
      JSONB_ARRAY_ELEMENTS(data -> 'metrics') ->> 'name' metric_name
    FROM
      {{ source('health', 'metrics') }}
  )
WHERE
  metric_name = 'basal_energy_burned'
