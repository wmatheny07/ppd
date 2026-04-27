{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    tags=['health']
)}}

SELECT
  "date"::timestamp AS record_date,
  person,
  qty::float AS step_count,
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
  metric_name = 'step_count'
{%- if is_incremental() %}
  AND "date"::timestamp > (SELECT MAX(record_date::timestamp) FROM {{ this }})
  {% endif %}
