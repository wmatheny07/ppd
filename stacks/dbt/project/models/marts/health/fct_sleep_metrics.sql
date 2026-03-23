{{ config(materialized='incremental', unique_key=['record_date', 'person']) }}

SELECT
  record_date,
  person,
  rem,
  core,
  deep,
  awake,
  in_bed_start,
  sleep_start,
  in_bed_end,
  sleep_end,
  total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
  {% if is_incremental() %}
  WHERE record_date::timestamp > (SELECT MAX(record_date::timestamp) FROM {{ this }})
{% endif %}