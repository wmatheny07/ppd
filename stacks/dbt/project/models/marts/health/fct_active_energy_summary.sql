{{ config(materialized='incremental', unique_key=['id', 'person', 'data_source']) }}

SELECT
  id,
  person,
  MIN(date::timestamp) start_time,
  MAX(date::timestamp) end_time,
  data_source,
  ROUND(SUM(qty::decimal(10, 3)), 2) total_calories
FROM
  {{ ref('vw_active_energy') }}
{% if is_incremental() %}
WHERE date::timestamp > (SELECT MAX(end_time) FROM {{ this }})
{% endif %}
GROUP BY
  id,
  person,
  data_source
