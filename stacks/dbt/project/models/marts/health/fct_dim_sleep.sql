{{ config(materialized='incremental', unique_key=['record_date', 'person', 'sleep_type']) }}
{% set sleep_types = ['rem', 'core', 'deep', 'awake'] %}

{% for sleep_type in sleep_types %}
SELECT
  record_date,
  person,
  '{{ sleep_type }}' sleep_type,
  {{ sleep_type }} AS sleep_duration,
  total_sleep AS total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
WHERE
  total_sleep > 0
  {%- if is_incremental() %}
  AND record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
  {% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
