{{ config(materialized='incremental', unique_key=['record_date', 'person', 'sleep_type']) }}

SELECT
  record_date,
  person,
  'rem' sleep_type,
  rem AS sleep_duration,
  total_sleep AS total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
WHERE
  total_sleep > 0
  {% if is_incremental() %}
  AND record_date > (SELECT MAX(record_date) FROM {{ this }})
  {% endif %}
UNION ALL
SELECT
  record_date,
  person,
  'core' sleep_type,
  core AS sleep_duration,
  total_sleep AS total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
WHERE
  total_sleep > 0
  {% if is_incremental() %}
  AND record_date > (SELECT MAX(record_date) FROM {{ this }})
  {% endif %}
UNION ALL
SELECT
  record_date,
  person,
  'deep' sleep_type,
  deep AS sleep_duration,
  total_sleep AS total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
WHERE
  total_sleep > 0
  {% if is_incremental() %}
  AND record_date > (SELECT MAX(record_date) FROM {{ this }})
  {% endif %}
UNION ALL
SELECT
  record_date,
  person,
  'awake' sleep_type,
  awake AS sleep_duration,
  total_sleep AS total_sleep
FROM
  {{ ref('vw_sleep_metrics') }}
WHERE
  total_sleep > 0
  {% if is_incremental() %}
  AND record_date > (SELECT MAX(record_date) FROM {{ this }})
  {% endif %}
