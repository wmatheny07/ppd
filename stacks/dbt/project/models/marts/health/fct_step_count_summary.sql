{{ config (
    materialized='incremental',
    unique_key=['record_date', 'person'],
    tags=['health']
)}}

SELECT
    date(record_date) record_date,
    person,
    SUM(step_count) AS step_count  -- some sources send multiple records per day
FROM {{ ref('vw_step_count_metrics') }}
WHERE step_count > 0
{% if is_incremental() %}
  AND record_date::date >= (SELECT MAX(record_date::date) FROM {{ this }})
{% endif %}
GROUP BY date(record_date), person
ORDER BY date(record_date), person