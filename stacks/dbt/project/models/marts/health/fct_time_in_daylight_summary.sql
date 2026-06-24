{{ config (
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_starategy='delete+insert',
    tags=['health']
)}}

SELECT
    date(record_date) record_date,
    person,
    SUM(time_in_daylight) AS time_in_daylight  -- some sources send multiple records per day
FROM {{ ref('vw_time_in_daylight') }}
WHERE time_in_daylight > 0
{% if is_incremental() %}
  AND
  ( record_date::date >= (SELECT MAX(record_date::date) FROM {{ this }})
  or record_date::date > current_date - interval '30 day') -- reprocess last 30 days to capture updates
{% endif %}
GROUP BY date(record_date), person
ORDER BY date(record_date), person