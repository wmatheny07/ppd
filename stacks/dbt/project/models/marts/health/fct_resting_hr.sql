{{ config(
    materialized='incremental',
    unique_key=['id', 'person'],
    incremental_strategy='delete+insert'
) }}

SELECT
    MD5(CONCAT(record_date, person, resting_heart_rate)) AS id
    , record_date
    , person
    , resting_heart_rate
FROM {{ ref('vw_resting_hr_metrics') }}
{%- if is_incremental() %}
WHERE record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
    OR record_date::date > current_date - INTERVAL '30 day'
    -- reprocess last 30 days to capture updates
{%- endif %}
