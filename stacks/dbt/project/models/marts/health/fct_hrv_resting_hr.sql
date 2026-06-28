{{- config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert',
    tags=['health']
) -}}

SELECT
    hr.record_date
    , hr.person
    , hr.resting_heart_rate
    , hrv.overnight_avg_hrv
FROM {{ ref('fct_resting_hr') }} AS hr
JOIN {{ ref('fct_hrv_daily') }} AS hrv
    ON hr.record_date = hrv.record_date
    AND hr.person = hrv.person
WHERE overnight_avg_hrv IS NOT NULL
{% if is_incremental() %}
    AND (
        hr.record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
        OR hr.record_date::date > current_date - INTERVAL '30 day'
    )
    -- reprocess last 30 days to capture updates
{% endif %}
