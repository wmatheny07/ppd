{{- config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    tags=['health']
) -}}

SELECT
  hr.record_date,
  hr.person,
  hr.resting_heart_rate,
  hrv.overnight_avg_hrv
FROM
  {{ ref('fct_resting_hr') }} as hr
  join {{ ref('fct_hrv_daily') }} as hrv on
    hr.record_date = hrv.record_date
    and hr.person = hrv.person
WHERE overnight_avg_hrv is not null
{% if is_incremental() %}
  AND hr.record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
{% endif %}