{{ config(materialized='incremental', unique_key=['record_date', 'person', 'workout_type', 'hr_zone']) }}

WITH
  constants AS (
    SELECT
      date ('12/29/1983') birth_date
  ),
  base_data AS (
    SELECT
      date,
      vhrd.person,
      vws.workout_type,
      ROUND(avg::numeric, 2) AS hr,
      EXTRACT(
        YEAR
        FROM
          age (date ("date"), birth_date)
      ) age_at_reading
    FROM
      {{ ref('vw_heart_rate_data') }} vhrd
      join {{ ref('fct_workout_summary') }} vws on
        vhrd."date"::timestamp between date_trunc('minute', workout_start::timestamp) and workout_end::timestamp
      CROSS JOIN constants
    {% if is_incremental() %}
    WHERE vhrd."date"::date > (SELECT MAX(record_date::date) FROM {{ this }})
    {% endif %}
  ),
  zones AS (
    SELECT
      date,
      hr,
      workout_type,
      age_at_reading,
      person,
      CASE
        WHEN hr < (220 - age_at_reading) * 0.6 THEN 'Recovery'
        WHEN hr < (220 - age_at_reading) * 0.7 THEN 'Fat Burn'
        WHEN hr < (220 - age_at_reading) * 0.8 THEN 'Cardio'
        WHEN hr < (220 - age_at_reading) * 0.9 THEN 'Threshold'
        WHEN hr >= (220 - age_at_reading) * 0.9 THEN 'Peak'
        ELSE 'Unknown'
      END AS hr_zone,
      DATE_PART(
        'minute',
        LEAD("date"::timestamp) OVER (
          ORDER BY
            "date"
        ) - "date"::timestamp
      )::decimal(10, 3) + DATE_PART(
        'seconds',
        LEAD("date"::timestamp) OVER (
          ORDER BY
            "date"
        ) - "date"::timestamp
      )::decimal(10, 3) / 60 duration_at_reading
    FROM
      base_data
  )
SELECT
  date ("date") record_date,
  person,
  workout_type,
  hr_zone,
  ROUND(SUM(duration_at_reading), 2) time_in_zone
FROM
  zones
GROUP BY
  record_date,
  workout_type,
  hr_zone,
  person
ORDER BY
  record_date, workout_type
