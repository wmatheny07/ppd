{{ config(materialized='incremental', unique_key=['id', 'person']) }}

WITH
  diff_check AS (
    SELECT
      ves.id,
      ves.person,
      date
    FROM
      (
        SELECT
          id,
          person,
          MAX(perc_diff_prev) perc_diff
        FROM
          {{ ref('vw_active_energy') }}
        GROUP BY
          id, person
      ) diffs
      JOIN {{ ref('vw_active_energy') }} ves ON diffs.id = ves.id
      AND diffs.perc_diff = ves.perc_diff_prev
    WHERE
      ABS(diffs.perc_diff) > 0.15
  )
SELECT
  vae.id,
  vae.person,
  MIN(vae.date) start_date,
  MAX(vae.date) end_date,
  diff_check.date AS updated_end_date
FROM
  {{ ref('vw_active_energy') }} vae
  LEFT JOIN diff_check ON vae.id = diff_check.id
{% if is_incremental() %}
WHERE vae.date::timestamp > (SELECT MAX(end_date::timestamp) FROM {{ this }})
{% endif %}
GROUP BY
  vae.id,
  vae.person,
  diff_check.date
ORDER BY
  start_date DESC
