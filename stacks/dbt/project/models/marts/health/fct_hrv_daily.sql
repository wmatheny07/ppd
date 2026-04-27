{{ config(materialized='incremental', unique_key=['id', 'person']) }}

WITH
  hrv_raw AS (
    -- Pull individual HRV readings from staging.
    -- Apply sanity bounds: SDNN (the unit Apple Watch reports) is physiologically
    -- implausible below 1ms or above 300ms. Filtering these avoids outliers from
    -- corrupt exports skewing daily averages.
    SELECT
      record_date::date AS record_date,
      record_date       AS reading_ts,
      person,
      heart_rate_variability
    FROM {{ ref('vw_hr_variability_metrics') }}
    WHERE heart_rate_variability BETWEEN 1 AND 300
    {% if is_incremental() %}
    AND record_date::date > (SELECT MAX(record_date) FROM {{ this }})
    {% endif %}
  ),

  sleep_windows AS (
    -- Exact sleep start/end timestamps for each person-night.
    -- Used below to classify HRV readings by where they fall relative to sleep.
    -- Apple Health associates sleep sessions with the wake date, so a session
    -- from Jan 5 23:00 → Jan 6 07:00 has record_date = Jan 6.
    SELECT
      person,
      sleep_start,
      sleep_end
    FROM {{ ref('fct_sleep_metrics') }}
    WHERE sleep_start IS NOT NULL
      AND sleep_end   IS NOT NULL
  ),

  hrv_classified AS (
    -- Assign each reading to one of three windows:
    --
    --   overnight — reading falls within the sleep session itself.
    --               Apple Watch takes background readings ~every 5 min during sleep,
    --               making this window the most data-dense.
    --
    --   morning   — reading falls within 2 hours after sleep_end (wake time).
    --               This is the most clinically meaningful window for recovery
    --               tracking: the autonomic nervous system is still in a low-stress
    --               state, so HRV reflects true recovery quality rather than
    --               activity-induced noise.
    --
    --   daytime   — everything else. Still kept for completeness; useful for
    --               spotting acute stress events mid-day.
    --
    -- The LATERAL join finds the one sleep session whose window brackets this
    -- reading (either within it, or within 2h after it ends). Using LATERAL +
    -- LIMIT 1 avoids duplicate rows if two sleep sessions somehow overlap.
    SELECT
      h.record_date,
      h.person,
      h.heart_rate_variability,
      CASE
        WHEN s.sleep_start IS NULL       THEN 'daytime'   -- no matching sleep session found
        WHEN h.reading_ts <= s.sleep_end THEN 'overnight'
        ELSE                                  'morning'
      END AS reading_window
    FROM hrv_raw h
    LEFT JOIN LATERAL (
      SELECT sleep_start, sleep_end
      FROM sleep_windows sw
      WHERE sw.person    = h.person
        AND h.reading_ts BETWEEN sw.sleep_start
                             AND sw.sleep_end + INTERVAL '2 hours'
      ORDER BY sw.sleep_end ASC   -- closest session if readings are near a boundary
      LIMIT 1
    ) s ON true
  )

SELECT
  {{ dbt_utils.generate_surrogate_key(['record_date', 'person']) }} AS id,
  record_date,
  person,

  -- ── All-day statistics ──────────────────────────────────────────────────
  -- Aggregate across every reading regardless of window. Use these for
  -- long-term trend lines where day-to-day noise is smoothed by a rolling
  -- average in your BI layer.
  ROUND(AVG(heart_rate_variability)::numeric, 2) AS avg_hrv,
  ROUND(MIN(heart_rate_variability)::numeric, 2) AS min_hrv,
  ROUND(MAX(heart_rate_variability)::numeric, 2) AS max_hrv,
  COUNT(*)                                        AS reading_count,

  -- ── Morning window ──────────────────────────────────────────────────────
  -- Primary recovery signal. Prefer this column over avg_hrv for any
  -- day-over-day recovery dashboard tile or model feature.
  -- NULL on days with no sleep data or no post-wake reading captured.
  ROUND(
    AVG(CASE WHEN reading_window = 'morning' THEN heart_rate_variability END)::numeric, 2
  ) AS morning_avg_hrv,
  COUNT(CASE WHEN reading_window = 'morning'   THEN 1 END) AS morning_reading_count,

  -- ── Overnight window ────────────────────────────────────────────────────
  -- Readings taken during sleep. Useful for understanding autonomic activity
  -- during rest and correlating with sleep stage data.
  -- overnight_min_hrv is interesting: a very low minimum can indicate a
  -- brief period of poor autonomic regulation during the night.
  ROUND(
    AVG(CASE WHEN reading_window = 'overnight' THEN heart_rate_variability END)::numeric, 2
  ) AS overnight_avg_hrv,
  ROUND(
    MIN(CASE WHEN reading_window = 'overnight' THEN heart_rate_variability END)::numeric, 2
  ) AS overnight_min_hrv,
  COUNT(CASE WHEN reading_window = 'overnight' THEN 1 END) AS overnight_reading_count

FROM hrv_classified
GROUP BY record_date, person
ORDER BY record_date DESC
