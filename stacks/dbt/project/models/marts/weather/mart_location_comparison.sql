-- mart_location_comparison.sql
-- =====================================================================
-- Mart: Cross-location weather comparison for a given day.
-- Enables side-by-side analysis of conditions across all 5 locations.
-- Useful for travel planning and understanding eldercare conditions.
-- =====================================================================

{{
    config(
        materialized='table'
    )
}}

WITH daily AS (

    SELECT * FROM {{ ref('mart_daily_weather_summary') }}

),

with_rankings AS (

    SELECT
        *

        -- Rank locations by key health metrics each day
        , RANK() OVER (
            PARTITION BY observation_date
            ORDER BY outdoor_health_score_avg DESC
        ) AS best_outdoor_rank

        , RANK() OVER (
            PARTITION BY observation_date
            ORDER BY temp_max_f DESC
        ) AS hottest_rank

        , RANK() OVER (
            PARTITION BY observation_date
            ORDER BY temp_min_f ASC
        ) AS coldest_rank

        , RANK() OVER (
            PARTITION BY observation_date
            ORDER BY aqi_avg ASC
        ) AS best_air_quality_rank

        , RANK() OVER (
            PARTITION BY observation_date
            ORDER BY pressure_swing_hpa DESC
        ) AS most_pressure_volatility_rank

        -- Location vs home base delta
        , temp_avg_f - FIRST_VALUE(temp_avg_f) OVER (
            PARTITION BY observation_date
            ORDER BY CASE
                WHEN location_id = 'summerville_sc' THEN 0
                ELSE 1
            END
        ) AS temp_delta_vs_home_f

        , aqi_avg - FIRST_VALUE(aqi_avg) OVER (
            PARTITION BY observation_date
            ORDER BY CASE
                WHEN location_id = 'summerville_sc' THEN 0
                ELSE 1
            END
        ) AS aqi_delta_vs_home

    FROM daily

)

SELECT * FROM with_rankings
