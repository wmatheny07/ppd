-- mart_daily_weather_summary.sql
-- =====================================================================
-- Mart: Daily weather summary per location.
-- Designed for Superset dashboards and health correlation analysis.
-- =====================================================================

{{
    config(
        materialized='table'
    )
}}

WITH hourly AS (

    SELECT * FROM {{ ref('int_weather_air_quality_combined') }}

),

daily AS (

    SELECT
        location_id
        , location_name
        , context
        , DATE(observation_hour) AS observation_date

        -- Temperature
        , MIN(temperature_f)      AS temp_min_f
        , MAX(temperature_f)      AS temp_max_f
        , AVG(temperature_f)      AS temp_avg_f
        , MAX(temperature_f) - MIN(temperature_f) AS temp_range_f
        , MIN(feels_like_f)       AS feels_like_min_f
        , MAX(feels_like_f)       AS feels_like_max_f

        -- Humidity
        , AVG(relative_humidity_pct)  AS humidity_avg_pct
        , MIN(relative_humidity_pct)  AS humidity_min_pct
        , MAX(relative_humidity_pct)  AS humidity_max_pct
        , AVG(dew_point_f)            AS dew_point_avg_f

        -- Pressure
        , AVG(pressure_msl_hpa)       AS pressure_avg_hpa
        , MIN(pressure_msl_hpa)       AS pressure_min_hpa
        , MAX(pressure_msl_hpa)       AS pressure_max_hpa
        , MAX(pressure_msl_hpa) - MIN(pressure_msl_hpa)
            AS pressure_swing_hpa
        , MAX(ABS(pressure_change_3h_hpa))
            AS max_pressure_change_3h_hpa

        -- Wind
        , AVG(wind_speed_mph)     AS wind_speed_avg_mph
        , MAX(wind_gusts_mph)     AS wind_gust_max_mph

        -- Precipitation
        , SUM(precipitation_in)   AS precipitation_total_in
        , SUM(rain_in)            AS rain_total_in
        , SUM(snowfall_in)        AS snowfall_total_in
        , SUM(CASE WHEN precipitation_in > 0 THEN 1 ELSE 0 END)
            AS hours_with_precip

        -- UV
        , MAX(uv_index)           AS uv_index_max
        , AVG(uv_index)           AS uv_index_avg
        , SUM(CASE WHEN uv_index >= 6 THEN 1 ELSE 0 END)
            AS hours_high_uv

        -- Cloud cover
        , AVG(cloud_cover_pct)    AS cloud_cover_avg_pct

        -- Air quality
        , AVG(us_aqi_composite)   AS aqi_avg
        , MAX(us_aqi_composite)   AS aqi_max
        , AVG(pm2_5_ugm3)         AS pm2_5_avg_ugm3
        , MAX(pm2_5_ugm3)         AS pm2_5_max_ugm3
        , AVG(o3_ugm3)            AS ozone_avg_ugm3

        -- Pollen
        , AVG(total_pollen_load)  AS pollen_load_avg
        , MAX(total_pollen_load)  AS pollen_load_max
        , AVG(pollen_grass)       AS pollen_grass_avg
        , AVG(pollen_ragweed)     AS pollen_ragweed_avg

        -- Health scores
        , AVG(exercise_suitability_score) AS exercise_score_avg
        , MIN(exercise_suitability_score) AS exercise_score_min
        , AVG(outdoor_health_score)       AS outdoor_health_score_avg

        -- Risk category hours
        , SUM(CASE
            WHEN heat_risk_category IN ('danger', 'extreme_danger')
                THEN 1 ELSE 0
        END) AS hours_heat_danger
        , SUM(CASE
            WHEN heat_risk_category IN ('caution', 'extreme_caution')
                THEN 1 ELSE 0
        END) AS hours_heat_caution
        , SUM(CASE
            WHEN cold_risk_category IN ('severe_cold', 'extreme_cold')
                THEN 1 ELSE 0
        END) AS hours_cold_danger
        , SUM(CASE
            WHEN aqi_health_category
                IN ('unhealthy', 'very_unhealthy', 'hazardous')
                THEN 1 ELSE 0
        END) AS hours_unhealthy_air

        -- Dominant conditions (mode)
        , MODE() WITHIN GROUP (ORDER BY humidity_comfort_level)
            AS dominant_humidity_comfort
        , MODE() WITHIN GROUP (ORDER BY aqi_health_category)
            AS dominant_aqi_category

        , COUNT(*) AS hours_with_data

    FROM hourly
    GROUP BY 1, 2, 3, 4

)

SELECT * FROM daily
