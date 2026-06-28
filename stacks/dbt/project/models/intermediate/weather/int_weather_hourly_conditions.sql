-- int_weather_hourly_conditions.sql
-- =====================================================================
-- Intermediate: Hourly weather conditions with derived health metrics.
-- Aggregates 15-minute data to hourly, enriches with health categories.
-- =====================================================================

{{
    config(
        materialized='view'
    )
}}

WITH fifteen_min_data AS (

    SELECT * FROM {{ ref('stg_weather_observations') }}
    WHERE data_resolution = '15min'

),

hourly_data AS (

    SELECT * FROM {{ ref('stg_weather_observations') }}
    WHERE data_resolution = 'hourly'

),

-- Aggregate 15-min data to hourly averages
fifteen_min_hourly AS (

    SELECT
        location_id
        , location_name
        , latitude
        , longitude
        , elevation_m
        , context
        , DATE_TRUNC('hour', observation_time) AS observation_hour
        , 'aggregated_15min' AS data_source

        -- Averages
        , AVG(temperature_f)           AS temperature_f
        , AVG(feels_like_f)            AS feels_like_f
        , AVG(relative_humidity_pct)   AS relative_humidity_pct
        , AVG(dew_point_f)             AS dew_point_f
        , AVG(pressure_msl_hpa)        AS pressure_msl_hpa
        , AVG(surface_pressure_hpa)    AS surface_pressure_hpa
        , AVG(wind_speed_mph)          AS wind_speed_mph
        , AVG(wind_direction_deg)      AS wind_direction_deg

        -- Maxes (for gusts, precipitation)
        , MAX(wind_gusts_mph)          AS wind_gusts_mph
        , SUM(precipitation_in)        AS precipitation_in
        , SUM(rain_in)                 AS rain_in

        -- Mode of weather code (most frequent in hour)
        , MODE() WITHIN GROUP (ORDER BY weather_code) AS weather_code

        , COUNT(*) AS readings_in_hour

    FROM fifteen_min_data
    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

-- Fill in hourly-only variables from the hourly feed
enriched AS (

    SELECT
        f.location_id
        , f.location_name
        , f.latitude
        , f.longitude
        , f.elevation_m
        , f.context
        , f.observation_hour
        , f.data_source

        -- Core weather from 15-min aggregation
        , f.temperature_f
        , f.feels_like_f
        , f.relative_humidity_pct
        , f.dew_point_f
        , f.pressure_msl_hpa
        , f.surface_pressure_hpa
        , f.wind_speed_mph
        , f.wind_direction_deg
        , f.wind_gusts_mph
        , f.precipitation_in
        , f.rain_in
        , f.weather_code
        , f.readings_in_hour

        -- Hourly-only variables
        , h.cloud_cover_pct
        , h.cloud_cover_low_pct
        , h.cloud_cover_mid_pct
        , h.cloud_cover_high_pct
        , h.visibility_m
        , h.uv_index
        , h.uv_index_clear_sky
        , h.direct_radiation_wm2
        , h.diffuse_radiation_wm2
        , h.snowfall_in
        , h.snow_depth_in
        , h.soil_temp_surface_f
        , h.soil_moisture_0_1cm

        -- =========================================================
        -- DERIVED HEALTH METRICS
        -- =========================================================

        -- Heat Index Category
        , CASE
            WHEN f.feels_like_f >= 130 THEN 'extreme_danger'
            WHEN f.feels_like_f >= 105 THEN 'danger'
            WHEN f.feels_like_f >= 90  THEN 'extreme_caution'
            WHEN f.feels_like_f >= 80  THEN 'caution'
            ELSE 'normal'
        END AS heat_risk_category

        -- Wind Chill Warning
        , CASE
            WHEN f.feels_like_f <= -20 THEN 'extreme_cold'
            WHEN f.feels_like_f <= 0   THEN 'severe_cold'
            WHEN f.feels_like_f <= 20  THEN 'cold_warning'
            WHEN f.feels_like_f <= 32  THEN 'cold'
            ELSE 'normal'
        END AS cold_risk_category

        -- Barometric pressure change (for migraine/joint pain tracking)
        , f.pressure_msl_hpa - LAG(f.pressure_msl_hpa, 1) OVER (
            PARTITION BY f.location_id ORDER BY f.observation_hour
        ) AS pressure_change_1h_hpa

        , f.pressure_msl_hpa - LAG(f.pressure_msl_hpa, 3) OVER (
            PARTITION BY f.location_id ORDER BY f.observation_hour
        ) AS pressure_change_3h_hpa

        -- Dew point comfort (respiratory health)
        , CASE
            WHEN f.dew_point_f >= 75 THEN 'oppressive'
            WHEN f.dew_point_f >= 70 THEN 'very_humid'
            WHEN f.dew_point_f >= 65 THEN 'humid'
            WHEN f.dew_point_f >= 60 THEN 'comfortable'
            WHEN f.dew_point_f >= 50 THEN 'dry'
            ELSE 'very_dry'
        END AS humidity_comfort_level

        -- UV exposure risk
        , CASE
            WHEN h.uv_index >= 11 THEN 'extreme'
            WHEN h.uv_index >= 8  THEN 'very_high'
            WHEN h.uv_index >= 6  THEN 'high'
            WHEN h.uv_index >= 3  THEN 'moderate'
            ELSE 'low'
        END AS uv_risk_category

        -- Exercise suitability score (0-100)
        -- Penalizes extreme heat, cold, wind, rain, poor visibility
        , GREATEST(0, LEAST(100,
            100
            - (CASE WHEN f.feels_like_f > 95 THEN (f.feels_like_f - 95) * 5 ELSE 0 END)
            - (CASE WHEN f.feels_like_f < 20 THEN (20 - f.feels_like_f) * 3 ELSE 0 END)
            - (CASE WHEN f.wind_gusts_mph > 30 THEN (f.wind_gusts_mph - 30) * 2 ELSE 0 END)
            - (CASE WHEN f.precipitation_in > 0 THEN 30 ELSE 0 END)
            - (CASE WHEN h.uv_index > 8 THEN (h.uv_index - 8) * 5 ELSE 0 END)
            - (CASE WHEN f.relative_humidity_pct > 85 THEN (f.relative_humidity_pct - 85) * 2 ELSE 0 END)
        )) AS exercise_suitability_score

    FROM fifteen_min_hourly AS f
    LEFT JOIN hourly_data AS h
        ON f.location_id = h.location_id
        AND f.observation_hour = h.observation_time

)

SELECT * FROM enriched
