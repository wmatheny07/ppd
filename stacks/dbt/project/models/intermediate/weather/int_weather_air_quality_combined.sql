-- int_weather_air_quality_combined.sql
-- =====================================================================
-- Intermediate: Joins hourly weather conditions with air quality data
-- to create a single unified view for health correlation analysis.
-- =====================================================================

{{
    config(
        materialized='view'
    )
}}

WITH weather AS (

    SELECT * FROM {{ ref('int_weather_hourly_conditions') }}

),

air_quality AS (

    SELECT * FROM {{ ref('stg_air_quality_observations') }}

),

combined AS (

    SELECT
        w.location_id
        , w.location_name
        , w.latitude
        , w.longitude
        , w.elevation_m
        , w.context
        , w.observation_hour

        -- Weather conditions
        , w.temperature_f
        , w.feels_like_f
        , w.relative_humidity_pct
        , w.dew_point_f
        , w.pressure_msl_hpa
        , w.surface_pressure_hpa
        , w.wind_speed_mph
        , w.wind_direction_deg
        , w.wind_gusts_mph
        , w.precipitation_in
        , w.rain_in
        , w.snowfall_in
        , w.weather_code
        , w.cloud_cover_pct
        , w.visibility_m
        , w.uv_index
        , w.direct_radiation_wm2

        -- Derived weather-health metrics
        , w.heat_risk_category
        , w.cold_risk_category
        , w.pressure_change_1h_hpa
        , w.pressure_change_3h_hpa
        , w.humidity_comfort_level
        , w.uv_risk_category
        , w.exercise_suitability_score

        -- Air quality
        , aq.us_aqi_composite
        , aq.eu_aqi_composite
        , aq.pm2_5_ugm3
        , aq.pm10_ugm3
        , aq.o3_ugm3
        , aq.no2_ugm3
        , aq.so2_ugm3
        , aq.co_ugm3
        , aq.dust_ugm3

        -- Pollen
        , aq.pollen_alder
        , aq.pollen_birch
        , aq.pollen_grass
        , aq.pollen_mugwort
        , aq.pollen_olive
        , aq.pollen_ragweed

        -- AQI health category
        , CASE
            WHEN aq.us_aqi_composite <= 50  THEN 'good'
            WHEN aq.us_aqi_composite <= 100 THEN 'moderate'
            WHEN aq.us_aqi_composite <= 150 THEN 'unhealthy_sensitive'
            WHEN aq.us_aqi_composite <= 200 THEN 'unhealthy'
            WHEN aq.us_aqi_composite <= 300 THEN 'very_unhealthy'
            ELSE 'hazardous'
        END AS aqi_health_category

        -- Total pollen load
        , COALESCE(aq.pollen_alder, 0)
            + COALESCE(aq.pollen_birch, 0)
            + COALESCE(aq.pollen_grass, 0)
            + COALESCE(aq.pollen_mugwort, 0)
            + COALESCE(aq.pollen_olive, 0)
            + COALESCE(aq.pollen_ragweed, 0) AS total_pollen_load

        -- Composite outdoor health score (0-100)
        -- Combines exercise suitability with air quality
        , GREATEST(0, LEAST(100,
            w.exercise_suitability_score
            - (CASE
                WHEN aq.us_aqi_composite > 150 THEN 40
                WHEN aq.us_aqi_composite > 100 THEN 20
                WHEN aq.us_aqi_composite > 50  THEN 5
                ELSE 0
            END)
            - (CASE
                WHEN (COALESCE(aq.pollen_grass, 0)
                    + COALESCE(aq.pollen_ragweed, 0)) > 50 THEN 15
                WHEN (COALESCE(aq.pollen_grass, 0)
                    + COALESCE(aq.pollen_ragweed, 0)) > 20 THEN 5
                ELSE 0
            END)
        )) AS outdoor_health_score

    FROM weather AS w
    LEFT JOIN air_quality AS aq
        ON w.location_id = aq.location_id
        AND w.observation_hour = aq.observation_time

)

SELECT * FROM combined
