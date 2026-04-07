-- int_weather_air_quality_combined.sql
-- =====================================================================
-- Intermediate: Joins hourly weather conditions with air quality data
-- to create a single unified view for health correlation analysis.
-- =====================================================================

with weather as (

    select * from {{ ref('int_weather_hourly_conditions') }}

),

air_quality as (

    select * from {{ ref('stg_air_quality_observations') }}

),

combined as (

    select
        w.location_id,
        w.location_name,
        w.latitude,
        w.longitude,
        w.elevation_m,
        w.context,
        w.observation_hour,

        -- Weather conditions
        w.temperature_f,
        w.feels_like_f,
        w.relative_humidity_pct,
        w.dew_point_f,
        w.pressure_msl_hpa,
        w.surface_pressure_hpa,
        w.wind_speed_mph,
        w.wind_direction_deg,
        w.wind_gusts_mph,
        w.precipitation_in,
        w.rain_in,
        w.snowfall_in,
        w.weather_code,
        w.cloud_cover_pct,
        w.visibility_m,
        w.uv_index,
        w.direct_radiation_wm2,

        -- Derived weather-health metrics
        w.heat_risk_category,
        w.cold_risk_category,
        w.pressure_change_1h_hpa,
        w.pressure_change_3h_hpa,
        w.humidity_comfort_level,
        w.uv_risk_category,
        w.exercise_suitability_score,

        -- Air quality
        aq.us_aqi_composite,
        aq.eu_aqi_composite,
        aq.pm2_5_ugm3,
        aq.pm10_ugm3,
        aq.o3_ugm3,
        aq.no2_ugm3,
        aq.so2_ugm3,
        aq.co_ugm3,
        aq.dust_ugm3,

        -- Pollen
        aq.pollen_alder,
        aq.pollen_birch,
        aq.pollen_grass,
        aq.pollen_mugwort,
        aq.pollen_olive,
        aq.pollen_ragweed,

        -- AQI health category
        case
            when aq.us_aqi_composite <= 50  then 'good'
            when aq.us_aqi_composite <= 100 then 'moderate'
            when aq.us_aqi_composite <= 150 then 'unhealthy_sensitive'
            when aq.us_aqi_composite <= 200 then 'unhealthy'
            when aq.us_aqi_composite <= 300 then 'very_unhealthy'
            else 'hazardous'
        end as aqi_health_category,

        -- Total pollen load
        coalesce(aq.pollen_alder, 0)
        + coalesce(aq.pollen_birch, 0)
        + coalesce(aq.pollen_grass, 0)
        + coalesce(aq.pollen_mugwort, 0)
        + coalesce(aq.pollen_olive, 0)
        + coalesce(aq.pollen_ragweed, 0) as total_pollen_load,

        -- Composite outdoor health score (0-100)
        -- Combines exercise suitability with air quality
        greatest(0, least(100,
            w.exercise_suitability_score
            - (case
                when aq.us_aqi_composite > 150 then 40
                when aq.us_aqi_composite > 100 then 20
                when aq.us_aqi_composite > 50  then 5
                else 0
            end)
            - (case
                when (coalesce(aq.pollen_grass, 0) + coalesce(aq.pollen_ragweed, 0)) > 50 then 15
                when (coalesce(aq.pollen_grass, 0) + coalesce(aq.pollen_ragweed, 0)) > 20 then 5
                else 0
            end)
        )) as outdoor_health_score

    from weather w
    left join air_quality aq
        on w.location_id = aq.location_id
        and w.observation_hour = aq.observation_time

)

select * from combined
