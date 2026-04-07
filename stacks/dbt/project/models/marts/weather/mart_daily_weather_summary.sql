-- mart_daily_weather_summary.sql
-- =====================================================================
-- Mart: Daily weather summary per location.
-- Designed for Superset dashboards and health correlation analysis.
-- =====================================================================

with hourly as (

    select * from {{ ref('int_weather_air_quality_combined') }}

),

daily as (

    select
        location_id,
        location_name,
        context,
        date(observation_hour) as observation_date,

        -- Temperature
        min(temperature_f)      as temp_min_f,
        max(temperature_f)      as temp_max_f,
        avg(temperature_f)      as temp_avg_f,
        max(temperature_f) - min(temperature_f) as temp_range_f,
        min(feels_like_f)       as feels_like_min_f,
        max(feels_like_f)       as feels_like_max_f,

        -- Humidity
        avg(relative_humidity_pct)  as humidity_avg_pct,
        min(relative_humidity_pct)  as humidity_min_pct,
        max(relative_humidity_pct)  as humidity_max_pct,
        avg(dew_point_f)            as dew_point_avg_f,

        -- Pressure
        avg(pressure_msl_hpa)       as pressure_avg_hpa,
        min(pressure_msl_hpa)       as pressure_min_hpa,
        max(pressure_msl_hpa)       as pressure_max_hpa,
        max(pressure_msl_hpa) - min(pressure_msl_hpa) as pressure_swing_hpa,
        max(abs(pressure_change_3h_hpa)) as max_pressure_change_3h_hpa,

        -- Wind
        avg(wind_speed_mph)     as wind_speed_avg_mph,
        max(wind_gusts_mph)     as wind_gust_max_mph,

        -- Precipitation
        sum(precipitation_in)   as precipitation_total_in,
        sum(rain_in)            as rain_total_in,
        sum(snowfall_in)        as snowfall_total_in,
        sum(case when precipitation_in > 0 then 1 else 0 end) as hours_with_precip,

        -- UV
        max(uv_index)           as uv_index_max,
        avg(uv_index)           as uv_index_avg,
        sum(case when uv_index >= 6 then 1 else 0 end) as hours_high_uv,

        -- Cloud cover
        avg(cloud_cover_pct)    as cloud_cover_avg_pct,

        -- Air quality
        avg(us_aqi_composite)   as aqi_avg,
        max(us_aqi_composite)   as aqi_max,
        avg(pm2_5_ugm3)         as pm2_5_avg_ugm3,
        max(pm2_5_ugm3)         as pm2_5_max_ugm3,
        avg(o3_ugm3)            as ozone_avg_ugm3,

        -- Pollen
        avg(total_pollen_load)  as pollen_load_avg,
        max(total_pollen_load)  as pollen_load_max,
        avg(pollen_grass)       as pollen_grass_avg,
        avg(pollen_ragweed)     as pollen_ragweed_avg,

        -- Health scores
        avg(exercise_suitability_score) as exercise_score_avg,
        min(exercise_suitability_score) as exercise_score_min,
        avg(outdoor_health_score)       as outdoor_health_score_avg,

        -- Risk category hours
        sum(case when heat_risk_category in ('danger', 'extreme_danger') then 1 else 0 end)
            as hours_heat_danger,
        sum(case when heat_risk_category in ('caution', 'extreme_caution') then 1 else 0 end)
            as hours_heat_caution,
        sum(case when cold_risk_category in ('severe_cold', 'extreme_cold') then 1 else 0 end)
            as hours_cold_danger,
        sum(case when aqi_health_category in ('unhealthy', 'very_unhealthy', 'hazardous') then 1 else 0 end)
            as hours_unhealthy_air,

        -- Dominant conditions (mode)
        mode() within group (order by humidity_comfort_level) as dominant_humidity_comfort,
        mode() within group (order by aqi_health_category)   as dominant_aqi_category,

        count(*) as hours_with_data

    from hourly
    group by 1, 2, 3, 4

)

select * from daily
