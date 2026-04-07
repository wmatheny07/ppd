-- int_weather_hourly_conditions.sql
-- =====================================================================
-- Intermediate: Hourly weather conditions with derived health metrics.
-- Aggregates 15-minute data to hourly, enriches with health categories.
-- =====================================================================

with fifteen_min_data as (

    select * from {{ ref('stg_weather_observations') }}
    where data_resolution = '15min'

),

hourly_data as (

    select * from {{ ref('stg_weather_observations') }}
    where data_resolution = 'hourly'

),

-- Aggregate 15-min data to hourly averages
fifteen_min_hourly as (

    select
        location_id,
        location_name,
        latitude,
        longitude,
        elevation_m,
        context,
        date_trunc('hour', observation_time) as observation_hour,
        'aggregated_15min' as data_source,

        -- Averages
        avg(temperature_f)           as temperature_f,
        avg(feels_like_f)            as feels_like_f,
        avg(relative_humidity_pct)   as relative_humidity_pct,
        avg(dew_point_f)             as dew_point_f,
        avg(pressure_msl_hpa)        as pressure_msl_hpa,
        avg(surface_pressure_hpa)    as surface_pressure_hpa,
        avg(wind_speed_mph)          as wind_speed_mph,
        avg(wind_direction_deg)      as wind_direction_deg,

        -- Maxes (for gusts, precipitation)
        max(wind_gusts_mph)          as wind_gusts_mph,
        sum(precipitation_in)        as precipitation_in,
        sum(rain_in)                 as rain_in,

        -- Mode of weather code (most frequent in hour)
        mode() within group (order by weather_code) as weather_code,

        count(*) as readings_in_hour

    from fifteen_min_data
    group by 1, 2, 3, 4, 5, 6, 7

),

-- Fill in hourly-only variables from the hourly feed
enriched as (

    select
        f.location_id,
        f.location_name,
        f.latitude,
        f.longitude,
        f.elevation_m,
        f.context,
        f.observation_hour,
        f.data_source,

        -- Core weather from 15-min aggregation
        f.temperature_f,
        f.feels_like_f,
        f.relative_humidity_pct,
        f.dew_point_f,
        f.pressure_msl_hpa,
        f.surface_pressure_hpa,
        f.wind_speed_mph,
        f.wind_direction_deg,
        f.wind_gusts_mph,
        f.precipitation_in,
        f.rain_in,
        f.weather_code,
        f.readings_in_hour,

        -- Hourly-only variables
        h.cloud_cover_pct,
        h.cloud_cover_low_pct,
        h.cloud_cover_mid_pct,
        h.cloud_cover_high_pct,
        h.visibility_m,
        h.uv_index,
        h.uv_index_clear_sky,
        h.direct_radiation_wm2,
        h.diffuse_radiation_wm2,
        h.snowfall_in,
        h.snow_depth_in,
        h.soil_temp_surface_f,
        h.soil_moisture_0_1cm,

        -- =========================================================
        -- DERIVED HEALTH METRICS
        -- =========================================================

        -- Heat Index Category
        case
            when f.feels_like_f >= 130 then 'extreme_danger'
            when f.feels_like_f >= 105 then 'danger'
            when f.feels_like_f >= 90  then 'extreme_caution'
            when f.feels_like_f >= 80  then 'caution'
            else 'normal'
        end as heat_risk_category,

        -- Wind Chill Warning
        case
            when f.feels_like_f <= -20 then 'extreme_cold'
            when f.feels_like_f <= 0   then 'severe_cold'
            when f.feels_like_f <= 20  then 'cold_warning'
            when f.feels_like_f <= 32  then 'cold'
            else 'normal'
        end as cold_risk_category,

        -- Barometric pressure change (for migraine/joint pain tracking)
        f.pressure_msl_hpa - lag(f.pressure_msl_hpa, 1) over (
            partition by f.location_id order by f.observation_hour
        ) as pressure_change_1h_hpa,

        f.pressure_msl_hpa - lag(f.pressure_msl_hpa, 3) over (
            partition by f.location_id order by f.observation_hour
        ) as pressure_change_3h_hpa,

        -- Dew point comfort (respiratory health)
        case
            when f.dew_point_f >= 75 then 'oppressive'
            when f.dew_point_f >= 70 then 'very_humid'
            when f.dew_point_f >= 65 then 'humid'
            when f.dew_point_f >= 60 then 'comfortable'
            when f.dew_point_f >= 50 then 'dry'
            else 'very_dry'
        end as humidity_comfort_level,

        -- UV exposure risk
        case
            when h.uv_index >= 11 then 'extreme'
            when h.uv_index >= 8  then 'very_high'
            when h.uv_index >= 6  then 'high'
            when h.uv_index >= 3  then 'moderate'
            else 'low'
        end as uv_risk_category,

        -- Exercise suitability score (0-100)
        -- Penalizes extreme heat, cold, wind, rain, poor visibility
        greatest(0, least(100,
            100
            - (case when f.feels_like_f > 95 then (f.feels_like_f - 95) * 5 else 0 end)
            - (case when f.feels_like_f < 20 then (20 - f.feels_like_f) * 3 else 0 end)
            - (case when f.wind_gusts_mph > 30 then (f.wind_gusts_mph - 30) * 2 else 0 end)
            - (case when f.precipitation_in > 0 then 30 else 0 end)
            - (case when h.uv_index > 8 then (h.uv_index - 8) * 5 else 0 end)
            - (case when f.relative_humidity_pct > 85 then (f.relative_humidity_pct - 85) * 2 else 0 end)
        )) as exercise_suitability_score

    from fifteen_min_hourly f
    left join hourly_data h
        on f.location_id = h.location_id
        and f.observation_hour = h.observation_time

)

select * from enriched
