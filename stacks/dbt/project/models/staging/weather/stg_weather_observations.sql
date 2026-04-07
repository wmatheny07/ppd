-- stg_weather_observations.sql
-- =====================================================================
-- Staging: Clean, typed, and deduplicated weather observations.
-- Uses 15-minute resolution data only (native HRRR for US locations).
-- Hourly data used as fallback for variables not in 15-min feed.
-- =====================================================================

with source as (

    select * from {{ source('raw_weather', 'weather_observations') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by location_id, observation_time, data_resolution
            order by loaded_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key([
            'location_id',
            'observation_time',
            'data_resolution'
        ]) }} as observation_key,

        location_id,
        location_name,
        latitude,
        longitude,
        elevation_m,
        context,

        -- Timestamps
        observation_time,
        data_resolution,
        ingested_at,
        loaded_at,
        source,
        model,

        -- Core weather (health-relevant)
        temperature_2m              as temperature_f,
        apparent_temperature        as feels_like_f,
        relative_humidity_2m        as relative_humidity_pct,
        dew_point_2m                as dew_point_f,

        -- Precipitation
        precipitation               as precipitation_in,
        rain                        as rain_in,
        snowfall                    as snowfall_in,
        snow_depth                  as snow_depth_in,
        weather_code,

        -- Pressure
        pressure_msl                as pressure_msl_hpa,
        surface_pressure            as surface_pressure_hpa,

        -- Cloud & visibility
        cloud_cover                 as cloud_cover_pct,
        cloud_cover_low             as cloud_cover_low_pct,
        cloud_cover_mid             as cloud_cover_mid_pct,
        cloud_cover_high            as cloud_cover_high_pct,
        visibility                  as visibility_m,

        -- Wind
        wind_speed_10m              as wind_speed_mph,
        wind_direction_10m          as wind_direction_deg,
        wind_gusts_10m              as wind_gusts_mph,

        -- UV & solar
        uv_index,
        uv_index_clear_sky,
        direct_radiation            as direct_radiation_wm2,
        diffuse_radiation           as diffuse_radiation_wm2,

        -- Soil
        soil_temperature_0cm        as soil_temp_surface_f,
        soil_moisture_0_to_1cm      as soil_moisture_0_1cm

    from deduplicated
    where _row_num = 1

)

select * from cleaned
