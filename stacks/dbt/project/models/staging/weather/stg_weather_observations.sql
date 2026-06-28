-- stg_weather_observations.sql
-- =====================================================================
-- Staging: Clean, typed, and deduplicated weather observations.
-- Uses 15-minute resolution data only (native HRRR for US locations).
-- Hourly data used as fallback for variables not in 15-min feed.
-- =====================================================================

{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT * FROM {{ source('raw_weather', 'weather_observations') }}

),

deduplicated AS (

    SELECT
        DISTINCT ON (location_id, observation_time, data_resolution) *
    FROM source
    ORDER BY location_id, observation_time, data_resolution, loaded_at DESC

),

cleaned AS (

    SELECT
        -- Keys
        {{ dbt_utils.generate_surrogate_key([
            'location_id',
            'observation_time',
            'data_resolution'
        ]) }} AS observation_key

        , location_id
        , location_name
        , latitude
        , longitude
        , elevation_m
        , context

        -- Timestamps
        , observation_time
        , data_resolution
        , ingested_at
        , loaded_at
        , source
        , model

        -- Core weather (health-relevant)
        , temperature_2m              AS temperature_f
        , apparent_temperature        AS feels_like_f
        , relative_humidity_2m        AS relative_humidity_pct
        , dew_point_2m                AS dew_point_f

        -- Precipitation
        , precipitation               AS precipitation_in
        , rain                        AS rain_in
        , snowfall                    AS snowfall_in
        , snow_depth                  AS snow_depth_in
        , weather_code

        -- Pressure
        , pressure_msl                AS pressure_msl_hpa
        , surface_pressure            AS surface_pressure_hpa

        -- Cloud & visibility
        , cloud_cover                 AS cloud_cover_pct
        , cloud_cover_low             AS cloud_cover_low_pct
        , cloud_cover_mid             AS cloud_cover_mid_pct
        , cloud_cover_high            AS cloud_cover_high_pct
        , visibility                  AS visibility_m

        -- Wind
        , wind_speed_10m              AS wind_speed_mph
        , wind_direction_10m          AS wind_direction_deg
        , wind_gusts_10m              AS wind_gusts_mph

        -- UV & solar
        , uv_index
        , uv_index_clear_sky
        , direct_radiation            AS direct_radiation_wm2
        , diffuse_radiation           AS diffuse_radiation_wm2

        -- Soil
        , soil_temperature_0cm        AS soil_temp_surface_f
        , soil_moisture_0_to_1cm      AS soil_moisture_0_1cm

    FROM deduplicated

)

SELECT * FROM cleaned
