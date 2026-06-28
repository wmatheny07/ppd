-- stg_air_quality_observations.sql
-- =====================================================================
-- Staging: Clean, typed, and deduplicated air quality observations.
-- =====================================================================

{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT * FROM {{ source('raw_weather', 'air_quality_observations') }}

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
        , context

        -- Timestamps
        , observation_time
        , data_resolution
        , ingested_at
        , loaded_at
        , source

        -- Particulates (ug/m3)
        , pm2_5                       AS pm2_5_ugm3
        , pm10                        AS pm10_ugm3
        , dust                        AS dust_ugm3

        -- Gases (ug/m3)
        , carbon_monoxide             AS co_ugm3
        , nitrogen_dioxide            AS no2_ugm3
        , sulphur_dioxide             AS so2_ugm3
        , ozone                       AS o3_ugm3

        -- UV
        , uv_index
        , uv_index_clear_sky

        -- US AQI (0-500 scale)
        , us_aqi                      AS us_aqi_composite
        , us_aqi_pm2_5
        , us_aqi_pm10
        , us_aqi_nitrogen_dioxide     AS us_aqi_no2
        , us_aqi_ozone                AS us_aqi_o3
        , us_aqi_sulphur_dioxide      AS us_aqi_so2
        , us_aqi_carbon_monoxide      AS us_aqi_co
        , european_aqi                AS eu_aqi_composite

        -- Pollen (grains/m3)
        , alder_pollen                AS pollen_alder
        , birch_pollen                AS pollen_birch
        , grass_pollen                AS pollen_grass
        , mugwort_pollen              AS pollen_mugwort
        , olive_pollen                AS pollen_olive
        , ragweed_pollen              AS pollen_ragweed

    FROM deduplicated

)

SELECT * FROM cleaned
