-- stg_air_quality_observations.sql
-- =====================================================================
-- Staging: Clean, typed, and deduplicated air quality observations.
-- =====================================================================

with source as (

    select * from {{ source('raw_weather', 'air_quality_observations') }}

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
        context,

        -- Timestamps
        observation_time,
        data_resolution,
        ingested_at,
        loaded_at,
        source,

        -- Particulates (µg/m³)
        pm2_5                       as pm2_5_ugm3,
        pm10                        as pm10_ugm3,
        dust                        as dust_ugm3,

        -- Gases (µg/m³)
        carbon_monoxide             as co_ugm3,
        nitrogen_dioxide            as no2_ugm3,
        sulphur_dioxide             as so2_ugm3,
        ozone                       as o3_ugm3,

        -- UV
        uv_index,
        uv_index_clear_sky,

        -- US AQI (0-500 scale)
        us_aqi                      as us_aqi_composite,
        us_aqi_pm2_5,
        us_aqi_pm10,
        us_aqi_nitrogen_dioxide     as us_aqi_no2,
        us_aqi_ozone                as us_aqi_o3,
        us_aqi_sulphur_dioxide      as us_aqi_so2,
        us_aqi_carbon_monoxide      as us_aqi_co,
        european_aqi                as eu_aqi_composite,

        -- Pollen (grains/m³)
        alder_pollen                as pollen_alder,
        birch_pollen                as pollen_birch,
        grass_pollen                as pollen_grass,
        mugwort_pollen              as pollen_mugwort,
        olive_pollen                as pollen_olive,
        ragweed_pollen              as pollen_ragweed

    from deduplicated
    where _row_num = 1

)

select * from cleaned
