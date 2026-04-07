-- mart_health_weather_alerts.sql
-- =====================================================================
-- Mart: Health-relevant weather alerts per location per hour.
-- Flags conditions that may impact exercise, respiratory health,
-- joint pain, migraines, eldercare safety, and outdoor activity.
-- =====================================================================

with hourly as (

    select * from {{ ref('int_weather_air_quality_combined') }}

),

alerts as (

    select
        location_id,
        location_name,
        context,
        observation_hour,

        -- Current conditions snapshot
        temperature_f,
        feels_like_f,
        relative_humidity_pct,
        pressure_msl_hpa,
        pressure_change_3h_hpa,
        wind_speed_mph,
        wind_gusts_mph,
        uv_index,
        us_aqi_composite,
        pm2_5_ugm3,
        total_pollen_load,
        exercise_suitability_score,
        outdoor_health_score,

        -- =========================================================
        -- ALERT FLAGS
        -- =========================================================

        -- Heat alerts (relevant for BBC outdoor workouts)
        case when feels_like_f >= 105 then true else false end as alert_extreme_heat,
        case when feels_like_f >= 90  then true else false end as alert_heat_caution,

        -- Cold alerts (eldercare - Clifton Forge winters)
        case when feels_like_f <= 0   then true else false end as alert_extreme_cold,
        case when feels_like_f <= 32  then true else false end as alert_freezing,

        -- Barometric pressure (migraine / joint pain trigger)
        case
            when abs(coalesce(pressure_change_3h_hpa, 0)) >= 6 then true
            else false
        end as alert_rapid_pressure_change,

        -- Air quality (respiratory health)
        case when us_aqi_composite > 100 then true else false end as alert_poor_air_quality,
        case when us_aqi_composite > 150 then true else false end as alert_unhealthy_air,

        -- PM2.5 specifically (fine particulate danger)
        case when pm2_5_ugm3 > 35 then true else false end as alert_high_pm25,

        -- UV exposure
        case when uv_index >= 8  then true else false end as alert_very_high_uv,
        case when uv_index >= 11 then true else false end as alert_extreme_uv,

        -- High pollen (allergy triggers)
        case when total_pollen_load > 50 then true else false end as alert_high_pollen,

        -- Wind safety
        case when wind_gusts_mph >= 40 then true else false end as alert_dangerous_wind,

        -- Exercise not recommended
        case when exercise_suitability_score < 30 then true else false end as alert_skip_outdoor_exercise,

        -- Composite outdoor health warning
        case when outdoor_health_score < 40 then true else false end as alert_outdoor_health_warning,

        -- Humidity extremes (respiratory)
        case when relative_humidity_pct >= 90 then true else false end as alert_very_high_humidity,
        case when relative_humidity_pct <= 20 then true else false end as alert_very_low_humidity,

        -- Severity score (0-10, higher = more concerning)
        (
            (case when feels_like_f >= 105 then 3 when feels_like_f >= 90 then 1 else 0 end)
          + (case when feels_like_f <= 0 then 3 when feels_like_f <= 32 then 1 else 0 end)
          + (case when us_aqi_composite > 150 then 3 when us_aqi_composite > 100 then 1 else 0 end)
          + (case when abs(coalesce(pressure_change_3h_hpa, 0)) >= 6 then 2 else 0 end)
          + (case when uv_index >= 11 then 2 when uv_index >= 8 then 1 else 0 end)
          + (case when total_pollen_load > 50 then 1 else 0 end)
        ) as alert_severity_score,

        -- Alert summary text
        concat_ws(', ',
            case when feels_like_f >= 105 then 'EXTREME HEAT' end,
            case when feels_like_f >= 90 and feels_like_f < 105 then 'Heat Caution' end,
            case when feels_like_f <= 0 then 'EXTREME COLD' end,
            case when feels_like_f > 0 and feels_like_f <= 32 then 'Freezing' end,
            case when abs(coalesce(pressure_change_3h_hpa, 0)) >= 6 then 'Pressure Shift' end,
            case when us_aqi_composite > 150 then 'UNHEALTHY AIR' end,
            case when us_aqi_composite > 100 and us_aqi_composite <= 150 then 'Poor Air' end,
            case when uv_index >= 11 then 'EXTREME UV' end,
            case when uv_index >= 8 and uv_index < 11 then 'Very High UV' end,
            case when total_pollen_load > 50 then 'High Pollen' end,
            case when wind_gusts_mph >= 40 then 'Dangerous Wind' end
        ) as alert_summary

    from hourly

)

select * from alerts
where alert_severity_score > 0
