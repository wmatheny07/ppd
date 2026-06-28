-- mart_health_weather_alerts.sql
-- =====================================================================
-- Mart: Health-relevant weather alerts per location per hour.
-- Flags conditions that may impact exercise, respiratory health,
-- joint pain, migraines, eldercare safety, and outdoor activity.
-- =====================================================================

{{
    config(
        materialized='table'
    )
}}

WITH hourly AS (

    SELECT * FROM {{ ref('int_weather_air_quality_combined') }}

),

alerts AS (

    SELECT
        location_id
        , location_name
        , context
        , observation_hour

        -- Current conditions snapshot
        , temperature_f
        , feels_like_f
        , relative_humidity_pct
        , pressure_msl_hpa
        , pressure_change_3h_hpa
        , wind_speed_mph
        , wind_gusts_mph
        , uv_index
        , us_aqi_composite
        , pm2_5_ugm3
        , total_pollen_load
        , exercise_suitability_score
        , outdoor_health_score

        -- =========================================================
        -- ALERT FLAGS
        -- =========================================================

        -- Heat alerts (relevant for BBC outdoor workouts)
        , CASE WHEN feels_like_f >= 105 THEN TRUE ELSE FALSE END
            AS alert_extreme_heat
        , CASE WHEN feels_like_f >= 90  THEN TRUE ELSE FALSE END
            AS alert_heat_caution

        -- Cold alerts (eldercare - Clifton Forge winters)
        , CASE WHEN feels_like_f <= 0   THEN TRUE ELSE FALSE END
            AS alert_extreme_cold
        , CASE WHEN feels_like_f <= 32  THEN TRUE ELSE FALSE END
            AS alert_freezing

        -- Barometric pressure (migraine / joint pain trigger)
        , CASE
            WHEN ABS(COALESCE(pressure_change_3h_hpa, 0)) >= 6
                THEN TRUE
            ELSE FALSE
        END AS alert_rapid_pressure_change

        -- Air quality (respiratory health)
        , CASE WHEN us_aqi_composite > 100 THEN TRUE ELSE FALSE END
            AS alert_poor_air_quality
        , CASE WHEN us_aqi_composite > 150 THEN TRUE ELSE FALSE END
            AS alert_unhealthy_air

        -- PM2.5 specifically (fine particulate danger)
        , CASE WHEN pm2_5_ugm3 > 35 THEN TRUE ELSE FALSE END
            AS alert_high_pm25

        -- UV exposure
        , CASE WHEN uv_index >= 8  THEN TRUE ELSE FALSE END
            AS alert_very_high_uv
        , CASE WHEN uv_index >= 11 THEN TRUE ELSE FALSE END
            AS alert_extreme_uv

        -- High pollen (allergy triggers)
        , CASE WHEN total_pollen_load > 50 THEN TRUE ELSE FALSE END
            AS alert_high_pollen

        -- Wind safety
        , CASE WHEN wind_gusts_mph >= 40 THEN TRUE ELSE FALSE END
            AS alert_dangerous_wind

        -- Exercise not recommended
        , CASE WHEN exercise_suitability_score < 30
            THEN TRUE ELSE FALSE
        END AS alert_skip_outdoor_exercise

        -- Composite outdoor health warning
        , CASE WHEN outdoor_health_score < 40
            THEN TRUE ELSE FALSE
        END AS alert_outdoor_health_warning

        -- Humidity extremes (respiratory)
        , CASE WHEN relative_humidity_pct >= 90
            THEN TRUE ELSE FALSE
        END AS alert_very_high_humidity
        , CASE WHEN relative_humidity_pct <= 20
            THEN TRUE ELSE FALSE
        END AS alert_very_low_humidity

        -- Severity score (0-10, higher = more concerning)
        , (
            (CASE
                WHEN feels_like_f >= 105 THEN 3
                WHEN feels_like_f >= 90 THEN 1
                ELSE 0
            END)
            + (CASE
                WHEN feels_like_f <= 0 THEN 3
                WHEN feels_like_f <= 32 THEN 1
                ELSE 0
            END)
            + (CASE
                WHEN us_aqi_composite > 150 THEN 3
                WHEN us_aqi_composite > 100 THEN 1
                ELSE 0
            END)
            + (CASE
                WHEN ABS(COALESCE(pressure_change_3h_hpa, 0)) >= 6
                    THEN 2
                ELSE 0
            END)
            + (CASE
                WHEN uv_index >= 11 THEN 2
                WHEN uv_index >= 8 THEN 1
                ELSE 0
            END)
            + (CASE
                WHEN total_pollen_load > 50 THEN 1
                ELSE 0
            END)
        ) AS alert_severity_score

        -- Alert summary text
        , CONCAT_WS(', ',
            CASE WHEN feels_like_f >= 105
                THEN 'EXTREME HEAT' END,
            CASE WHEN feels_like_f >= 90
                AND feels_like_f < 105
                THEN 'Heat Caution' END,
            CASE WHEN feels_like_f <= 0
                THEN 'EXTREME COLD' END,
            CASE WHEN feels_like_f > 0
                AND feels_like_f <= 32
                THEN 'Freezing' END,
            CASE WHEN ABS(COALESCE(pressure_change_3h_hpa, 0)) >= 6
                THEN 'Pressure Shift' END,
            CASE WHEN us_aqi_composite > 150
                THEN 'UNHEALTHY AIR' END,
            CASE WHEN us_aqi_composite > 100
                AND us_aqi_composite <= 150
                THEN 'Poor Air' END,
            CASE WHEN uv_index >= 11
                THEN 'EXTREME UV' END,
            CASE WHEN uv_index >= 8
                AND uv_index < 11
                THEN 'Very High UV' END,
            CASE WHEN total_pollen_load > 50
                THEN 'High Pollen' END,
            CASE WHEN wind_gusts_mph >= 40
                THEN 'Dangerous Wind' END
        ) AS alert_summary

    FROM hourly

)

SELECT * FROM alerts
WHERE alert_severity_score > 0
