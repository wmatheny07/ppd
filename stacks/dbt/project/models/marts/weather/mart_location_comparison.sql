-- mart_location_comparison.sql
-- =====================================================================
-- Mart: Cross-location weather comparison for a given day.
-- Enables side-by-side analysis of conditions across all 5 locations.
-- Useful for travel planning and understanding eldercare conditions.
-- =====================================================================

with daily as (

    select * from {{ ref('mart_daily_weather_summary') }}

),

with_rankings as (

    select
        *,

        -- Rank locations by key health metrics each day
        rank() over (
            partition by observation_date
            order by outdoor_health_score_avg desc
        ) as best_outdoor_rank,

        rank() over (
            partition by observation_date
            order by temp_max_f desc
        ) as hottest_rank,

        rank() over (
            partition by observation_date
            order by temp_min_f asc
        ) as coldest_rank,

        rank() over (
            partition by observation_date
            order by aqi_avg asc
        ) as best_air_quality_rank,

        rank() over (
            partition by observation_date
            order by pressure_swing_hpa desc
        ) as most_pressure_volatility_rank,

        -- Location vs home base delta
        temp_avg_f - first_value(temp_avg_f) over (
            partition by observation_date
            order by case when location_id = 'summerville_sc' then 0 else 1 end
        ) as temp_delta_vs_home_f,

        aqi_avg - first_value(aqi_avg) over (
            partition by observation_date
            order by case when location_id = 'summerville_sc' then 0 else 1 end
        ) as aqi_delta_vs_home

    from daily

)

select * from with_rankings
