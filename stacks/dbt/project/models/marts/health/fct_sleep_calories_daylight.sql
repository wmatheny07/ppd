{{config(
    materialized='view',
    schema='marts'
)
}}

select fsc.*, ftids.time_in_daylight from {{ ref('fct_sleep_calories') }} fsc
    join {{ ref('fct_time_in_daylight_summary') }} ftids on
    fsc.record_date = ftids.record_date 
	and fsc.person = ftids.person