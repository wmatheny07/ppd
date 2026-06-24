{{ config(
    materialized='incremental', 
    unique_key=['id', 'person'],
    incremental_strategy='delete+insert'
)
}}

select
    md5(concat(record_date, person, resting_heart_rate)) as id,
        record_date,
        person,
        resting_heart_rate
        from
            {{ ref('vw_resting_hr_metrics') }}
        {%- if is_incremental() %}
        where
        record_date::date > (SELECT MAX(record_date::date) FROM {{ this }})
        or record_date::date > current_date - interval '30 day' -- reprocess last 30 days to capture updates
         {%- endif %}