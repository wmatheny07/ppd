{{ config(materialized='incremental', unique_key=['id', 'person']) }}

select
    md5(concat(record_date, person, resting_heart_rate)) as id,
        record_date,
        person,
        resting_heart_rate
        from
            {{ ref('vw_resting_hr_metrics') }}
        {% if is_incremental() %}
        where
        record_date > (SELECT MAX(record_date) FROM {{ this }})
        {% endif %}