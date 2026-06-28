{{ config(
    materialized='incremental',
    unique_key=['record_date', 'person'],
    incremental_strategy='delete+insert',
    tags=['health']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'person',
        'record_date'
    ]) }} AS id
    , record_date
    , person
    , rem
    , core
    , deep
    , awake
    , in_bed_start
    , sleep_start
    , in_bed_end
    , sleep_end
    , total_sleep
FROM {{ ref('vw_sleep_metrics') }}
{% if is_incremental() %}
WHERE record_date::timestamp > (SELECT MAX(record_date::timestamp) FROM {{ this }})
    OR record_date::date > current_date - INTERVAL '30 day'
    -- reprocess last 30 days to capture updates
{% endif %}
