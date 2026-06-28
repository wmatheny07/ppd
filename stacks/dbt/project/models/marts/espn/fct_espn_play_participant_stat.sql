{{ config(
    materialized='incremental',
    unique_key='participant_stat_pk',
    on_schema_change='sync_all_columns'
) }}

WITH s AS (
    SELECT *
    FROM {{ ref('stg_espn_play_participant_stat') }}
    {% if is_incremental() %}
        WHERE updated_at >= (
            SELECT COALESCE(MAX(updated_at), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
)

SELECT * FROM s
