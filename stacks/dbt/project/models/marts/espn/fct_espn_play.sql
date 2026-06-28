{{ config(
    materialized='incremental',
    unique_key='play_espn_id',
    on_schema_change='sync_all_columns'
) }}

WITH p AS (
    SELECT *
    FROM {{ ref('stg_espn_play') }}
    {% if is_incremental() %}
        WHERE modified_at >= (
            SELECT COALESCE(MAX(modified_at), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
)

SELECT * FROM p
