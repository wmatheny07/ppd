{{ config(
    materialized='incremental',
    unique_key='play_espn_id',
    on_schema_change='sync_all_columns'
) }}

with p as (
  select *
  from {{ ref('stg_espn_play') }}
  {% if is_incremental() %}
    where modified >= (select coalesce(max(modified), '1900-01-01') from {{ this }})
  {% endif %}
)

select * from p
