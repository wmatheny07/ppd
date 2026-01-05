{{ config(
    materialized='incremental',
    unique_key='participant_stat_pk',
    on_schema_change='sync_all_columns'
) }}

with s as (
  select *
  from {{ ref('stg_espn_play_participant_stat') }}
  {% if is_incremental() %}
    where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
  {% endif %}
)

select * from s
