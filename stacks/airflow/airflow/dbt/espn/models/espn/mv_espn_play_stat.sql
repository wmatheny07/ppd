{{ config(
    materialized='materialized_view',
    schema='espn', 
    unique_key='play_stat_pk',
    indexes=[
      {'columns': ['event_id', 'play_id']},
      {'columns': ['athlete_id']},
      {'columns': ['team_id']},
      {'columns': ['stat_key']}
    ]
) }}

with base as (
  select
    ps.play_id,
    ps.scope,
    ps.athlete_id,
    ps.team_id,
    ps.stat_type,
    ps.stat_key,
    ps.stat_value,
    ps.raw_data as stat_payload,
    case
      when p.offense_team_id = ps.team_id then 'offense'
      else 'defense'
    end as team_type,
    p.espn_id as play_espn_id,
    p.sequence_number,
    p.period_number,
    p.clock_display,
    p.wallclock,
    p.event_espn_id,
    p.competition_espn_id,
    p.created_at,
    p.updated_at,
    p.modified
  from {{ source('espn', 'espn_play_stat') }} ps
  join {{ source('espn', 'espn_play') }} p
    on ps.play_id = p.id
),
enriched as (
  select
    -- stable PK for downstream use (and dedupe if needed)
    md5(
      concat_ws('|',
        base.play_id::text,
        base.scope,
        coalesce(base.athlete_id::text,'0'),
        coalesce(base.team_id::text,'0'),
        base.stat_key
      )
    ) as play_stat_pk,
    base.event_espn_id as event_id,
    base.competition_espn_id as competition_id,
    base.play_id,
    base.play_espn_id,
    base.sequence_number,
    base.period_number,
    base.clock_display,
    base.wallclock,
    base.scope,
    base.athlete_id,
    base.team_id, 
    base.team_type,
    base.stat_type,
    base.stat_key,
    -- normalize value types if you want
    base.stat_value,
    base.stat_payload,
    base.created_at,
    base.updated_at,
    base.modified
  from base
)

select * from enriched;