with src as (
  select
    id as team_stat_pk,
    play_id,
    event_id,
    competition_id,
    team_espn_id,
    team_type,              -- offense/defense etc.
    team_order,
    stat_type,
    stat_payload,
    created_at,
    updated_at
  from {{ source('espn_raw', 'espn_play_team_stat') }}
)

select * from src
