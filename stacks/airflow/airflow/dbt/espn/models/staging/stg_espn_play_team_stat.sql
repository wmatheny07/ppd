with src as (
  select
    md5(
      concat_ws('|',
        event_id::text,
        competition_id::text,
        team_id::text,
        stat_type::text
      )
    ) as team_stat_pk,
    event_id,
    competition_id,
    team_id,
    team_type,              -- offense/defense etc.
    sequence_number team_order,
    stat_type,
    stat_payload,
    created_at,
    updated_at
  from {{ ref('mv_espn_play_stat') }}
)
select * from src
