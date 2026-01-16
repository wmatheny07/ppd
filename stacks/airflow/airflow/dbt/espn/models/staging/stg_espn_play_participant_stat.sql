with src as (
  select
    md5(
      concat_ws('|',
        ps.play_id::text,
        p.event_espn_id::text,
        p.competition_espn_id::text,
        ps.athlete_id::text
      )
    ) as participant_stat_pk,
    play_id::bigint                    as play_id,          -- make sure this is DB play PK, not ESPN play id
    p.event_espn_id::bigint                   as event_id,
    p.competition_espn_id::bigint             as competition_id,
    nullif(athlete_id::text,'')::text   as athlete_id,
    nullif(a.position,'')::text  as position,
    sequence_number             as participant_order,
    nullif(stat_type,'')::text         as stat_type,        -- '0'/'1' or whatever you store
    esp.payload::jsonb                as stat_payload,
    p.created_at,
    p.updated_at
  from {{ source('espn','espn_play_stat') }} ps
  join {{ source('espn','espn_play') }} p
    on ps.play_id = p.id
  join {{ source('espn','espn_athlete') }} a
    on ps.athlete_id = a.id
    join {{ source('espn','espn_stat_payload') }} esp
    on ps.payload_id = esp.id
  where scope = 'participant'
)
select * from src