with src as (
  select
    id                                 as participant_stat_pk,

    play_id::bigint                    as play_id,          -- make sure this is DB play PK, not ESPN play id
    event_id::bigint                   as event_id,
    competition_id::bigint             as competition_id,

    nullif(athlete_espn_id,'')::text   as athlete_espn_id,
    nullif(position_espn_id,'')::text  as position_espn_id,

    nullif(participant_type,'')::text  as participant_type,
    participant_order::int             as participant_order,

    nullif(stat_type,'')::text         as stat_type,        -- '0'/'1' or whatever you store
    stat_payload::jsonb                as stat_payload,

    created_at,
    updated_at
  from {{ source('espn', 'espn_play_participant_stat') }}
)

select * from src