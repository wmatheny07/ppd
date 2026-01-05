with base as (
  select
    play_id,
    event_id,
    competition_id,
    athlete_espn_id,
    participant_type,
    participant_order,
    stat_type,
    stat_payload
  from {{ ref('stg_espn_play_participant_stat') }}
),

cats as (
  select
    *,
    jsonb_array_elements(coalesce(stat_payload->'categories','[]'::jsonb)) as cat
  from base
),

stats as (
  select
    play_id,
    event_id,
    competition_id,
    athlete_espn_id,
    participant_type,
    participant_order,
    stat_type,
    cat->>'name' as category_name,
    st->>'name' as stat_key,
    coalesce(st->>'displayValue', st->>'value') as stat_value
  from cats
  cross join lateral jsonb_array_elements(coalesce(cat->'stats','[]'::jsonb)) as st
)

select *
from stats
where stat_key is not null