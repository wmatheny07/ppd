with src as (
  select
    id as play_pk,
    espn_id as play_espn_id,
    event_espn_id,
    competition_espn_id,
    sequence_number,
    type_id as play_type_id,
    type_text as play_type_text,
    type_abbreviation as play_type_abbr,
    text as play_text,
    short_text as play_short_text,
    period_number,
    clock_display,
    clock_value as clock_value_seconds,
    scoring_play,
    stat_yardage,
    wallclock,
    modified,
    offense_team_id,
    defense_team_id,
    (start_json -> 'down')::int start_down,
    (start_json -> 'distance')::int start_distance,
    case
      when (start_json -> 'yardLine')::int > 50 then (100 - (start_json -> 'yardLine')::int)::int
      else (start_json -> 'yardLine')::int
    end as start_yard_line,
    (start_json -> 'yardsToEndzone')::int start_yards_to_endzone,
    offense_team_id as start_team_espn_id,
    (end_json -> 'down')::int end_down,
    (end_json -> 'distance')::int end_distance,
    case
      when (end_json -> 'yardLine')::int > 50 then (100 - (end_json -> 'yardLine')::int)::int
      else (end_json -> 'yardLine')::int
    end as end_yard_line,
    (end_json -> 'yardsToEndzone')::int end_yards_to_endzone,
    case
      when team_id <> offense_team_id then team_id
      else offense_team_id
    end end_team_espn_id,
    (end_json -> 'endPossessionText')::text end_possession_text,
    coalesce((penalty_json -> 'yards')::int,0) penalty_yards,
    (penalty_json -> 'type' ->> 'slug')::text penalty_slug,
    (penalty_json -> 'status' ->> 'slug')::text penalty_status_slug,
    (penalty_json -> 'type' ->> 'text')::text penalty_text,
    (penalty_json -> 'status' ->> 'text')::text penalty_status_text,
    created_at,
    updated_at
  from {{ source('espn', 'espn_play') }}
)

select * from src
