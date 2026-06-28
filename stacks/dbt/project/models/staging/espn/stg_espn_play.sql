WITH src AS (
    SELECT
        id AS play_pk
        , espn_id AS play_espn_id
        , event_id
        , competition_id
        , sequence_number
        , type_id AS play_type_id
        , type_text AS play_type_text
        , type_abbreviation AS play_type_abbr
        , text AS play_text
        , short_text AS play_short_text
        , period_number
        , clock_display
        , clock_value_seconds
        , scoring_play
        , stat_yardage
        , wallclock_at
        , modified_at
        , team_espn_id
        , drive_espn_id
        , start_down
        , start_distance
        , start_yard_line
        , start_yards_to_endzone
        , start_team_espn_id
        , end_down
        , end_distance
        , end_yard_line
        , end_yards_to_endzone
        , end_down_distance_text
        , end_possession_text
        , end_team_espn_id
        , penalty_yards
        , penalty_type_text
        , penalty_status_text
        , created_at
        , updated_at
    FROM {{ source('espn_raw', 'espn_play') }}
)

SELECT * FROM src
