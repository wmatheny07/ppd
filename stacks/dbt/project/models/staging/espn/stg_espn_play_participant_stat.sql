WITH src AS (
    SELECT
        id AS participant_stat_pk
        , play_id
        , event_id
        , competition_id
        , athlete_espn_id
        , position_espn_id
        , participant_type
        , participant_order
        , stat_type              -- e.g. 0 vs 1 from ESPN endpoints (playStatistics/statistics)
        , stat_payload           -- jsonb (raw)
        , created_at
        , updated_at
    FROM {{ source('espn_raw', 'espn_play_participant_stat') }}
)

SELECT * FROM src
