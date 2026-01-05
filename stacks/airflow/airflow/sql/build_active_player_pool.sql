BEGIN;

-- Idempotent rebuild
TRUNCATE TABLE nfl_dfs.active_player_pool;

INSERT INTO nfl_dfs.active_player_pool (
    athlete_id,
    athlete_espn_id,
    full_name,
    team_id,
    team_espn_id,
    team_abbr,
    position,
    position_group,
    season,
    season_type,
    week,
    is_active,
    is_starter,
    depth_chart_rank,
    injury_status,
    practice_status,
    eligible_dfs,
    source,
    updated_at
)
SELECT
    a.id::bigint                              AS athlete_id,
    a.espn_id::varchar(50)                    AS athlete_espn_id,
    a.full_name                               AS full_name,

    t.id::bigint                              AS team_id,
    t.espn_id::varchar(50)                    AS team_espn_id,
    COALESCE(t.abbreviation, 'N/A')    AS team_abbr,

    -- prefer abbreviation for DFS (QB/RB/WR/TE/DST), otherwise fallback
    COALESCE(NULLIF(a.position_abbreviation, ''), NULLIF(a.position, '')) AS position,

    CASE
      WHEN COALESCE(a.position_abbreviation, a.position) IN ('QB','RB','WR','TE','DST','K') THEN COALESCE(a.position_abbreviation, a.position)
      ELSE 'OTHER'
    END                                       AS position_group,

    -- these need to come from somewhere; set via Airflow/DB variables
    COALESCE(NULLIF(current_setting('nfl_dfs.season', true), '')::int, 2025)       AS season,
    COALESCE(NULLIF(current_setting('nfl_dfs.season_type', true), '')::int, 2)    AS season_type,
    COALESCE(NULLIF(current_setting('nfl_dfs.week', true), '')::int, 1)           AS week,

    TRUE                                      AS is_active,
    FALSE                                     AS is_starter,
    NULL::int                                 AS depth_chart_rank,
    NULL::varchar(50)                         AS injury_status,
    NULL::varchar(50)                         AS practice_status,
    TRUE                                      AS eligible_dfs,

    'espn'                                    AS source,
    now()                                     AS updated_at
FROM espn_raw.espn_athlete a
JOIN espn_raw.espn_team t
  ON t.id = a.team_id
WHERE
    COALESCE(a.is_active, true) = true
    AND COALESCE(NULLIF(a.position_abbreviation, ''), NULLIF(a.position, '')) IS NOT NULL
    AND a.full_name IS NOT NULL;

COMMIT;