-- Ensure schema
CREATE SCHEMA IF NOT EXISTS nfl_dfs;

-- DK salaries table (if you want Airflow to own creation)
-- (skip if your dk_ingest.py already creates it; but it's fine to keep idempotent)
CREATE TABLE IF NOT EXISTS nfl_dfs.dk_salaries (
  id BIGSERIAL PRIMARY KEY,
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  slate_date DATE NOT NULL,
  source_file TEXT NOT NULL,

  dk_player_id BIGINT,
  name_id TEXT NOT NULL,
  name TEXT,

  position TEXT,
  roster_position TEXT,
  salary INTEGER CHECK (salary >= 0),

  game_info TEXT,
  team_abbrev TEXT,
  avg_points_per_game NUMERIC,

  raw JSONB NOT NULL DEFAULT '{}'::jsonb,

  dk_key TEXT GENERATED ALWAYS AS (COALESCE(dk_player_id::text, name_id)) STORED,
  UNIQUE (slate_date, source_file, dk_key, roster_position, position)
);

CREATE INDEX IF NOT EXISTS dk_salaries_slate_idx ON nfl_dfs.dk_salaries (slate_date);

-- PFN depth charts (example)
CREATE TABLE IF NOT EXISTS nfl_dfs.depth_chart_current (
  slate_date DATE NOT NULL,
  team TEXT NOT NULL,
  team_abbrev TEXT,
  position_group TEXT NOT NULL,
  depth_rank INT NOT NULL,
  player_name TEXT NOT NULL,

  -- generated normalized column for joins
  player_name_norm TEXT
    GENERATED ALWAYS AS (lower(player_name)) STORED,

  player_slug TEXT,
  player_href TEXT,
  source TEXT NOT NULL DEFAULT 'pfn',
  ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (slate_date, team, position_group, depth_rank, player_name)
);

CREATE INDEX IF NOT EXISTS depth_chart_current_join_idx
ON nfl_dfs.depth_chart_current (slate_date, team_abbrev, player_name_norm);

CREATE TABLE IF NOT EXISTS nfl_dfs.active_player_pool (
    id BIGSERIAL PRIMARY KEY,

    -- identity
    athlete_id        VARCHAR(50) NOT NULL,
    athlete_espn_id   VARCHAR(50) NOT NULL,
    full_name         TEXT NOT NULL,

    -- team / position
    team_id           BIGINT NULL,
    team_espn_id      VARCHAR(50),
    team_abbr         VARCHAR(10),
    position          VARCHAR(10) NOT NULL,
    position_group    VARCHAR(20),   -- QB / RB / WR / TE / DST

    -- slate context
    season            INTEGER NOT NULL,
    season_type       INTEGER NOT NULL, -- 1=pre, 2=reg, 3=post
    week              INTEGER NOT NULL,

    -- availability / status
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    is_starter        BOOLEAN DEFAULT FALSE,
    depth_chart_rank  INTEGER,
    injury_status     VARCHAR(50),     -- OUT / DOUBTFUL / QUESTIONABLE
    practice_status   VARCHAR(50),

    -- DFS eligibility helpers
    eligible_dfs      BOOLEAN NOT NULL DEFAULT TRUE,
    salary_floor      INTEGER,          -- optional (min seen salary)
    salary_ceiling    INTEGER,          -- optional (max seen salary)

    -- metadata
    source            VARCHAR(50) DEFAULT 'espn',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- uniqueness: one row per player per slate
    UNIQUE (athlete_espn_id, season, season_type, week)
);