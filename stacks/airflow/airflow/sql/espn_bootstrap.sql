CREATE TABLE IF NOT EXISTS espn_athlete (
  athlete_id    BIGINT PRIMARY KEY,
  data          JSONB NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS espn_team_athlete (
  season_year   INT    NOT NULL,
  season_type   INT    NOT NULL,
  team_espn_id  INT    NOT NULL,
  athlete_id    BIGINT NOT NULL REFERENCES espn_athlete(athlete_id),
  data          JSONB  NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (season_year, season_type, team_espn_id, athlete_id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_espn_team_athlete_team
  ON espn_team_athlete(team_espn_id);

CREATE INDEX IF NOT EXISTS ix_espn_team_athlete_season
  ON espn_team_athlete(season_year, season_type);
