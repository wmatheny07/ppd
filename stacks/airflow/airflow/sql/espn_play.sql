CREATE TABLE espn.espn_play (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  espn_id varchar(50) NOT NULL UNIQUE,          -- play id (e.g., 40177271039)
  event_espn_id varchar(50) NOT NULL,           -- game id (e.g., 401772710)
  competition_espn_id varchar(50) NOT NULL,     -- often same as event id in NFL core

  sequence_number int NULL,
  type_id varchar(50) NULL,
  type_text varchar(100) NULL,
  type_abbreviation varchar(20) NULL,

  text text NULL,
  short_text text NULL,
  alternative_text text NULL,
  short_alternative_text text NULL,

  period_number int NULL,
  clock_value numeric NULL,
  clock_display varchar(20) NULL,

  wallclock timestamptz NULL,
  modified timestamptz NULL,

  home_score int NULL,
  away_score int NULL,
  scoring_play boolean NULL,
  priority boolean NULL,
  score_value int NULL,
  stat_yardage int NULL,

  team_id bigint NULL REFERENCES public.espn_team(id) DEFERRABLE INITIALLY DEFERRED, -- “team” object on play
  offense_team_id bigint NULL REFERENCES public.espn_team(id) DEFERRABLE INITIALLY DEFERRED,
  defense_team_id bigint NULL REFERENCES public.espn_team(id) DEFERRABLE INITIALLY DEFERRED,

  start_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  end_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
  penalty_json jsonb NOT NULL DEFAULT '{}'::jsonb,

  drive_ref text NULL,
  probability_ref text NULL,

  raw_data jsonb NOT NULL
);

CREATE INDEX espn_play_event_idx ON espn.espn_play(event_espn_id);
CREATE INDEX espn_play_team_idx ON espn.espn_play(team_id);
CREATE INDEX espn_play_offdef_idx ON espn.espn_play(offense_team_id, defense_team_id);
