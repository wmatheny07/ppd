CREATE TABLE public.espn_play_stat (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),

  play_id bigint NOT NULL REFERENCES public.espn_play(id) ON DELETE CASCADE,

  scope varchar(20) NOT NULL,          -- 'participant' or 'team'

  athlete_id bigint NULL REFERENCES public.espn_athlete(id),
  team_id bigint NULL REFERENCES public.espn_team(id),

  -- normalized non-null keys for uniqueness (avoid COALESCE in constraint)
  athlete_id_norm bigint NOT NULL DEFAULT 0,
  team_id_norm bigint NOT NULL DEFAULT 0,

  stat_type varchar(50) NULL,
  stat_key varchar(100) NOT NULL,
  stat_value varchar(200) NULL,

  raw_data jsonb NOT NULL,

  CONSTRAINT espn_play_stat_scope_check CHECK (scope IN ('participant','team')),

  CONSTRAINT espn_play_stat_unique
    UNIQUE (play_id, scope, athlete_id_norm, team_id_norm, stat_key)
);

CREATE INDEX espn_play_stat_play_idx ON public.espn_play_stat(play_id);
CREATE INDEX espn_play_stat_athlete_idx ON public.espn_play_stat(athlete_id);
CREATE INDEX espn_play_stat_team_idx ON public.espn_play_stat(team_id);