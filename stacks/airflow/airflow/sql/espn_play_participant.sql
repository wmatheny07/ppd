CREATE TABLE public.espn_play_participant (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  play_id bigint NOT NULL REFERENCES public.espn_play(id) ON DELETE CASCADE,
  athlete_id bigint NULL REFERENCES public.espn_athlete(id) DEFERRABLE INITIALLY DEFERRED,
  team_id bigint NULL REFERENCES public.espn_team(id) DEFERRABLE INITIALLY DEFERRED,

  role_type varchar(50) NULL,      -- e.g., passer, receiver, rusher, tackler, penalized
  "order" int NULL,

  athlete_espn_id varchar(50) NULL, -- keep raw espn_id for easier backfill if athlete not yet present
  team_espn_id varchar(50) NULL,

  position_ref text NULL,
  statistics_ref text NULL,
  play_statistics_ref text NULL,

  raw_data jsonb NOT NULL,

  UNIQUE (play_id, athlete_espn_id, role_type, "order")
);

CREATE INDEX espn_play_participant_play_idx ON public.espn_play_participant(play_id);
CREATE INDEX espn_play_participant_athlete_idx ON public.espn_play_participant(athlete_id);
