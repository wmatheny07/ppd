#!/usr/bin/env python3
import argparse
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_batch  # already imported in your other scripts
# optional:
from psycopg2.extras import execute_values

import requests
from requests.adapters import HTTPAdapter
import psycopg2

try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None  # type: ignore

CORE_BASE = "http://sports.core.api.espn.com/v2"


# -----------------------
# HTTP helpers
# -----------------------
def _set_query_param(url: str, params: dict) -> str:
    parts = urlparse(url)
    q = parse_qs(parts.query)
    for k, v in params.items():
        q[k] = [str(v)]
    new_query = urlencode(q, doseq=True)
    return urlunparse(parts._replace(query=new_query))

def _retry():
    if Retry is None:
        return None
    kw = dict(
        total=8,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
    )
    try:
        return Retry(**kw, allowed_methods=frozenset(["GET"]))
    except TypeError:
        return Retry(**kw, method_whitelist=frozenset(["GET"]))

def session_with_retries() -> requests.Session:
    s = requests.Session()
    r = _retry()
    if r is not None:
        a = HTTPAdapter(max_retries=r)
        s.mount("http://", a)
        s.mount("https://", a)
    s.headers.update({"User-Agent": "ppd-espn-pbp/1.0", "Accept": "application/json"})
    return s

def get_json(s: requests.Session, url: str, timeout: int = 30) -> Dict[str, Any]:
    r = s.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


# -----------------------
# DB helpers
# -----------------------
def get_engine() -> "sqlalchemy.Engine":
    """
    Prefers explicit DB_* env vars.
    (You can keep your Airflow-conn approach too, but env is simplest for BashOperator.)
    """
    host = os.getenv("ESPN_DB_HOST", "").strip()
    port = os.getenv("ESPN_DB_PORT", "5432").strip()
    db = os.getenv("ESPN_DB_NAME", "").strip()
    user = os.getenv("ESPN_DB_USER", "").strip()
    pwd = os.getenv("ESPN_DB_PASSWORD", "").strip()

    if not all([host, db, user, pwd]):
        raise RuntimeError("Missing DB env vars (DB_HOST/DB_NAME/DB_USER/DB_PASSWORD).")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url,execution_options={"stream_results": True})

def to_bool(v):
    if v is None: return None
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    s = str(v).strip().lower()
    if s in ("true","t","1","yes","y"): return True
    if s in ("false","f","0","no","n"): return False
    return None

def parse_ts(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None

def espn_id_from_ref(ref: str) -> Optional[str]:
    # Pull trailing number after /teams/{id} or /athletes/{id} etc.
    m = re.search(r"/(teams|athletes)/(\d+)", ref)
    if not m:
        return None
    return m.group(2)

def play_id_from_ref(ref: str) -> str:
    m = re.search(r"/plays/(\d+)", ref)
    if not m:
        raise ValueError(f"Cannot parse play id from: {ref}")
    return m.group(1)

def plays_url(sport: str, league: str, event_id: str) -> str:
    # scheme: /events/{event}/competitions/{event}/plays
    return f"{CORE_BASE}/sports/{sport}/leagues/{league}/events/{event_id}/competitions/{event_id}/plays?lang=en&region=us"

def iter_play_refs(session: requests.Session, url: str) -> Iterable[str]:
    # ESPN Core uses ?page=1..pageCount
    page = 1
    while True:
        u = _set_query_param(url, {"page": page})
        data = get_json(session, u)
        for it in (data.get("items") or []):
            ref = it.get("$ref")
            if ref:
                yield ref

        page_index = int(data.get("pageIndex") or page)
        page_count = int(data.get("pageCount") or page_index)
        if page_index >= page_count:
            break
        page += 1

def stat_type_from_ref(sref: str) -> str | None:
    m = re.search(r"/(statistics|playStatistics)/(\d+)(?:/|\?|$)", sref)
    return m.group(2) if m else None

def slim_payload(payload: dict, sref: str, ref_key: str) -> dict:
    splits = payload.get("splits") or {}
    cats = splits.get("categories") or []
    return {
        "ref": sref,
        "ref_key": ref_key,                 # "statistics" vs "playStatistics"
        "splits_id": splits.get("id"),
        "categories": [
            {
                "name": c.get("name"),
                "stats": [
                    {
                        "name": st.get("name"),
                        "displayValue": st.get("displayValue"),
                        "value": st.get("value"),
                    }
                    for st in (c.get("stats") or [])
                    if st.get("name") is not None
                ],
            }
            for c in cats
        ],
    }

# -----------------------
# Upserts
# -----------------------
UPSERT_PLAY_SQL = """
INSERT INTO public.espn_play (
  created_at, updated_at,
  espn_id, event_espn_id, competition_espn_id,
  sequence_number, type_id, type_text, type_abbreviation,
  text, short_text, alternative_text, short_alternative_text,
  period_number, clock_value, clock_display,
  wallclock, modified,
  home_score, away_score, scoring_play, priority, score_value, stat_yardage,
  team_id, offense_team_id, defense_team_id,
  start_json, end_json, penalty_json,
  drive_ref, probability_ref,
  raw_data
) VALUES (
  now(), now(),
  %(espn_id)s, %(event_espn_id)s, %(competition_espn_id)s,
  %(sequence_number)s, %(type_id)s, %(type_text)s, %(type_abbreviation)s,
  %(text)s, %(short_text)s, %(alternative_text)s, %(short_alternative_text)s,
  %(period_number)s, %(clock_value)s, %(clock_display)s,
  %(wallclock)s, %(modified)s,
  %(home_score)s, %(away_score)s, %(scoring_play)s, %(priority)s, %(score_value)s, %(stat_yardage)s,
  %(team_id)s, %(offense_team_id)s, %(defense_team_id)s,
  %(start_json)s::jsonb, %(end_json)s::jsonb, %(penalty_json)s::jsonb,
  %(drive_ref)s, %(probability_ref)s,
  %(raw_data)s::jsonb
)
ON CONFLICT (espn_id) DO UPDATE SET
  updated_at = now(),
  event_espn_id = EXCLUDED.event_espn_id,
  competition_espn_id = EXCLUDED.competition_espn_id,
  sequence_number = EXCLUDED.sequence_number,
  type_id = EXCLUDED.type_id,
  type_text = EXCLUDED.type_text,
  type_abbreviation = EXCLUDED.type_abbreviation,
  text = EXCLUDED.text,
  short_text = EXCLUDED.short_text,
  alternative_text = EXCLUDED.alternative_text,
  short_alternative_text = EXCLUDED.short_alternative_text,
  period_number = EXCLUDED.period_number,
  clock_value = EXCLUDED.clock_value,
  clock_display = EXCLUDED.clock_display,
  wallclock = EXCLUDED.wallclock,
  modified = EXCLUDED.modified,
  home_score = EXCLUDED.home_score,
  away_score = EXCLUDED.away_score,
  scoring_play = EXCLUDED.scoring_play,
  priority = EXCLUDED.priority,
  score_value = EXCLUDED.score_value,
  stat_yardage = EXCLUDED.stat_yardage,
  team_id = EXCLUDED.team_id,
  offense_team_id = EXCLUDED.offense_team_id,
  defense_team_id = EXCLUDED.defense_team_id,
  start_json = EXCLUDED.start_json,
  end_json = EXCLUDED.end_json,
  penalty_json = EXCLUDED.penalty_json,
  drive_ref = EXCLUDED.drive_ref,
  probability_ref = EXCLUDED.probability_ref,
  raw_data = EXCLUDED.raw_data;
"""

UPSERT_PARTICIPANT_SQL = """
INSERT INTO public.espn_play_participant (
  created_at, updated_at,
  play_id, athlete_id, team_id,
  role_type, "order",
  athlete_espn_id, team_espn_id,
  position_ref, statistics_ref, play_statistics_ref,
  raw_data
) VALUES (
  now(), now(),
  %(play_id)s, %(athlete_id)s, %(team_id)s,
  %(role_type)s, %(order)s,
  %(athlete_espn_id)s, %(team_espn_id)s,
  %(position_ref)s, %(statistics_ref)s, %(play_statistics_ref)s,
  %(raw_data)s::jsonb
)
ON CONFLICT (play_id, athlete_espn_id, role_type, "order") DO UPDATE SET
  updated_at = now(),
  athlete_id = EXCLUDED.athlete_id,
  team_id = EXCLUDED.team_id,
  team_espn_id = EXCLUDED.team_espn_id,
  position_ref = EXCLUDED.position_ref,
  statistics_ref = EXCLUDED.statistics_ref,
  play_statistics_ref = EXCLUDED.play_statistics_ref,
  raw_data = EXCLUDED.raw_data;
"""


def build_team_lookup(cur) -> Dict[str, int]:
    cur.execute("SELECT id, espn_id FROM public.espn_team WHERE espn_id IS NOT NULL;")
    return {str(r[1]): int(r[0]) for r in cur.fetchall()}

def build_athlete_lookup(cur) -> Dict[str, int]:
    cur.execute("SELECT id, espn_id FROM public.espn_athlete WHERE espn_id IS NOT NULL;")
    return {str(r[1]): int(r[0]) for r in cur.fetchall()}

def parse_off_def_team_ids(play_obj: Dict[str, Any], team_map: Dict[str, int]) -> Tuple[Optional[int], Optional[int]]:
    offense = None
    defense = None
    for tp in (play_obj.get("teamParticipants") or []):
        tref = ((tp.get("team") or {}).get("$ref")) or ""
        tid = espn_id_from_ref(tref)
        if not tid:
            continue
        pk = team_map.get(str(tid))
        if tp.get("type") == "offense":
            offense = pk
        elif tp.get("type") == "defense":
            defense = pk
    return offense, defense

def parse_play_row(play_obj: Dict[str, Any], event_id: str, team_map: Dict[str, int]) -> Dict[str, Any]:
    team_ref = ((play_obj.get("team") or {}).get("$ref")) or ""
    play_team_espn = espn_id_from_ref(team_ref)
    play_team_pk = team_map.get(str(play_team_espn)) if play_team_espn else None

    offense_pk, defense_pk = parse_off_def_team_ids(play_obj, team_map)

    ptype = play_obj.get("type") or {}
    period = play_obj.get("period") or {}
    clock = play_obj.get("clock") or {}

    return {
        "espn_id": str(play_obj.get("id") or ""),
        "event_espn_id": str(event_id),
        "competition_espn_id": str(event_id),
        "sequence_number": int(play_obj["sequenceNumber"]) if str(play_obj.get("sequenceNumber") or "").isdigit() else None,
        "type_id": str(ptype.get("id")) if ptype.get("id") is not None else None,
        "type_text": ptype.get("text"),
        "type_abbreviation": ptype.get("abbreviation"),
        "text": play_obj.get("text"),
        "short_text": play_obj.get("shortText"),
        "alternative_text": play_obj.get("alternativeText"),
        "short_alternative_text": play_obj.get("shortAlternativeText"),
        "period_number": int(period.get("number")) if period.get("number") is not None else None,
        "clock_value": clock.get("value"),
        "clock_display": clock.get("displayValue"),
        "wallclock": parse_ts(play_obj.get("wallclock")),
        "modified": parse_ts(play_obj.get("modified")),
        "home_score": play_obj.get("homeScore"),
        "away_score": play_obj.get("awayScore"),
        "scoring_play": to_bool(play_obj.get("scoringPlay")),
        "priority": to_bool(play_obj.get("priority")),
        "score_value": play_obj.get("scoreValue"),
        "stat_yardage": play_obj.get("statYardage"),
        "team_id": play_team_pk,
        "offense_team_id": offense_pk,
        "defense_team_id": defense_pk,
        "start_json": json.dumps(play_obj.get("start") or {}),
        "end_json": json.dumps(play_obj.get("end") or {}),
        "penalty_json": json.dumps(play_obj.get("penalty") or {}),
        "drive_ref": ((play_obj.get("drive") or {}).get("$ref")),
        "probability_ref": ((play_obj.get("probability") or {}).get("$ref")),
        "raw_data": json.dumps(play_obj),
    }

def build_participant_rows(
    play_db_id: int,
    play_obj: Dict[str, Any],
    team_map: Dict[str, int],
    athlete_map: Dict[str, int],
) -> list[Dict[str, Any]]:
    play_team_ref = ((play_obj.get("team") or {}).get("$ref")) or ""
    play_team_espn = espn_id_from_ref(play_team_ref)
    play_team_pk = team_map.get(str(play_team_espn)) if play_team_espn else None

    out: list[Dict[str, Any]] = []
    for p in (play_obj.get("participants") or []):
        aref = ((p.get("athlete") or {}).get("$ref")) or ""
        aid = espn_id_from_ref(aref)
        athlete_pk = athlete_map.get(str(aid)) if aid else None

        out.append(
            {
                "play_id": play_db_id,
                "athlete_id": athlete_pk,
                "team_id": play_team_pk,
                "role_type": p.get("type"),
                "order": p.get("order"),
                # IMPORTANT: never NULL for uniqueness behavior
                "athlete_espn_id": str(aid) if aid else "unknown",
                "team_espn_id": str(play_team_espn) if play_team_espn else None,
                "position_ref": ((p.get("position") or {}).get("$ref")),
                "statistics_ref": ((p.get("statistics") or {}).get("$ref")),
                "play_statistics_ref": ((p.get("playStatistics") or {}).get("$ref")),
                "raw_data": json.dumps(p),
            }
        )
    return out

# --- new: parse stats payload into normalized rows ---
def extract_stats(payload: dict) -> list[tuple[str, str]]:
    out = []
    splits = payload.get("splits") or {}
    for cat in (splits.get("categories") or []):
        for st in (cat.get("stats") or []):
            key = st.get("name")
            if not key:
                continue
            # prefer displayValue, fallback to numeric value
            val = st.get("displayValue")
            if val is None:
                v = st.get("value")
                val = None if v is None else str(v)
            out.append((str(key), None if val is None else str(val)))
    return out

# --- participant execute_values template (tuple-based for speed) ---
PART_COLS = [
    "play_id",
    "athlete_id",
    "team_id",
    "role_type",
    "order",
    "athlete_espn_id",
    "team_espn_id",
    "position_ref",
    "statistics_ref",
    "play_statistics_ref",
    "raw_data",
]

PART_TEMPLATE = "(" + ",".join(["now()", "now()"] + ["%s"] * len(PART_COLS[:-1]) + ["%s::jsonb"]) + ")"
# Explanation: created_at, updated_at are now(); last field raw_data is cast to jsonb

PART_VALUES_SQL = """
INSERT INTO public.espn_play_participant (
  created_at, updated_at,
  play_id, athlete_id, team_id,
  role_type, "order",
  athlete_espn_id, team_espn_id,
  position_ref, statistics_ref, play_statistics_ref,
  raw_data
) VALUES %s
ON CONFLICT (play_id, athlete_espn_id, role_type, "order") DO UPDATE SET
  updated_at = now(),
  athlete_id = EXCLUDED.athlete_id,
  team_id = EXCLUDED.team_id,
  team_espn_id = EXCLUDED.team_espn_id,
  position_ref = EXCLUDED.position_ref,
  statistics_ref = EXCLUDED.statistics_ref,
  play_statistics_ref = EXCLUDED.play_statistics_ref,
  raw_data = EXCLUDED.raw_data;
"""

UPSERT_PLAY_STAT_SQL = """
INSERT INTO public.espn_play_stat (
  created_at,
  play_id,
  scope,
  athlete_id,
  team_id,
  athlete_id_norm,
  team_id_norm,
  stat_type,
  stat_key,
  stat_value,
  raw_data
) VALUES (
  now(),
  %(play_id)s,
  %(scope)s,
  %(athlete_id)s,
  %(team_id)s,
  %(athlete_id_norm)s,
  %(team_id_norm)s,
  %(stat_type)s,
  %(stat_key)s,
  %(stat_value)s,
  %(raw_data)s::jsonb
)
ON CONFLICT (play_id, scope, athlete_id_norm, team_id_norm, stat_key) DO UPDATE SET
  stat_type = EXCLUDED.stat_type,
  stat_value = EXCLUDED.stat_value,
  raw_data = EXCLUDED.raw_data;
"""

def build_participant_stat_rows(session, play_db_id, play_obj, play_team_pk, athlete_map):
    rows = []
    for p in (play_obj.get("participants") or []):
        aref = ((p.get("athlete") or {}).get("$ref")) or ""
        athlete_espn_id = espn_id_from_ref(aref)
        athlete_pk = athlete_map.get(str(athlete_espn_id)) if athlete_espn_id else None

        for ref_key in ("statistics", "playStatistics"):
            sref = ((p.get(ref_key) or {}).get("$ref")) or ""
            if not sref:
                continue

            payload = get_json(session, sref)

            # ✅ stable 0/1 from URL
            stat_type = stat_type_from_ref(sref)

            # ✅ lean raw_data
            raw = {
                "ref": sref,
                "ref_key": ref_key,
                "stat_type": stat_type,
                "splits_id": (payload.get("splits") or {}).get("id"),
                "athlete_espn_id": athlete_espn_id,   # super useful
            }

            for stat_key, stat_value in extract_stats(payload):
                rows.append({
                    "play_id": play_db_id,
                    "scope": "participant",
                    "athlete_id": athlete_pk,
                    "team_id": play_team_pk,
                    "athlete_id_norm": int(athlete_pk or 0),
                    "team_id_norm": int(play_team_pk or 0),
                    "stat_type": stat_type,
                    "stat_key": stat_key,
                    "stat_value": stat_value,
                    "raw_data": json.dumps(raw),
                })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="football")
    ap.add_argument("--league", default="nfl")
    ap.add_argument("--sleep", type=float, default=0.0)

    ap.add_argument("--event-id", action="append", default=[])
    ap.add_argument("--events-sql", default="")
    ap.add_argument("--limit-events", type=int, default=0)

    ap.add_argument("--play-batch-size", type=int, default=250)
    ap.add_argument("--participant-batch-size", type=int, default=5000)
    args = ap.parse_args()

    s = session_with_retries()
    engine = get_engine()

    PLAY_BATCH_SIZE = max(1, int(args.play_batch_size))
    PART_BATCH_SIZE = max(1, int(args.participant_batch_size))
    STAT_BATCH_SIZE = 5000
    stat_batch: list[dict] = []

    with engine.begin() as conn:
        raw = conn.connection  # psycopg2
        with raw.cursor() as cur:
            team_map = build_team_lookup(cur)
            athlete_map = build_athlete_lookup(cur)

            # resolve events
            event_ids = [str(x) for x in args.event_id if str(x).strip()]
            if args.events_sql:
                cur.execute(args.events_sql)
                event_ids.extend([str(r[0]) for r in cur.fetchall()])

            event_ids = list(dict.fromkeys(event_ids))
            if args.limit_events and args.limit_events > 0:
                event_ids = event_ids[: args.limit_events]

            if not event_ids:
                raise SystemExit("No events provided. Use --event-id ... or --events-sql ...")

            plays_processed = 0
            parts_processed = 0

            participant_batch: list[Dict[str, Any]] = []

            def flush_stats():
                nonlocal stat_batch
                if not stat_batch:
                    return
                execute_batch(cur, UPSERT_PLAY_STAT_SQL, stat_batch, page_size=500)
                stat_batch = []
                
            def flush_participants():
                nonlocal participant_batch, parts_processed
                if not participant_batch:
                    return

                tuples = [tuple(p[c] for c in PART_COLS) for p in participant_batch]
                execute_values(
                    cur,
                    PART_VALUES_SQL,
                    tuples,
                    template=PART_TEMPLATE,
                    page_size=1000,
                )
                parts_processed += len(participant_batch)
                participant_batch = []

            def flush_plays(play_batch: list[dict], play_objs_by_espn_id: dict):
                nonlocal plays_processed, participant_batch, stat_batch

                if not play_batch:
                    return

                # upsert plays (dict-params)
                execute_batch(cur, UPSERT_PLAY_SQL, play_batch, page_size=250)

                espn_ids: list[str] = [str(p["espn_id"]) for p in play_batch if p.get("espn_id")]
                if not espn_ids:
                    return

                # fetch db ids for the batch
                cur.execute(
                    "SELECT id, espn_id FROM public.espn_play WHERE espn_id = ANY(%s::text[]);",
                    (espn_ids,),
                )
                id_map = {str(r[1]): int(r[0]) for r in cur.fetchall()}

                # build participants for each play
                for espn_id in espn_ids:
                    play_db_id = id_map.get(espn_id)
                    pobj = play_objs_by_espn_id.get(espn_id)
                    if not play_db_id or not pobj:
                        continue

                    # figure out play_team_pk once per play for stats
                    play_team_ref = ((pobj.get("team") or {}).get("$ref")) or ""
                    play_team_espn = espn_id_from_ref(play_team_ref)
                    play_team_pk = team_map.get(str(play_team_espn)) if play_team_espn else None

                    participant_batch.extend(
                        build_participant_rows(play_db_id, pobj, team_map, athlete_map)
                    )

                    if len(participant_batch) >= PART_BATCH_SIZE:
                        flush_participants()

                    # queue participant stats rows
                    stat_batch.extend(
                        build_participant_stat_rows(
                            session=s,
                            play_db_id=play_db_id,
                            play_obj=pobj,
                            play_team_pk=play_team_pk,
                            athlete_map=athlete_map,
                        )
                    )
                    if len(stat_batch) >= STAT_BATCH_SIZE:
                        flush_stats()

                plays_processed += len(play_batch)

            for event_id in event_ids:
                url = plays_url(args.sport, args.league, event_id)
                print(f"[event {event_id}] starting plays ingest", flush=True)

                play_batch: list[dict] = []
                play_objs_by_espn_id: dict[str, dict] = {}

                for pref in iter_play_refs(s, url):
                    play_obj = get_json(s, pref)
                    play_row = parse_play_row(play_obj, event_id, team_map)
                    espn_id = str(play_row.get("espn_id") or "").strip()
                    if not espn_id:
                        continue

                    play_batch.append(play_row)
                    play_objs_by_espn_id[espn_id] = play_obj

                    if len(play_batch) >= PLAY_BATCH_SIZE:
                        flush_plays(play_batch, play_objs_by_espn_id)
                        play_batch = []
                        play_objs_by_espn_id = {}

                        print(
                            f"[event {event_id}] plays={plays_processed} participants_flushed={parts_processed} queued={len(participant_batch)}",
                            flush=True,
                        )

                    if args.sleep:
                        time.sleep(args.sleep)

                # flush leftovers for this event
                flush_plays(play_batch, play_objs_by_espn_id)
                flush_stats()
                flush_participants()

                print(
                    f"[event {event_id}] done. plays_total={plays_processed} participants_total={parts_processed}",
                    flush=True,
                )

    print("✅ finished", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
