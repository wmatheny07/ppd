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
import csv
from pathlib import Path
import hashlib
import requests
from requests.adapters import HTTPAdapter
import psycopg2
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None  # type: ignore

CORE_BASE = "http://sports.core.api.espn.com/v2"

OFFENSE_ROLES = {
  "passer","receiver","rusher","fumbler","kicker","punter","returner",
  "holder","snapper","blocker"
}
DEFENSE_ROLES = {"tackler","interceptor","sacker","defender","recoverer"}

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

def get_off_def_team_pks(play_obj, team_map):
    offense_pk = None
    defense_pk = None

    for tp in play_obj.get("teamParticipants", []):
        ref = (tp.get("team") or {}).get("$ref")
        tid = espn_id_from_ref(ref) if ref else None
        if not tid:
            continue

        pk = team_map.get(str(tid))
        if tp.get("type") == "offense":
            offense_pk = pk
        elif tp.get("type") == "defense":
            defense_pk = pk

    return offense_pk, defense_pk

def participant_team_pk(role, offense_pk, defense_pk, play_team_pk=None, explicit_team_pk=None):
    # 1) explicit team (if ESPN ever provides it on participant)
    if explicit_team_pk:
        return explicit_team_pk

    # 2) role mapping
    if role in OFFENSE_ROLES and offense_pk:
        return offense_pk
    if role in DEFENSE_ROLES and defense_pk:
        return defense_pk

    # 3) last resort fallback
    return play_team_pk

# -----------------------
# DB helpers
# -----------------------

def get_engine(env_var: str = "ANALYTICS_DB_URI") -> "sqlalchemy.Engine":
    uri = os.getenv(env_var, "").strip()
    if not uri:
        raise RuntimeError(f"Missing DB connection URI in env var {env_var}")

    # Normalize scheme from Airflow "postgres://" to SQLAlchemy "postgresql+psycopg2://"
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql+psycopg2://", 1)

    # Optional: quick debug (comment out later)
    # print(f"Using DB URI ({env_var}): {uri}", flush=True)

    return create_engine(uri, pool_pre_ping=True)

def payload_hash(raw: dict) -> str:
    # stable string -> stable hash
    s = json.dumps(raw, separators=(",", ":"), sort_keys=True)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

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

def stat_type_from_ref(sref: str) -> Optional[str]:
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

def load_stat_flags(path: str) -> dict[tuple[str, str], bool]:
    """
    Returns {(stat_type, stat_key): include_bool}
    Supports stat_type='*' rows.
    """
    print(f"Loading stat flags from {path}...")
    rules: dict[str, bool] = {}
    if not path:
        return rules
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"stat flags csv not found: {path}")

    with p.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            stat_key = (row.get("stat_key") or "").strip()
            inc = (row.get("include") or "1").strip().lower() in ("1", "true", "t", "yes", "y")
            if inc:
                rules[(stat_key)] = inc
            else:
                print(f"{stat_key} will not be included in the extract.")
    return rules

def stat_allowed(rules: dict[str, bool], stat_key: str) -> bool:
    if not rules:
        return True  # no filter configured
    # explicit match wins; then wildcard stat_type; then default include
    if (stat_key) in rules:
        return rules[(stat_key)]
    return True

def get_payload_id(cur, cache: dict[str, int], raw: dict) -> int:
    ph = payload_hash(raw)
    if ph in cache:
        return cache[ph]
    cur.execute(PAYLOAD_UPSERT_SQL, (ph, json.dumps(raw)))
    pid = int(cur.fetchone()[0])
    cache[ph] = pid
    return pid

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
PAYLOAD_UPSERT_SQL = """
insert into public.espn_stat_payload (payload_hash, payload)
values (%s, %s::jsonb)
on conflict (payload_hash) do update
  set updated_at = now()
returning id;
"""

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
  payload_id
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
  %(payload_id)s
)
ON CONFLICT (play_id, scope, athlete_id_norm, team_id_norm, stat_key) DO UPDATE SET
  stat_type = EXCLUDED.stat_type,
  stat_value = EXCLUDED.stat_value,
  payload_id = EXCLUDED.payload_id;
"""

def process_event(event_id: str,
                  engine,
                  team_map: Dict[str, int],
                  athlete_map: Dict[str, int],
                  stat_rules: dict[str, bool],
                  args) -> tuple[str, int, int]:
    """
    Process a single event end-to-end in its own thread:
    - its own HTTP session
    - its own DB connection & cursor
    """
    s = session_with_retries()

    PLAY_BATCH_SIZE = max(1, int(args.play_batch_size))
    PART_BATCH_SIZE = max(1, int(args.participant_batch_size))
    STAT_BATCH_SIZE = 5000

    stat_batch: list[dict] = []
    payload_cache: dict[str, int] = {}
    participant_batch: list[Dict[str, Any]] = []

    plays_processed = 0
    parts_processed = 0

    with engine.begin() as conn:
        pg_conn = conn.connection  # psycopg2
        with pg_conn.cursor() as cur:

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

                # upsert plays
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

                # build participants + stats for each play
                for eid in espn_ids:
                    play_db_id = id_map.get(eid)
                    pobj = play_objs_by_espn_id.get(eid)
                    if not play_db_id or not pobj:
                        continue

                    # team for play
                    play_team_ref = ((pobj.get("team") or {}).get("$ref")) or ""
                    play_team_espn = espn_id_from_ref(play_team_ref)
                    play_team_pk = team_map.get(str(play_team_espn)) if play_team_espn else None

                    # participants
                    participant_batch.extend(
                        build_participant_rows(play_db_id, pobj, team_map, athlete_map)
                    )
                    if len(participant_batch) >= PART_BATCH_SIZE:
                        flush_participants()

                    # offense/defense teams
                    off_pk, def_pk = get_off_def_team_pks(pobj, team_map)

                    # stats
                    stat_rows = build_participant_stat_rows(
                        session=s,
                        cur=cur,
                        payload_cache=payload_cache,
                        play_db_id=play_db_id,
                        play_obj=pobj,
                        play_team_pk=play_team_pk,
                        offense_pk=off_pk,
                        defense_pk=def_pk,
                        athlete_map=athlete_map,
                        stat_rules=stat_rules,
                    )

                    stat_batch.extend(stat_rows)
                    if len(stat_batch) >= STAT_BATCH_SIZE:
                        flush_stats()

                plays_processed += len(play_batch)

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
                        f"[event {event_id}] plays={plays_processed} "
                        f"participants_flushed={parts_processed} "
                        f"queued={len(participant_batch)}",
                        flush=True,
                    )

                if args.sleep:
                    time.sleep(args.sleep)

            # flush leftovers
            flush_plays(play_batch, play_objs_by_espn_id)
            flush_stats()
            flush_participants()

            print(
                f"[event {event_id}] done. "
                f"plays_total={plays_processed} participants_total={parts_processed}",
                flush=True,
            )

    return event_id, plays_processed, parts_processed

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

def build_participant_stat_rows(session, cur, payload_cache, play_db_id, play_obj, play_team_pk, offense_pk, defense_pk, athlete_map, stat_rules):
    rows = []
    for p in (play_obj.get("participants") or []):
        aref = ((p.get("athlete") or {}).get("$ref")) or ""
        athlete_espn_id = espn_id_from_ref(aref)
        athlete_pk = athlete_map.get(str(athlete_espn_id)) if athlete_espn_id else None
        role = p.get("type")
        explicit_team_pk = None  # if you later add parsing for participant.team
        team_pk = participant_team_pk(role, offense_pk, defense_pk, play_team_pk=play_team_pk, explicit_team_pk=explicit_team_pk)

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

            # DEFINE raw_payload HERE (once per endpoint)
            raw_payload = {
                "ref": sref,
                "ref_key": ref_key,
                "stat_type": stat_type,
                "splits_id": (payload.get("splits") or {}).get("id"),
                "athlete_espn_id": athlete_espn_id,
            }

            # Resolve payload_id ONCE
            payload_id = get_payload_id(cur, payload_cache, raw_payload)
            for stat_key, stat_value in extract_stats(payload):
                if not stat_allowed(stat_rules, stat_key):
                    continue
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
                    "payload_id": payload_id,
                })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="football")
    ap.add_argument("--league", default="nfl")
    ap.add_argument("--sleep", type=float)
    ap.add_argument("--event-id", action="append", default=[])
    ap.add_argument("--events-sql")
    ap.add_argument("--limit-events", type=int)
    ap.add_argument("--play-batch-size", type=int)
    ap.add_argument("--participant-batch-size", type=int)
    ap.add_argument("--stat-flags-csv")
    ap.add_argument("--workers", type=int, help="Number of parallel event workers")
    args = ap.parse_args()

    engine = get_engine(env_var="ESPN_DB_URI")

    # Load stat flags once
    print("Loading stat flags...")
    rules = load_stat_flags(args.stat_flags_csv)

    # Build team/athlete maps once (single short-lived connection)
    with engine.begin() as conn:
        pg_conn = conn.connection
        with pg_conn.cursor() as cur:
            team_map = build_team_lookup(cur)
            athlete_map = build_athlete_lookup(cur)

    # Resolve events (using a connection again)
    with engine.begin() as conn:
        pg_conn = conn.connection
        with pg_conn.cursor() as cur:
            event_ids = [str(x) for x in args.event_id if str(x).strip()]
            if args.events_sql:
                cur.execute(args.events_sql)
                event_ids.extend([str(r[0]) for r in cur.fetchall()])

    event_ids = list(dict.fromkeys(event_ids))
    if args.limit_events and args.limit_events > 0:
        event_ids = event_ids[: args.limit_events]

    if not event_ids:
        raise SystemExit("No events provided. Use --event-id ... or --events-sql ...")

    print(f"Processing {len(event_ids)} events with {args.workers} workers...", flush=True)

    workers = max(1, int(args.workers))
    results = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_event = {
            pool.submit(process_event, eid, engine, team_map, athlete_map, rules, args): eid
            for eid in event_ids
        }

        for fut in as_completed(future_to_event):
            eid = future_to_event[fut]
            try:
                event_id, plays, parts = fut.result()
                print(f"[event {event_id}] COMPLETED plays={plays} participants={parts}", flush=True)
                results.append((event_id, plays, parts))
            except Exception as e:
                print(f"[event {eid}] FAILED: {e}", flush=True)

    print("✅ finished", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
