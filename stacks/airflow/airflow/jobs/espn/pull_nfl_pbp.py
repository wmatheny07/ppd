#!/usr/bin/env python3
import argparse
import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

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
def pg_conn():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "espn"),
        user=os.getenv("PGUSER", "analytics"),
        password=os.getenv("ANALYTICS_PASSWORD", ""),
    )

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
  raw_data = EXCLUDED.raw_data
RETURNING id;
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
        "scoring_play": play_obj.get("scoringPlay"),
        "priority": play_obj.get("priority"),
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

def upsert_participants(cur, play_db_id: int, play_obj: Dict[str, Any],
                        team_map: Dict[str, int], athlete_map: Dict[str, int]):
    # Build a "best guess" team for participant:
    # 1) if their athlete exists in espn_athlete with team_id already set, use that (optional)
    # 2) else use play.team_id as fallback
    # For now: use play.team_id fallback only.
    play_team_ref = ((play_obj.get("team") or {}).get("$ref")) or ""
    play_team_espn = espn_id_from_ref(play_team_ref)
    play_team_pk = team_map.get(str(play_team_espn)) if play_team_espn else None

    for p in (play_obj.get("participants") or []):
        aref = ((p.get("athlete") or {}).get("$ref")) or ""
        aid = espn_id_from_ref(aref)

        athlete_pk = athlete_map.get(str(aid)) if aid else None

        params = {
            "play_id": play_db_id,
            "athlete_id": athlete_pk,
            "team_id": play_team_pk,
            "role_type": p.get("type"),
            "order": p.get("order"),
            "athlete_espn_id": str(aid) if aid else None,
            "team_espn_id": str(play_team_espn) if play_team_espn else None,
            "position_ref": ((p.get("position") or {}).get("$ref")),
            "statistics_ref": ((p.get("statistics") or {}).get("$ref")),
            "play_statistics_ref": ((p.get("playStatistics") or {}).get("$ref")),
            "raw_data": json.dumps(p),
        }
        cur.execute(UPSERT_PARTICIPANT_SQL, params)


# -----------------------
# Main
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="football")
    ap.add_argument("--league", default="nfl")
    ap.add_argument("--sleep", type=float, default=0.05)

    # input events:
    ap.add_argument("--event-id", action="append", default=[], help="One or more event ids (repeatable).")
    ap.add_argument("--events-sql", default="", help="SQL that returns one column of event_espn_id values.")
    ap.add_argument("--limit-events", type=int, default=0)

    args = ap.parse_args()
    s = session_with_retries()

    with pg_conn() as conn, conn.cursor() as cur:
        team_map = build_team_lookup(cur)
        athlete_map = build_athlete_lookup(cur)

        # resolve events
        event_ids = [str(x) for x in args.event_id if str(x).strip()]
        if args.events_sql:
            cur.execute(args.events_sql)
            event_ids.extend([str(r[0]) for r in cur.fetchall()])

        # de-dupe
        event_ids = list(dict.fromkeys(event_ids))
        if args.limit_events and args.limit_events > 0:
            event_ids = event_ids[: args.limit_events]

        if not event_ids:
            raise SystemExit("No events provided. Use --event-id ... or --events-sql ...")

        plays_processed = 0
        parts_processed = 0

        for event_id in event_ids:
            url = plays_url(args.sport, args.league, event_id)
            print(f"[event {event_id}] plays: {url}")

            for pref in iter_play_refs(s, url):
                play_obj = get_json(s, pref)

                play_row = parse_play_row(play_obj, event_id, team_map)

                cur.execute(UPSERT_PLAY_SQL, play_row)
                play_db_id = cur.fetchone()[0]
                plays_processed += 1

                before = parts_processed
                upsert_participants(cur, play_db_id, play_obj, team_map, athlete_map)
                # we can’t easily count rows inserted w/out RETURNING; rough count:
                parts_processed += len(play_obj.get("participants") or [])

                if args.sleep:
                    time.sleep(args.sleep)

                if plays_processed % 500 == 0:
                    conn.commit()
                    print(f"  committed at plays={plays_processed} participants≈{parts_processed}")

            conn.commit()
            print(f"[event {event_id}] done. plays={plays_processed} participants≈{parts_processed}")

    print("✅ finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
