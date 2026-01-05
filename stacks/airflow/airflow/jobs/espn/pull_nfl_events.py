#!/usr/bin/env python3
import argparse
import json
import os
import time
from datetime import datetime

import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text

CORE_BASE = "http://sports.core.api.espn.com/v2"


def session_with_retries():
    retry = Retry(
        total=8,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "ppd-espn-events/1.0", "Accept": "application/json"})
    return s


def get_engine():
    host = os.getenv("ESPN_DB_HOST", "").strip()
    port = os.getenv("ESPN_DB_PORT", "5432").strip()
    db = os.getenv("ESPN_DB_NAME", "").strip()
    user = os.getenv("ESPN_DB_USER", "").strip()
    pwd = os.getenv("ESPN_DB_PASSWORD", "").strip()

    if not all([host, db, user, pwd]):
        raise RuntimeError(
            "Missing DB env vars (ESPN_DB_HOST/ESPN_DB_NAME/ESPN_DB_USER/ESPN_DB_PASSWORD)."
        )

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, execution_options={"stream_results": True})


def get_json(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def events_url(season: int, season_type: int, week: int):
    return (
        f"{CORE_BASE}/sports/football/leagues/nfl/"
        f"seasons/{season}/types/{season_type}/weeks/{week}/events"
        f"?lang=en&region=us"
    )


UPSERT_SQL = """
INSERT INTO public.espn_event (
  created_at, updated_at,
  espn_id, uid,
  "date", "name", short_name,
  season_year, season_type, season_slug,
  week,
  status, status_detail, clock, "period",
  attendance,
  broadcasts, links, raw_data,
  league_id, venue_id
) VALUES %s
ON CONFLICT (league_id, espn_id) DO UPDATE SET
  updated_at = now(),
  uid = EXCLUDED.uid,
  "date" = EXCLUDED."date",
  "name" = EXCLUDED."name",
  short_name = EXCLUDED.short_name,
  season_year = EXCLUDED.season_year,
  season_type = EXCLUDED.season_type,
  season_slug = EXCLUDED.season_slug,
  week = EXCLUDED.week,
  status = EXCLUDED.status,
  status_detail = EXCLUDED.status_detail,
  clock = EXCLUDED.clock,
  "period" = EXCLUDED."period",
  attendance = EXCLUDED.attendance,
  broadcasts = EXCLUDED.broadcasts,
  links = EXCLUDED.links,
  raw_data = EXCLUDED.raw_data,
  venue_id = EXCLUDED.venue_id;
"""


def pick_status_fields(event: dict) -> tuple[str, str, str, int | None]:
    """
    Returns: (status, status_detail, clock, period)
    ESPN event JSON may vary; we defensively pick common locations.
    """
    # Common shape: event.status.type.name / event.status.type.detail / event.status.displayClock / event.status.period
    st = event.get("status") or {}
    st_type = st.get("type") or {}

    status = (st_type.get("name") or st_type.get("state") or "").strip() or "UNKNOWN"
    status_detail = (
        (st_type.get("detail") or st_type.get("description") or st.get("type", {}).get("detail") or "")
    ).strip()

    clock = (st.get("displayClock") or st.get("clock") or "").strip()
    period = st.get("period")
    try:
        period = int(period) if period is not None else None
    except Exception:
        period = None

    # enforce NOT NULLs
    if not status_detail:
        status_detail = status
    if not clock:
        clock = "0:00"

    # truncate to fit your DDL sizes if needed
    return status[:20], status_detail[:100], clock[:20], period


def infer_short_name(event: dict) -> str:
    # prefer explicit shortName
    sn = (event.get("shortName") or "").strip()
    if sn:
        return sn[:100]
    # fallback: name truncated
    nm = (event.get("name") or "").strip()
    return (nm[:100] if nm else "UNKNOWN")


def infer_season_slug(season_type: int) -> str:
    # You can refine this, but keeps schema satisfied.
    return {1: "preseason", 2: "regular-season", 3: "postseason"}.get(season_type, f"type-{season_type}")[:50]


def lookup_league_id(conn, league_slug: str = "nfl") -> int:
    """
    Adjust this query if your espn_league schema uses different columns.
    The goal: get the internal PK for NFL.
    """
    # Common possibilities: slug='nfl' or abbreviation='NFL' or name='National Football League'
    row = conn.execute(
        text("""
            SELECT id
            FROM public.espn_league
            WHERE lower(slug) = :slug
               OR lower(abbreviation) = :slug
               OR lower(name) = :slug
            ORDER BY id
            LIMIT 1
        """),
        {"slug": league_slug.lower()},
    ).fetchone()
    if not row:
        raise RuntimeError("Could not find league_id in public.espn_league for slug='nfl'.")
    return int(row[0])


def venue_pk_from_venue_ref(conn, venue_ref: str) -> int | None:
    """
    Optional: if you ingest venues into public.espn_venue with espn_id,
    this maps the venue ref -> venue espn_id -> your internal venue PK.
    """
    if not venue_ref:
        return None
    # venue_ref looks like .../venues/3970?lang=...
    try:
        espn_vid = venue_ref.split("/venues/")[1].split("?")[0].split("/")[0]
    except Exception:
        return None

    row = conn.execute(
        text("SELECT id FROM public.espn_venue WHERE espn_id = :espn_id LIMIT 1"),
        {"espn_id": str(espn_vid)},
    ).fetchone()
    return int(row[0]) if row else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--season-type", type=int, default=2)
    ap.add_argument("--week-start", type=int, required=True)
    ap.add_argument("--week-end", type=int, required=True)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--league-id", type=int, default=0, help="Optional: override espn_league.id lookup")
    args = ap.parse_args()

    engine = get_engine()
    session = session_with_retries()

    rows = []

    with engine.begin() as conn:
        league_id = args.league_id or lookup_league_id(conn, "nfl")

        for week in range(args.week_start, args.week_end + 1):
            url = events_url(args.season, args.season_type, week)
            print(f"📅 Fetching season={args.season} type={args.season_type} week={week}")
            data = get_json(session, url)

            for item in data.get("items", []):
                ref = item.get("$ref")
                if not ref:
                    continue

                event = get_json(session, ref)

                espn_id = str(event.get("id") or "")
                uid = str(event.get("uid") or "").strip()
                if not uid:
                    # keep NOT NULL satisfied; uid usually exists but just in case
                    uid = f"s:20~l:28~e:{espn_id}" if espn_id else "unknown"

                name = (event.get("name") or "").strip()
                if not name:
                    name = "UNKNOWN"

                short_name = infer_short_name(event)

                # event["date"] is ISO; psycopg2 can coerce string -> timestamptz
                date_val = event.get("date")
                if not date_val:
                    # keep NOT NULL satisfied; you can also choose to skip instead
                    date_val = datetime.utcnow().isoformat() + "Z"

                status, status_detail, clock, period = pick_status_fields(event)

                attendance = event.get("attendance")
                try:
                    attendance = int(attendance) if attendance is not None else None
                except Exception:
                    attendance = None

                broadcasts = event.get("broadcasts") or []
                links = event.get("links") or []

                # Optional venue mapping (requires venues ingested)
                venue_ref = ""
                # event may have competitions -> venue
                comps = event.get("competitions") or []
                if comps and isinstance(comps, list):
                    venue = (comps[0].get("venue") or {}) if isinstance(comps[0], dict) else {}
                    venue_ref = venue.get("$ref") or ""
                venue_id = venue_pk_from_venue_ref(conn, venue_ref) if venue_ref else None

                season_slug = infer_season_slug(args.season_type)

                rows.append(
                    (
                        datetime.utcnow(),  # created_at
                        datetime.utcnow(),  # updated_at
                        espn_id,
                        uid,
                        date_val,
                        name[:200],
                        short_name[:100],
                        int(args.season),
                        int(args.season_type),
                        season_slug[:50],
                        int(week),
                        status,
                        status_detail,
                        clock,
                        period,
                        attendance,
                        json.dumps(broadcasts),
                        json.dumps(links),
                        json.dumps(event),
                        int(league_id),
                        venue_id,
                    )
                )

                time.sleep(args.sleep)

        if not rows:
            print("⚠️ No events found")
            return 0

        raw = conn.connection  # DBAPI connection (psycopg2)
        with raw.cursor() as cur:
            execute_values(
                cur,
                UPSERT_SQL,
                rows,
                page_size=200,
            )

    print(f"✅ Upserted {len(rows)} events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())