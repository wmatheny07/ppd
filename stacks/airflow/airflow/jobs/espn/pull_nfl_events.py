#!/usr/bin/env python3
import argparse
import json
import os
import time
from datetime import datetime
from urllib.parse import urlencode

import requests
import psycopg2
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    s.headers.update(
        {"User-Agent": "ppd-espn-events/1.0", "Accept": "application/json"}
    )
    return s


def pg_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def events_url(season: int, week: int):
    return (
        f"{CORE_BASE}/sports/football/leagues/nfl/"
        f"seasons/{season}/types/2/weeks/{week}/events"
        f"?lang=en&region=us"
    )


def get_json(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


UPSERT_SQL = """
INSERT INTO espn_event (
    espn_id,
    season,
    week,
    name,
    date_utc,
    status,
    raw_data,
    created_at,
    updated_at
)
VALUES %s
ON CONFLICT (espn_id) DO UPDATE SET
    season = EXCLUDED.season,
    week = EXCLUDED.week,
    name = EXCLUDED.name,
    date_utc = EXCLUDED.date_utc,
    status = EXCLUDED.status,
    raw_data = EXCLUDED.raw_data,
    updated_at = now();
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--week-start", type=int, required=True)
    ap.add_argument("--week-end", type=int, required=True)
    ap.add_argument("--sleep", type=float, default=0.2)
    args = ap.parse_args()

    session = session_with_retries()
    rows = []

    for week in range(args.week_start, args.week_end + 1):
        url = events_url(args.season, week)
        print(f"📅 Fetching season={args.season} week={week}")
        data = get_json(session, url)

        for item in data.get("items", []):
            ref = item.get("$ref")
            if not ref:
                continue

            event = get_json(session, ref)
            rows.append(
                (
                    str(event.get("id")),
                    args.season,
                    week,
                    event.get("name"),
                    event.get("date"),
                    event.get("status", {}).get("type", {}).get("name"),
                    json.dumps(event),
                    datetime.utcnow(),
                    datetime.utcnow(),
                )
            )
            time.sleep(args.sleep)

    if not rows:
        print("⚠️ No events found")
        return 0

    with pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=100)
        conn.commit()

    print(f"✅ Upserted {len(rows)} events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
