#!/usr/bin/env python3
import argparse
import json
import os
import re
import time
from datetime import datetime

import requests
import psycopg2
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_batch
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
        {"User-Agent": "ppd-espn-athletes/1.0", "Accept": "application/json"}
    )
    return s

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


def athletes_index_url(season: int, page: int):
    return (
        f"{CORE_BASE}/sports/football/leagues/nfl/"
        f"seasons/{season}/athletes"
        f"?page={page}&lang=en&region=us"
    )


def get_json(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def athlete_id_from_ref(ref: str) -> str:
    m = re.search(r"/athletes/(\d+)", ref)
    if not m:
        raise ValueError(ref)
    return m.group(1)


UPSERT_SQL = """
INSERT INTO espn_athlete (
    espn_id,
    uid,
    first_name,
    last_name,
    full_name,
    display_name,
    short_name,
    position,
    position_abbreviation,
    jersey,
    is_active,
    height,
    weight,
    age,
    birth_date,
    birth_place,
    headshot,
    links,
    raw_data,
    created_at,
    updated_at
)
VALUES (
    %(espn_id)s, %(uid)s, %(first_name)s, %(last_name)s,
    %(full_name)s, %(display_name)s, %(short_name)s,
    %(position)s, %(position_abbreviation)s,
    %(jersey)s, %(is_active)s,
    %(height)s, %(weight)s, %(age)s,
    %(birth_date)s, %(birth_place)s,
    %(headshot)s, %(links)s::jsonb, %(raw_data)s::jsonb,
    now(), now()
)
ON CONFLICT (espn_id) DO UPDATE SET
    updated_at = now(),
    uid = EXCLUDED.uid,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    full_name = EXCLUDED.full_name,
    display_name = EXCLUDED.display_name,
    short_name = EXCLUDED.short_name,
    position = EXCLUDED.position,
    position_abbreviation = EXCLUDED.position_abbreviation,
    jersey = EXCLUDED.jersey,
    is_active = EXCLUDED.is_active,
    height = EXCLUDED.height,
    weight = EXCLUDED.weight,
    age = EXCLUDED.age,
    birth_date = EXCLUDED.birth_date,
    birth_place = EXCLUDED.birth_place,
    headshot = EXCLUDED.headshot,
    links = EXCLUDED.links,
    raw_data = EXCLUDED.raw_data;
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--sleep", type=float, default=0.1)
    args = ap.parse_args()
    engine = get_engine()

    session = session_with_retries()
    page = 1
    processed = 0

    BATCH_SIZE = 250  # tune

    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            while True:
                index = get_json(session, athletes_index_url(args.season, page))
                items = index.get("items", [])
                if not items:
                    print(f"✅ No more items; stopping at page={page}")
                    break

                batch = []
                for it in items:
                    ref = it.get("$ref")
                    if not ref:
                        continue
                    athlete = get_json(session, ref)

                    batch.append({
                        "espn_id": str(athlete.get("id")),
                        "uid": athlete.get("uid", ""),
                        "first_name": athlete.get("firstName", ""),
                        "last_name": athlete.get("lastName", ""),
                        "full_name": athlete.get("fullName", ""),
                        "display_name": athlete.get("displayName", ""),
                        "short_name": athlete.get("shortName", ""),
                        "position": (athlete.get("position") or {}).get("name", ""),
                        "position_abbreviation": (athlete.get("position") or {}).get("abbreviation", ""),
                        "jersey": str(athlete.get("jersey", "")),
                        "is_active": bool(athlete.get("active", True)),
                        "height": athlete.get("displayHeight", ""),
                        "weight": athlete.get("weight"),
                        "age": athlete.get("age"),
                        "birth_date": athlete.get("dateOfBirth"),
                        "birth_place": ", ".join(
                            filter(None, [
                                (athlete.get("birthPlace") or {}).get("city"),
                                (athlete.get("birthPlace") or {}).get("state"),
                                (athlete.get("birthPlace") or {}).get("country"),
                            ])
                        ),
                        "headshot": (athlete.get("headshot") or {}).get("href", ""),
                        "links": json.dumps(athlete.get("links", [])),
                        "raw_data": json.dumps(athlete),
                    })

                    if args.sleep:
                        time.sleep(args.sleep)

                execute_batch(cur, UPSERT_SQL, batch, page_size=BATCH_SIZE)
                processed += len(batch)
                print(f"📦 Page {page} upserted={len(batch)} processed_total={processed}")
                page += 1

    print(f"✅ Finished athletes ingest: {processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
