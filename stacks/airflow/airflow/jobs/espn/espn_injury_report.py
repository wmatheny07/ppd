from __future__ import annotations

import os
import re
import json
import requests
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text


URL = "https://www.espn.com/nfl/injuries"
HTML_PATH = Path(__file__).with_name("injury_report_html_snip.html")


# -----------------------------
# Team normalization + abbrev
# -----------------------------
TEAM_ABBREV_MAP: Dict[str, str] = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WAS",
}

# Common variants you may see in HTML/logos/alt sources
TEAM_ALIASES: Dict[str, str] = {
    # Cities only
    "Arizona": "Arizona Cardinals",
    "Atlanta": "Atlanta Falcons",
    "Baltimore": "Baltimore Ravens",
    "Buffalo": "Buffalo Bills",
    "Carolina": "Carolina Panthers",
    "Chicago": "Chicago Bears",
    "Cincinnati": "Cincinnati Bengals",
    "Cleveland": "Cleveland Browns",
    "Dallas": "Dallas Cowboys",
    "Denver": "Denver Broncos",
    "Detroit": "Detroit Lions",
    "Green Bay": "Green Bay Packers",
    "Houston": "Houston Texans",
    "Indianapolis": "Indianapolis Colts",
    "Jacksonville": "Jacksonville Jaguars",
    "Kansas City": "Kansas City Chiefs",
    "Las Vegas": "Las Vegas Raiders",
    "L.A. Chargers": "Los Angeles Chargers",
    "LA Chargers": "Los Angeles Chargers",
    "Los Angeles Chargers": "Los Angeles Chargers",
    "L.A. Rams": "Los Angeles Rams",
    "LA Rams": "Los Angeles Rams",
    "Los Angeles Rams": "Los Angeles Rams",
    "Miami": "Miami Dolphins",
    "Minnesota": "Minnesota Vikings",
    "New England": "New England Patriots",
    "New Orleans": "New Orleans Saints",
    "NY Giants": "New York Giants",
    "New York Giants": "New York Giants",
    "NY Jets": "New York Jets",
    "New York Jets": "New York Jets",
    "Philadelphia": "Philadelphia Eagles",
    "Pittsburgh": "Pittsburgh Steelers",
    "San Francisco": "San Francisco 49ers",
    "Seattle": "Seattle Seahawks",
    "Tampa Bay": "Tampa Bay Buccaneers",
    "Tennessee": "Tennessee Titans",
    "Washington": "Washington Commanders",

    # Legacy names
    "Washington Football Team": "Washington Commanders",
    "Washington Redskins": "Washington Commanders",
}


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def normalize_team_name(team_raw: str) -> str:
    t = _clean_text(team_raw)
    if not t:
        return ""

    # Exact canonical
    if t in TEAM_ABBREV_MAP:
        return t

    # Alias map
    if t in TEAM_ALIASES:
        return TEAM_ALIASES[t]

    # Soft normalization for punctuation variants
    t2 = t.replace(".", "").replace("  ", " ").strip()
    if t2 in TEAM_ALIASES:
        return TEAM_ALIASES[t2]
    if t2 in TEAM_ABBREV_MAP:
        return t2

    # If we can't normalize, return cleaned original (still store it)
    return t


def team_to_abbrev(team_name: str) -> Optional[str]:
    team_norm = normalize_team_name(team_name)
    return TEAM_ABBREV_MAP.get(team_norm)


# -----------------------------
# HTML parse
# -----------------------------
def parse_espn_injury_html(html: str, slate_date: date | None = None) -> pd.DataFrame:
    slate_date = slate_date or date.today()
    soup = BeautifulSoup(html, "html.parser")

    rows = []

    for team_block in soup.select("div.ResponsiveTable.Table__league-injuries"):
        team_name = None

        name_node = team_block.select_one("span.injuries__teamName")
        if name_node:
            team_name = _clean_text(name_node.get_text(" ", strip=True))
        else:
            img = team_block.select_one("img[title]")
            if img and img.get("title"):
                team_name = _clean_text(img["title"])

        if not team_name:
            continue

        team_norm = normalize_team_name(team_name)
        team_abbrev = team_to_abbrev(team_norm)  # may be None if ESPN changes format

        for tr in team_block.select("tbody.Table__TBODY tr.Table__TR"):
            player_td = tr.select_one("td.col-name")
            pos_td = tr.select_one("td.col-pos") or tr.select_one("td.col-position")
            date_td = tr.select_one("td.col-date")
            status_td = tr.select_one("td.col-stat") or tr.select_one("td.col-status")

            if not player_td:
                continue

            a = player_td.select_one("a[href]")
            player_name = _clean_text(a.get_text(" ", strip=True) if a else player_td.get_text(" ", strip=True))
            player_href = a.get("href") if a else None

            position = _clean_text(pos_td.get_text(" ", strip=True)) if pos_td else ""
            estimated_return = _clean_text(date_td.get_text(" ", strip=True)) if date_td else ""
            status = _clean_text(status_td.get_text(" ", strip=True)) if status_td else ""

            if not player_name:
                continue

            rows.append(
                {
                    "slate_date": slate_date,
                    "team": team_norm or team_name,
                    "team_abbrev": team_abbrev,
                    "player_name": player_name,
                    "position": position,
                    "status": status,
                    "estimated_return": estimated_return,
                    "player_href": player_href,
                    "source": "espn",
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["player_name"] = df["player_name"].astype("string").str.strip()
        df["team"] = df["team"].astype("string").str.strip()
        df["position"] = df["position"].astype("string").str.strip()
        df["status"] = df["status"].astype("string").str.strip()
        df["estimated_return"] = df["estimated_return"].astype("string").str.strip()
        df["player_href"] = df["player_href"].astype("string")
        df["team_abbrev"] = df["team_abbrev"].astype("string")
    return df


def fetch_espn_injuries(url: str = URL) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


# -----------------------------
# Postgres helpers (PFN-style)
# -----------------------------
def safe_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name or ""):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def get_engine() -> "sqlalchemy.Engine":
    """
    Prefers explicit DB_* env vars.
    (You can keep your Airflow-conn approach too, but env is simplest for BashOperator.)
    """
    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "5432").strip()
    db = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    pwd = os.getenv("DB_PASSWORD", "").strip()

    if not all([host, db, user, pwd]):
        raise RuntimeError("Missing DB env vars (DB_HOST/DB_NAME/DB_USER/DB_PASSWORD).")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_schema(engine, schema: str):
    schema = safe_ident(schema)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

def ensure_table(engine, schema: str):
    ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".injury_report_current (
        id BIGSERIAL PRIMARY KEY,
        ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

        slate_date DATE NOT NULL,

        team TEXT NOT NULL,
        team_abbrev TEXT,

        player_name TEXT NOT NULL,
        player_name_norm TEXT GENERATED ALWAYS AS (lower(player_name)) STORED,

        position TEXT,
        status TEXT,
        estimated_return TEXT,
        player_href TEXT,
        source TEXT NOT NULL DEFAULT 'espn',

        -- normalize nullable fields for uniqueness
        status_key TEXT GENERATED ALWAYS AS (COALESCE(status,'')) STORED,
        position_key TEXT GENERATED ALWAYS AS (COALESCE(position,'')) STORED,
        estimated_return_key TEXT GENERATED ALWAYS AS (COALESCE(estimated_return,'')) STORED,

        UNIQUE (slate_date, team, player_name, status_key, position_key, estimated_return_key)
        );
    """

    idx = [
        f'CREATE INDEX IF NOT EXISTS injury_slate_idx ON "{schema}".injury_report_current (slate_date);',
        f'CREATE INDEX IF NOT EXISTS injury_team_idx ON "{schema}".injury_report_current (slate_date, team_abbrev);',
        f'CREATE INDEX IF NOT EXISTS injury_player_idx ON "{schema}".injury_report_current (slate_date, player_name_norm);',
    ]

    with engine.begin() as conn:
        conn.execute(text(ddl))
        for s in idx:
            conn.execute(text(s))


def ingest_df(engine, df: pd.DataFrame, schema: str):
    schema = safe_ident(schema)
    ensure_schema(engine, schema)
    ensure_table(engine, schema)

    if df is None or df.empty:
        print("[espn-injury] no rows parsed; skipping ingest.")
        return

    tmp_table = "injury_report_tmp"

    # Write temp table into same schema
    df.to_sql(tmp_table, engine, schema=schema, if_exists="replace", index=False)

    upsert_sql = f"""
        INSERT INTO "{schema}".injury_report_current (
            slate_date, team, team_abbrev, player_name, position, status, estimated_return, player_href, source
        )
        SELECT
            slate_date, team, team_abbrev, player_name, position, status, estimated_return, player_href, source
        FROM "{schema}"."{tmp_table}"
        ON CONFLICT (slate_date, team, player_name, status_key, position_key, estimated_return_key)
        DO UPDATE SET
            team_abbrev      = EXCLUDED.team_abbrev,
            position         = EXCLUDED.position,
            status           = EXCLUDED.status,
            estimated_return = EXCLUDED.estimated_return,
            player_href      = EXCLUDED.player_href,
            source           = EXCLUDED.source;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp_table}";'))

    print(f"[espn-injury] upserted rows={len(df)} into {schema}.injury_report_current")


# -----------------------------
# Entrypoint
# -----------------------------
def main():
    schema = os.getenv("INJURY_SCHEMA", os.getenv("NFL_DFS_SCHEMA", "nfl_dfs")).strip()
    source_url = os.getenv("INJURY_URL", URL).strip()
    slate = os.getenv("SLATE_DATE", "").strip()  # optional YYYY-MM-DD

    slate_date = datetime.strptime(slate, "%Y-%m-%d").date() if slate else date.today()

    # Fetch + parse
    html = fetch_espn_injuries(source_url)
    df = parse_espn_injury_html(html, slate_date=slate_date)

    # Show quick summary for logs
    print(df.head(20))
    print(f"[espn-injury] rows={len(df)} teams={df['team'].nunique() if not df.empty else 0}")

    # Ingest
    engine = get_engine()
    ingest_df(engine, df, schema=schema)


if __name__ == "__main__":
    main()