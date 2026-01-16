import os
import re
from datetime import date, datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

from sqlalchemy import create_engine, text

# ✅ Airflow connection
from airflow.hooks.base import BaseHook

URL_DEFAULT = "https://www.profootballnetwork.com/nfl/depth-chart/"

TEAM_ABBREV = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL", "Baltimore Ravens": "BAL", "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR", "Chicago Bears": "CHI", "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL", "Denver Broncos": "DEN", "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND", "Jacksonville Jaguars": "JAX", "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC", "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN", "New England Patriots": "NE", "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT", "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB", "Tennessee Titans": "TEN", "Washington Commanders": "WAS",
}

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

import os
from sqlalchemy import create_engine

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

def ensure_schema_and_table(engine, schema: str):
    schema = schema.strip()

    table_ddl = f"""
    CREATE SCHEMA IF NOT EXISTS "{schema}";

    CREATE TABLE IF NOT EXISTS "{schema}".depth_chart_current (
      slate_date DATE NOT NULL,
      team TEXT NOT NULL,
      team_abbrev TEXT,
      position_group TEXT NOT NULL,
      depth_rank INT NOT NULL,
      player_name TEXT NOT NULL,
      player_name_norm TEXT GENERATED ALWAYS AS (lower(player_name)) STORED,
      player_slug TEXT,
      player_href TEXT,
      source TEXT NOT NULL DEFAULT 'pfn',
      ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (slate_date, team, position_group, depth_rank, player_name)
    );

    CREATE INDEX IF NOT EXISTS depth_chart_team_idx
      ON "{schema}".depth_chart_current (slate_date, team_abbrev);

    CREATE INDEX IF NOT EXISTS depth_chart_player_norm_idx
      ON "{schema}".depth_chart_current (slate_date, team_abbrev, player_name_norm);
    """

    with engine.begin() as conn:
        conn.execute(text(table_ddl))

def scrape_pfn_depth_chart(url: str, slate_date: date) -> pd.DataFrame:
    html = requests.get(url, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    for team_block in soup.select("div.team-depth-chart-section"):
        team = _clean_text(team_block.select_one("div.team-depth-header div.title").get_text(" ", strip=True))
        team_abbrev = TEAM_ABBREV.get(team)

        for pos_block in team_block.select("div.depth-content div.position-content"):
            pos = _clean_text(pos_block.select_one("div.position-title").get_text(" ", strip=True))

            players = []
            for pnode in pos_block.select("div.player-names-section div.player-name"):
                a = pnode.select_one("a")
                name = _clean_text(a.get_text(" ", strip=True) if a else pnode.get_text(" ", strip=True))
                href = a.get("href") if a else None
                if name:
                    players.append((name, href))

            for depth_rank, (player_name, href) in enumerate(players, start=1):
                slug = href.split("/players/")[-1] if href and "/players/" in href else None
                rows.append({
                    "slate_date": slate_date,
                    "team": team,
                    "team_abbrev": team_abbrev,
                    "position_group": pos,
                    "depth_rank": depth_rank,
                    "player_name": player_name,
                    "player_slug": slug,
                    "player_href": href,
                    "source": "pfn",
                })

    return pd.DataFrame(rows)

def upsert_depth(engine, schema: str, df: pd.DataFrame):
    if df.empty:
        print("[pfn] no rows scraped; nothing to upsert")
        return

    tmp = "depth_chart_tmp"
    df.to_sql(tmp, engine, schema=schema, if_exists="replace", index=False)

    sql = f"""
    INSERT INTO "{schema}".depth_chart_current (
      slate_date, team, team_abbrev, position_group, depth_rank,
      player_name, player_slug, player_href, source
    )
    SELECT
      slate_date, team, team_abbrev, position_group, depth_rank,
      player_name, player_slug, player_href, source
    FROM "{schema}"."{tmp}"
    ON CONFLICT (slate_date, team, position_group, depth_rank, player_name)
    DO UPDATE SET
      team_abbrev = EXCLUDED.team_abbrev,
      player_slug = EXCLUDED.player_slug,
      player_href = EXCLUDED.player_href,
      source      = EXCLUDED.source,
      ingest_ts   = now();
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp}";'))

def main():
    engine = get_engine()
    schema  = os.getenv("DEPTH_SCHEMA", "nfl_dfs").strip()
    source  = os.getenv("DEPTH_SOURCE", "pfn").strip()
    url     = os.getenv("DEPTH_URL", URL_DEFAULT).strip()

    slate = os.getenv("SLATE_DATE")
    slate_date = datetime.strptime(slate, "%Y-%m-%d").date() if slate else date.today()

    if source != "pfn":
        raise ValueError(f"Unsupported DEPTH_SOURCE={source!r} (only 'pfn' supported here)")

    ensure_schema_and_table(engine, schema)

    df = scrape_pfn_depth_chart(url=url, slate_date=slate_date)
    print(df.head(20))
    print(f"rows={len(df)} teams={df['team'].nunique() if not df.empty else 0}")

    upsert_depth(engine, schema, df)
    print(f"[pfn] upserted into {schema}.depth_chart_current")

if __name__ == "__main__":
    main()
