import os
import time
import shutil
from pathlib import Path
from datetime import datetime, date
import traceback
from sqlalchemy import create_engine, text
import json
import re
import pandas as pd
import argparse

RUN_ONCE = os.getenv("DK_RUN_ONCE", "false").lower() in ("1", "true", "yes", "y", "on")
INBOX = Path(os.getenv("DK_INBOX", "/data/inbox"))
ARCHIVE = Path(os.getenv("DK_ARCHIVE", "/data/archive"))
ERROR = Path(os.getenv("DK_ERROR", "/data/error"))
POLL_SECONDS = int(os.getenv("DK_POLL_SECONDS", "60"))

# ✅ schema per project
DK_SCHEMA = os.getenv("DK_SCHEMA", "public").strip()

FILE_GLOBS = ["DKSalaries*.csv", "DraftKings*.csv", "*.csv"]

DK_ID_REGEX = re.compile(r"\((\d+)\)\s*$")

def process_available_files(engine) -> int:
    candidates = find_candidates()
    if not candidates:
        return 0

    processed = 0
    for csv_path in sorted(candidates, key=lambda p: p.stat().st_mtime):
        try:
            print(f"[dk-ingest] ingesting {csv_path.name} -> schema {DK_SCHEMA}")
            ingest_one(engine, csv_path, DK_SCHEMA)

            dest = archive_path(ARCHIVE, csv_path)
            shutil.move(str(csv_path), str(dest))
            print(f"[dk-ingest] archived -> {dest.name}")
            processed += 1
        except Exception as e:
            print(f"[dk-ingest] ERROR on {csv_path.name}: {e!r}")
            dest = archive_path(ERROR, csv_path)
            shutil.move(str(csv_path), str(dest))
            print(f"[dk-ingest] moved to error -> {dest.name}")

    return processed

def extract_dk_player_id(row, id_col=None, name_id_col=None):
    """
    Prefer explicit ID column; fallback to parsing Name + ID.
    Returns Int64 (nullable).
    """
    # Case 1: explicit ID column
    if id_col and pd.notna(row.get(id_col)):
        try:
            return int(row[id_col])
        except Exception:
            pass

    # Case 2: parse from Name + ID
    if name_id_col and isinstance(row.get(name_id_col), str):
        m = DK_ID_REGEX.search(row[name_id_col])
        if m:
            return int(m.group(1))

    return pd.NA

def to_jsonb_safe(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str):
        return v  # already serialized or plain text
    # handles dict/list + numpy types + timestamps
    return json.dumps(v, ensure_ascii=False, default=str)

def _debug_params(obj, label):
    try:
        print(f"[dk-ingest][debug] {label} type={type(obj)}")
        if isinstance(obj, dict):
            print(f"[dk-ingest][debug] {label} dict keys={list(obj.keys())[:20]}")
        elif isinstance(obj, list):
            print(f"[dk-ingest][debug] {label} list len={len(obj)} first_type={type(obj[0]) if obj else None}")
            if obj and isinstance(obj[0], dict):
                print(f"[dk-ingest][debug] {label} first dict keys={list(obj[0].keys())[:20]}")
    except Exception as e:
        print(f"[dk-ingest][debug] failed inspecting {label}: {e}")

def safe_ident(name: str) -> str:
    """
    Minimal identifier sanitizer to avoid accidental injection via env.
    Allows letters, numbers, underscore. Must start with letter/_.
    """
    import re
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

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

def ensure_schema(engine, schema: str):
    schema = safe_ident(schema)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

def ensure_table(engine, schema: str):
    schema = safe_ident(schema)

    table_ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}".dk_salaries (
      id BIGSERIAL PRIMARY KEY,
      ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),

      -- Slate identity
      slate_date DATE NOT NULL,
      source_file TEXT NOT NULL,

      -- DK identity
      dk_player_id BIGINT,
      name_id TEXT NOT NULL,
      name TEXT,

      -- Roster / salary
      position TEXT,
      roster_position TEXT,
      salary INTEGER CHECK (salary >= 0),

      -- Game context
      game_info TEXT,
      team_abbrev TEXT,
      avg_points_per_game NUMERIC,

      -- Raw row from CSV
      raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,

      -- Natural key component (generated)
      dk_key TEXT GENERATED ALWAYS AS (COALESCE(dk_player_id::text, name_id)) STORED,

      CONSTRAINT dk_salaries_uniq UNIQUE (slate_date, source_file, dk_key, roster_position, position)
    );
    """

    index_ddls = [
        f'CREATE INDEX IF NOT EXISTS dk_salaries_slate_idx ON "{schema}".dk_salaries (slate_date);',
        f'CREATE INDEX IF NOT EXISTS dk_salaries_player_idx ON "{schema}".dk_salaries (dk_player_id);',
        f'CREATE INDEX IF NOT EXISTS dk_salaries_team_idx ON "{schema}".dk_salaries (slate_date, team_abbrev);',
        f'CREATE INDEX IF NOT EXISTS dk_salaries_raw_gin_idx ON "{schema}".dk_salaries USING GIN (raw);',
    ]

    with engine.begin() as conn:
        conn.execute(text(table_ddl))
        for ddl_stmt in index_ddls:
            conn.execute(text(ddl_stmt))

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {c.lower().strip(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n in colmap:
                return colmap[n]
        return None

    c_position = pick("position")
    c_name = pick("name")
    c_name_id = pick("name + id", "name+id", "name_id", "nameid")
    c_roster = pick("roster position", "rosterposition")
    c_salary = pick("salary")
    c_game = pick("game info", "gameinfo")
    c_team = pick("teamabbrev", "team abbrev", "team")
    c_avg = pick("avgpointspergame", "avg points per game", "avg points/game")
    c_id = pick("id")  # DK numeric id (sometimes present)

    out = pd.DataFrame()

    out["position"] = df[c_position] if c_position else None
    out["name"] = df[c_name] if c_name else None
    out["name_id"] = df[c_name_id] if c_name_id else None
    out["roster_position"] = df[c_roster] if c_roster else None
    out["salary"] = pd.to_numeric(df[c_salary], errors="coerce").astype("Int64") if c_salary else None
    out["game_info"] = df[c_game] if c_game else None
    out["team_abbrev"] = df[c_team] if c_team else None
    out["avg_points_per_game"] = pd.to_numeric(df[c_avg], errors="coerce") if c_avg else None

    # ⭐ NEW: dk_player_id
    out["dk_player_id"] = df.apply(
        extract_dk_player_id,
        axis=1,
        id_col=c_id,
        name_id_col=c_name_id
    ).astype("Int64")

    # Ensure name_id is never NULL (DDL requires NOT NULL)
    # Prefer original Name + ID, else synthesize "Name (ID)", else fallback to Name.
    if "name_id" in out.columns:
        out["name_id"] = out["name_id"].fillna(pd.NA)

    synth = pd.Series(pd.NA, index=out.index, dtype="string")

    if "name" in out.columns and "dk_player_id" in out.columns:
        synth = out["name"].astype("string").str.strip() + " (" + out["dk_player_id"].astype("Int64").astype("string") + ")"

    # If name_id missing/blank, fill from synthesized or name
    out["name_id"] = (
        out.get("name_id", pd.Series(pd.NA, index=out.index, dtype="string"))
          .astype("string")
          .str.strip()
          .replace("", pd.NA)
          .fillna(synth.replace("<NA> (<NA>)", pd.NA))   # guard bad synth
          .fillna(out.get("name", pd.Series(pd.NA, index=out.index, dtype="string")).astype("string").str.strip())
          .fillna("UNKNOWN")
    )

    # normalize strings
    for c in ["position", "name", "name_id", "roster_position", "game_info", "team_abbrev"]:
        if c in out.columns:
            out[c] = out[c].astype("string").str.strip()

    return out

def infer_slate_date_from_filename(path: Path) -> date:
    stem = path.stem
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        for token in stem.replace("_","-").split("-"):
            try:
                return datetime.strptime(token, fmt).date()
            except Exception:
                pass
    return date.today()

def archive_path(dest_dir: Path, src: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dest_dir / f"{src.stem}__{ts}{src.suffix}"

def ingest_one(engine, csv_path: Path, schema: str):
    schema = safe_ident(schema)
    ensure_schema(engine, schema)
    ensure_table(engine, schema)

    slate_date = infer_slate_date_from_filename(csv_path)
    df = pd.read_csv(csv_path)

    norm = normalize_columns(df)
    norm["slate_date"] = slate_date
    norm["source_file"] = csv_path.name
    norm["raw"] = df.astype("object").where(pd.notnull(df), None).to_dict(orient="records")
    norm["raw"] = norm["raw"].apply(to_jsonb_safe)

    # 🔎 prove it before to_sql
    bad = norm["raw"].apply(lambda x: isinstance(x, (dict, list))).sum()
    print(f"[dk-ingest][debug] raw non-serialized rows={bad}")
    print(f"[dk-ingest][debug] raw types:\n{norm['raw'].map(type).value_counts().head(5)}")
    if bad:
        i = norm["raw"].index[norm["raw"].apply(lambda x: isinstance(x, (dict, list)))][0]
        print("[dk-ingest][debug] example raw still dict/list:", norm.loc[i, "raw"])

    tmp_table = "dk_salaries_tmp"

    # ✅ write temp table inside your schema
    if "raw" in norm.columns:
        norm["raw"] = norm["raw"].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
    norm.to_sql(tmp_table, engine, schema=schema, if_exists="replace", index=False)

    upsert_sql = f"""
        INSERT INTO "{schema}".dk_salaries (
        slate_date, source_file,
        dk_player_id, name_id, name,
        position, roster_position, salary, game_info, team_abbrev, avg_points_per_game, raw
        )
        SELECT
        slate_date, source_file,
        dk_player_id, name_id, name,
        position, roster_position, salary, game_info, team_abbrev, avg_points_per_game, raw::jsonb
        FROM "{schema}"."{tmp_table}"
        ON CONFLICT ON CONSTRAINT dk_salaries_uniq
        DO UPDATE SET
        dk_player_id = EXCLUDED.dk_player_id,
        name         = EXCLUDED.name,
        salary       = EXCLUDED.salary,
        game_info    = EXCLUDED.game_info,
        team_abbrev  = EXCLUDED.team_abbrev,
        avg_points_per_game = EXCLUDED.avg_points_per_game,
        raw          = EXCLUDED.raw;
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp_table}";'))

def find_candidates():
    files = []
    for g in FILE_GLOBS:
        files.extend(INBOX.glob(g))
    return sorted(set(files), key=lambda p: p.stat().st_mtime, reverse=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one ingest pass then exit")
    parser.add_argument("--poll-seconds", type=int, default=POLL_SECONDS, help="Polling interval (seconds)")
    args = parser.parse_args()

    once = args.once or RUN_ONCE
    poll_seconds = int(args.poll_seconds)

    INBOX.mkdir(parents=True, exist_ok=True)
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    ERROR.mkdir(parents=True, exist_ok=True)

    engine = get_engine()

    mode = "once" if once else f"poll={poll_seconds}s"
    print(f"[dk-ingest] schema={DK_SCHEMA} watching {INBOX} (mode={mode})")

    if once:
        n = process_available_files(engine)
        print(f"[dk-ingest] done (processed={n})")
        return

    # continuous polling
    while True:
        n = process_available_files(engine)
        if n == 0:
            time.sleep(poll_seconds)
        else:
            time.sleep(2)

if __name__ == "__main__":
    main()
