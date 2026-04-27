"""
Migrate Superset metadata from SQLite -> PostgreSQL.

Reads every table from superset.db and upserts rows into the PostgreSQL
superset database that was already schema-migrated via 'superset db upgrade'.

Run inside the superset container:
  python3 /app/superset_home/migrate_to_pg.py
"""

import sqlite3
import uuid
import psycopg2
import psycopg2.extras
import sys

SQLITE_PATH = "/app/superset_home/superset.db"
PG_DSN = "postgresql://analytics:sausage5poem6moveGLADLY@postgres:5432/superset"

# Tables to skip — these are either auto-managed by Superset on startup
# or contain ephemeral data we don't need to carry over.
SKIP_TABLES = {
    "alembic_version",       # schema version — already set correctly by db upgrade
    "cache_keys",            # ephemeral query cache
    "celery_taskmeta",       # celery task results
    "celery_tasksetmeta",    # celery chord results
    "logs",                  # audit logs — optional, can be large
    "query",                 # SQL Lab query history — optional
}

def bytes_to_uuid(val):
    """Convert a 16-byte binary value (SQLite UUID storage) to a UUID string."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)) and len(val) == 16:
        return str(uuid.UUID(bytes=bytes(val)))
    return val  # already a string or other type

def get_pg_bool_columns(pg_conn, table):
    """Return the set of column names that are boolean type in PostgreSQL."""
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND data_type = 'boolean'
    """, (table,))
    return {r[0] for r in cur.fetchall()}

def coerce_row(row, col_names, bool_cols):
    """Convert a SQLite row tuple into a dict, handling binary UUIDs and booleans."""
    d = {}
    for col, val in zip(col_names, row):
        # SQLite stores UUIDs as BLOB(16) — detect and convert
        if isinstance(val, (bytes, bytearray)) and len(val) == 16:
            val = bytes_to_uuid(val)
        # SQLite stores booleans as 0/1 integers — cast to Python bool for PG
        elif col in bool_cols and val is not None:
            val = bool(val)
        d[col] = val
    return d

def get_sqlite_tables(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [r[0] for r in cur.fetchall()]

def migrate_table(sqlite_conn, pg_conn, table):
    s_cur = sqlite_conn.cursor()
    s_cur.execute(f"SELECT * FROM \"{table}\"")
    col_names = [d[0] for d in s_cur.description]
    rows = s_cur.fetchall()

    if not rows:
        print(f"  {table}: 0 rows — skipping")
        return 0

    bool_cols = get_pg_bool_columns(pg_conn, table)
    coerced = [coerce_row(r, col_names, bool_cols) for r in rows]

    cols = ', '.join(f'"{c}"' for c in col_names)
    placeholders = ', '.join(f'%({c})s' for c in col_names)
    # ON CONFLICT DO NOTHING — safe for re-runs; won't overwrite existing data
    sql = (
        f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders}) '
        f'ON CONFLICT DO NOTHING'
    )

    p_cur = pg_conn.cursor()
    try:
        psycopg2.extras.execute_batch(p_cur, sql, coerced, page_size=500)
        pg_conn.commit()
        print(f"  {table}: {len(coerced)} rows migrated")
        return len(coerced)
    except Exception as e:
        pg_conn.rollback()
        print(f"  {table}: ERROR — {e}")
        return 0

def main():
    print(f"Connecting to SQLite: {SQLITE_PATH}")
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    print(f"Connecting to PostgreSQL: {PG_DSN.split('@')[1]}")
    pg_conn = psycopg2.connect(PG_DSN)

    # Disable FK checks during migration to avoid ordering issues
    pg_conn.cursor().execute("SET session_replication_role = replica;")
    pg_conn.commit()

    tables = get_sqlite_tables(sqlite_conn)
    print(f"\nFound {len(tables)} tables in SQLite\n")

    # Migrate in dependency order — parent tables before child tables.
    # We'll do a best-effort ordered list for the most important tables,
    # then catch everything else afterward.
    priority = [
        "ab_role", "ab_permission", "ab_view_menu", "ab_permission_view",
        "ab_user", "ab_user_role", "ab_permission_view_role",
        "dbs", "clusters", "tables", "columns", "metrics", "table_columns",
        "slices", "dashboards", "dashboard_slices", "dashboard_user",
        "report_schedule", "report_execution_log",
        "css_templates", "favstar", "tag",
    ]

    total = 0
    migrated = set()

    for table in priority:
        if table in tables and table not in SKIP_TABLES:
            total += migrate_table(sqlite_conn, pg_conn, table)
            migrated.add(table)

    # Remaining tables not in priority list
    for table in tables:
        if table not in migrated and table not in SKIP_TABLES:
            total += migrate_table(sqlite_conn, pg_conn, table)

    # Re-enable FK checks
    pg_conn.cursor().execute("SET session_replication_role = DEFAULT;")
    pg_conn.commit()

    print(f"\nDone. {total} total rows migrated.")
    sqlite_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
