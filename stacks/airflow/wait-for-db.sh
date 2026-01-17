#!/usr/bin/env bash
set -euo pipefail

db_host="${DB_HOST:-postgres}"
db_port="${DB_PORT:-5432}"
db_user="${AIRFLOW_DB_USER:-${DB_USER:-airflow}}"
db_name="${AIRFLOW_DB_NAME:-${DB_NAME:-airflow}}"
db_password="${AIRFLOW_DB_PASSWORD:-${DB_PASSWORD:-}}"
timeout_seconds="${AIRFLOW_DB_WAIT_TIMEOUT:-60}"
interval_seconds="${AIRFLOW_DB_WAIT_INTERVAL:-2}"

start_ts="$(date +%s)"
echo "Waiting for Postgres at ${db_host}:${db_port} (db=${db_name}, user=${db_user})..."

while true; do
  if PGPASSWORD="${db_password}" pg_isready -h "${db_host}" -p "${db_port}" -U "${db_user}" -d "${db_name}" >/dev/null 2>&1; then
    break
  fi

  now_ts="$(date +%s)"
  if (( now_ts - start_ts >= timeout_seconds )); then
    echo "Timed out waiting for Postgres after ${timeout_seconds}s." >&2
    exit 1
  fi

  sleep "${interval_seconds}"
done

if [ -x /entrypoint ]; then
  exec /entrypoint "$@"
fi

exec airflow "$@"
