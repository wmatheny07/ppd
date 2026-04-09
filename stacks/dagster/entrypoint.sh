#!/bin/bash
# Generate dbt manifests before the code server starts.
# dbt parse (dbt-core >= 1.5) resolves project YAML/SQL without a live DB connection.
# Dagster reads these manifests at import time to expose models as software-defined assets.
set -e

PROFILES_DIR="/opt/dbt/profiles"
ARTIFACTS_DIR="/opt/dagster/dbt_artifacts"

for project in health; do
    project_dir="/opt/dagster/dbt/${project}"
    manifest_dir="${ARTIFACTS_DIR}/${project}"

    if [ -f "${project_dir}/dbt_project.yml" ] && [ -d "${PROFILES_DIR}" ]; then
        echo "▶ Parsing dbt project: ${project}"
        mkdir -p "${manifest_dir}/target"
        cd "${project_dir}"
        dbt parse \
            --profiles-dir "${PROFILES_DIR}" \
            --target-path "${manifest_dir}/target" \
            --log-level warn \
            2>&1 || echo "⚠  dbt parse for '${project}' failed — dbt assets will be unavailable until resolved"
    else
        echo "⚠  Skipping dbt parse for '${project}' (project dir or profiles not found)"
    fi
done

exec "$@"
