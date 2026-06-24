# PPD Security Scanning

Automated vulnerability and version monitoring for the full PPD + MathenyManor stack.
Two Dagster jobs orchestrate a `security-scanner` service that runs Trivy CVE scans,
Syft SBOM generation, and a LangGraph/Claude analysis pipeline. All results land in
the `security` schema on Postgres.

---

## Architecture

```
Dagster (port 13000)
  └── security_image_scan_job   ─── POST /scan/full ──►  security-scanner (port 18100)
  └── security_version_check_job ── POST /scan/versions ► security-scanner (port 18100)
                                                               │
                                          ┌────────────────────┼───────────────────────┐
                                          │                    │                       │
                                    Docker socket          OSV / NVD           Docker Hub /
                                    (image inventory)      (CVE feeds)         GitHub Releases
                                          │                                    (version check)
                                          ▼
                                    Trivy (CVE scan)
                                    Syft  (SBOM gen)
                                          │
                                    ┌─────┴──────┐
                                    │            │
                                  MinIO       Postgres
                              (SBOM JSON)   (security.*)
                                                 │
                                         LangGraph + Claude
                                         (triage + action plans)
                                                 │
                                         security.action_plans
```

The scanner runs autonomously on its own APScheduler and also accepts on-demand
triggers from Dagster. Both paths write to the same Postgres tables.

---

## The Two Jobs

### 1. `security_image_scan_job` — Daily CVE + SBOM scan

**Schedule:** Daily at 2:00 AM ET

**What it does:**

1. Queries the Docker socket for all running containers and deduplicates by image
2. Runs `trivy image --format json` against each image — produces a flat list of CVEs
3. Runs `syft <image> -o syft-json` against each image — produces a Software Bill of Materials
4. Stores raw SBOM JSON to MinIO (`security-scans` bucket)
5. Upserts all findings to `security.findings` (deduplicates by image + CVE + package)
6. Passes findings to LangGraph: `triage_node → plan_node → store_node`
   - **triage_node**: Claude groups findings into `immediate / this_week / backlog` using
     knowledge of which services are internet-facing vs. internal-only
   - **plan_node**: Claude generates a structured action plan per (priority, image) pair
   - **store_node**: Writes plans to `security.action_plans`
7. Marks the scan run complete with summary counts

**First real run output (2026-05-02):**

| Severity | Count |
|----------|-------|
| CRITICAL | 28 |
| HIGH | 332 |
| MEDIUM | 1,238 |
| LOW | 1,660 |
| UNKNOWN | 111 |
| **Total** | **3,369** |

16 SBOMs generated across 16 unique images.

---

### 2. `security_version_check_job` — Weekly version drift check

**Schedule:** Mondays at 3:00 AM ET

**What it does:**

1. Queries Docker socket for running images
2. For each known image, checks Docker Hub or GitHub Releases for the latest version
3. Writes a `version_drift` row for any image where `running_tag ≠ latest_tag`

**Known images monitored:**

| Image | Source |
|-------|--------|
| postgres / pgvector | Docker Hub |
| redis | Docker Hub |
| minio/minio | Docker Hub |
| metabase/metabase | Docker Hub |
| qdrant/qdrant | Docker Hub |
| provectuslabs/kafka-ui | Docker Hub |
| bitnami/kafka | Docker Hub |
| nginx | Docker Hub |
| apache/airflow | GitHub Releases |
| dagster | GitHub Releases |
| apache/superset | GitHub Releases |

Custom images (ppd-dagster, ppd-security-scanner, etc.) are skipped — they have no
upstream to compare against.

**First real run output (2026-05-02, 24 images checked):**

| Service | Running | Latest | Behind |
|---------|---------|--------|--------|
| nginx | `latest` | `1.30.0` | 1 major |
| redis | `7.2-bookworm` | `8.6.2` | 1 major |

> **Note on `latest` tags:** If a service is pinned to `latest` rather than a specific
> version tag (as nginx is here), the version check will always flag it as behind. Pin
> to a specific tag in the compose file to get accurate drift tracking.

---

## Outputs

### Postgres — `security` schema

#### `security.scan_runs`
One row per scan execution. The entry point for any query.

| Column | Type | Notes |
|--------|------|-------|
| `id` | serial | Primary key |
| `run_id` | varchar | UUID, unique per execution |
| `scan_type` | varchar | `full`, `version_check` |
| `started_at` | timestamptz | |
| `completed_at` | timestamptz | NULL while running |
| `status` | varchar | `running`, `completed`, `failed` |
| `images_scanned` | int | |
| `findings_count` | int | Total CVEs found |
| `critical_count` | int | |
| `high_count` | int | |
| `dagster_run_id` | varchar | Reserved for future linkage |

#### `security.findings`
One row per `(image_name, image_tag, cve_id, package_name)` tuple.
Upserted on every scan — `first_seen_at` and `last_seen_at` track lifecycle
without creating duplicate rows.

| Column | Type | Notes |
|--------|------|-------|
| `id` | serial | |
| `scan_run_id` | int | FK → scan_runs |
| `image_name` | varchar | e.g. `pgvector/pgvector` |
| `image_tag` | varchar | e.g. `pg16` |
| `cve_id` | varchar | e.g. `CVE-2024-12345` |
| `severity` | varchar | `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`, `UNKNOWN` |
| `cvss_score` | numeric | CVSS v3 score from Trivy |
| `package_name` | varchar | Affected OS/language package |
| `package_version` | varchar | Installed version |
| `fixed_version` | varchar | Version that resolves the CVE (if available) |
| `title` | text | Short CVE title |
| `description` | text | Up to 2,000 characters |
| `published_at` | timestamptz | CVE publication date |
| `vuln_references` | jsonb | Array of reference URLs |
| `status` | varchar | `open`, `resolved`, `accepted`, `false_positive` |
| `first_seen_at` | timestamptz | Date this CVE was first detected |
| `last_seen_at` | timestamptz | Updated on every scan where still present |

#### `security.sboms`
One row per SBOM generated. The full JSON artifact is in MinIO.

| Column | Type | Notes |
|--------|------|-------|
| `id` | serial | |
| `scan_run_id` | int | FK → scan_runs |
| `image_name` | varchar | |
| `image_tag` | varchar | |
| `sbom_format` | varchar | Always `syft-json` |
| `minio_path` | varchar | `security-scans/sboms/<run_id>/<image>.json` |
| `component_count` | int | Number of packages/libraries catalogued |
| `generated_at` | timestamptz | |

#### `security.version_drift`
One row per service per version_check run. Only populated when a newer version exists.

| Column | Type | Notes |
|--------|------|-------|
| `service_name` | varchar | Container name |
| `image_name` | varchar | |
| `running_tag` | varchar | Currently deployed tag |
| `latest_tag` | varchar | Newest available tag |
| `versions_behind` | int | Major version difference |
| `release_notes_url` | text | Docker Hub tags page or GitHub release URL |
| `checked_at` | timestamptz | |

#### `security.action_plans`
Claude-generated remediation items. One plan per `(priority, image)` group per scan.

| Column | Type | Notes |
|--------|------|-------|
| `id` | serial | |
| `scan_run_id` | int | FK → scan_runs |
| `priority` | varchar | `immediate`, `this_week`, `backlog` |
| `service_name` | varchar | Image name this plan targets |
| `action_type` | varchar | `patch`, `upgrade`, `config_change`, `monitor` |
| `title` | text | Short imperative title |
| `description` | text | 2–3 sentence risk summary |
| `steps` | jsonb | Ordered array of remediation steps |
| `estimated_effort` | varchar | `30min`, `2hrs`, `1day`, `1week` |
| `status` | varchar | `open`, `in_progress`, `completed` |
| `resolved_at` | timestamptz | Set when status → completed |
| `finding_ids` | int[] | IDs of findings that triggered this plan |

---

### MinIO — `security-scans` bucket

Raw Syft SBOM JSON files, one per image per scan run.

```
security-scans/
└── sboms/
    └── <scan_run_id>/
        └── <image_name>_<tag>.json   ← full Syft JSON SBOM
```

Access via the MinIO console (port 9001) or `rclone`/`boto3` with the standard
`MINIO_WES_USER` / `MINIO_WES_PASSWORD` credentials.

---

## Trigger Manually

### From Dagster UI (port 13000)
Navigate to **Jobs → security_image_scan_job** or **security_version_check_job**
and click **Materialize** / **Launch Run**.

### From the scanner API directly (port 18100)
```bash
# Full CVE + SBOM scan
curl -X POST http://localhost:18100/scan/full

# Version drift check
curl -X POST http://localhost:18100/scan/versions

# Check status of recent runs
curl http://localhost:18100/scan/status | python3 -m json.tool

# Health check
curl http://localhost:18100/health
```

---

## Useful Queries

```sql
-- Open critical findings, newest first
SELECT image_name, image_tag, cve_id, cvss_score, package_name, fixed_version, title
FROM security.findings
WHERE severity = 'CRITICAL' AND status = 'open'
ORDER BY cvss_score DESC NULLS LAST, last_seen_at DESC;

-- All findings with a known fix available
SELECT image_name, cve_id, severity, package_name, package_version, fixed_version
FROM security.findings
WHERE status = 'open' AND fixed_version IS NOT NULL
ORDER BY severity, image_name;

-- Finding count by image (most exposed first)
SELECT image_name,
       COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical,
       COUNT(*) FILTER (WHERE severity = 'HIGH')     AS high,
       COUNT(*) FILTER (WHERE severity = 'MEDIUM')   AS medium,
       COUNT(*) FILTER (WHERE severity = 'LOW')      AS low
FROM security.findings
WHERE status = 'open'
GROUP BY image_name
ORDER BY critical DESC, high DESC;

-- Immediate-priority action plans from the latest scan
SELECT ap.priority, ap.service_name, ap.action_type, ap.title,
       ap.estimated_effort, ap.steps
FROM security.action_plans ap
JOIN security.scan_runs sr ON ap.scan_run_id = sr.id
WHERE ap.status = 'open' AND ap.priority = 'immediate'
ORDER BY sr.started_at DESC, ap.service_name;

-- Images with version drift
SELECT service_name, image_name, running_tag, latest_tag,
       versions_behind, release_notes_url, checked_at
FROM security.version_drift
ORDER BY checked_at DESC, versions_behind DESC;

-- Scan run history
SELECT scan_type, status, images_scanned, findings_count,
       critical_count, high_count,
       EXTRACT(EPOCH FROM (completed_at - started_at))::int AS duration_sec
FROM security.scan_runs
ORDER BY started_at DESC
LIMIT 20;

-- Mark a finding accepted (suppress from dashboards)
UPDATE security.findings
SET status = 'accepted'
WHERE cve_id = 'CVE-XXXX-XXXXX' AND image_name = 'some/image';
```

---

## Stack Management

```bash
# Start the security stack
/opt/util/stack.sh \
  -f /opt/ppd/stacks/security/docker-compose.security.yml \
  -e /opt/config/runtime/.env.all \
  -p security up

# View live logs
/opt/util/stack.sh \
  -f /opt/ppd/stacks/security/docker-compose.security.yml \
  -e /opt/config/runtime/.env.all \
  -p security logs

# Rebuild after code changes to scanner/ (src/ is bind-mounted so restart suffices for Python changes)
docker restart security-scanner

# Full rebuild after Dockerfile or requirements.txt changes
/opt/util/stack.sh \
  -f /opt/ppd/stacks/security/docker-compose.security.yml \
  -e /opt/config/runtime/.env.all \
  -p security rebuild
```

---

## Configuration

All values injected at runtime from `/opt/config/runtime/.env.all` via `stack.sh`.

| Variable | Default | Notes |
|----------|---------|-------|
| `ANALYTICS_DB_*` | — | Standard PPD Postgres credentials |
| `MINIO_WES_USER` / `MINIO_WES_PASSWORD` | — | MinIO credentials |
| `MINIO_SECURITY_BUCKET` | `security-scans` | Bucket for SBOM artifacts |
| `ANTHROPIC_API_KEY` | — | Required for LangGraph/Claude analysis |
| `NVD_API_KEY` | _(empty)_ | Optional — raises NVD rate limit from 5 to 50 req/30s |
| `GITHUB_TOKEN` | _(empty)_ | Optional — needed for GitHub Releases lookups; avoids 60 req/hr anonymous limit |
| `SCAN_SCHEDULE_CRON` | `0 2 * * *` | Full scan schedule (cron, America/New_York) |
| `VERSION_CHECK_SCHEDULE_CRON` | `0 3 * * 1` | Version check schedule |
| `LOG_LEVEL` | `INFO` | `DEBUG` for verbose Trivy/Syft output |

### Recommended secrets to add to `/opt/config/.env`

```
NVD_API_KEY=op://Peak Precision Data/NVD API/credential
GITHUB_TOKEN=op://Peak Precision Data/GitHub Token/credential
```

---

## Extending

### Add a new image to version monitoring
Edit [`version_check.py`](scanner/src/version_check.py) and add to the appropriate dict:

```python
# Docker Hub image
_DOCKERHUB_REPOS["my-org/my-image"] = "my-org/my-image"

# GitHub releases
_GITHUB_REPOS["my-org/my-image"] = "my-org/my-repo"
```

### Update Claude's stack context
If you add or remove an internet-facing service, update `_STACK_CONTEXT` in
[`agent.py`](scanner/src/agent.py) so triage prioritization stays accurate.

### Suppress a known false positive
```sql
UPDATE security.findings
SET status = 'false_positive'
WHERE cve_id = 'CVE-XXXX-XXXXX'
  AND image_name = 'affected/image';
```

### Mark an action plan resolved
```sql
UPDATE security.action_plans
SET status = 'completed', resolved_at = NOW()
WHERE id = <plan_id>;
```

---

## File Layout

```
/opt/ppd/stacks/security/
├── docker-compose.security.yml
└── scanner/
    ├── Dockerfile               # Python 3.11-slim + Trivy + Syft binaries
    ├── requirements.txt
    └── src/
        ├── main.py              # FastAPI app + APScheduler entry point
        ├── scanner.py           # Trivy + Syft orchestration, MinIO upload
        ├── inventory.py         # Docker socket → running image list
        ├── agent.py             # LangGraph graph (triage → plan → store)
        ├── cve_feeds.py         # OSV API + NVD API clients
        ├── version_check.py     # Docker Hub + GitHub Releases version lookup
        ├── db.py                # Postgres operations (all security.* tables)
        └── schema.sql           # Idempotent schema creation (runs on startup)

/opt/ppd/stacks/dagster/project/dagster_ppd/jobs/
└── security_scan.py             # Dagster ops + job definitions
```
