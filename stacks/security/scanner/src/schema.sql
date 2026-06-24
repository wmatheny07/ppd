-- Security monitoring schema
-- Idempotent: safe to re-run on every container start.

CREATE SCHEMA IF NOT EXISTS security;

-- ─── Scan runs ────────────────────────────────────────────────────────────────
-- One row per scan execution (full, version_check, or manual).
CREATE TABLE IF NOT EXISTS security.scan_runs (
    id               SERIAL PRIMARY KEY,
    run_id           VARCHAR(36) NOT NULL UNIQUE,
    scan_type        VARCHAR(50) NOT NULL,  -- 'full' | 'version_check' | 'manual'
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ,
    status           VARCHAR(20) NOT NULL DEFAULT 'running',  -- 'running' | 'completed' | 'failed'
    images_scanned   INT,
    findings_count   INT,
    critical_count   INT,
    high_count       INT,
    dagster_run_id   VARCHAR(255)
);

-- ─── Vulnerability findings ───────────────────────────────────────────────────
-- One row per (image, CVE, package) tuple; upserted on each scan so history
-- tracks first_seen vs last_seen without duplicating rows.
CREATE TABLE IF NOT EXISTS security.findings (
    id               SERIAL PRIMARY KEY,
    scan_run_id      INT REFERENCES security.scan_runs(id),
    image_name       VARCHAR(500) NOT NULL,
    image_tag        VARCHAR(100),
    image_digest     VARCHAR(100),
    cve_id           VARCHAR(50),
    severity         VARCHAR(20),   -- CRITICAL | HIGH | MEDIUM | LOW | UNKNOWN
    cvss_score       NUMERIC(4, 1),
    package_name     VARCHAR(255),
    package_version  VARCHAR(100),
    fixed_version    VARCHAR(100),
    title            TEXT,
    description      TEXT,
    published_at     TIMESTAMPTZ,
    vuln_references  JSONB,
    status           VARCHAR(20) NOT NULL DEFAULT 'open',  -- 'open' | 'resolved' | 'accepted' | 'false_positive'
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (image_name, image_tag, cve_id, package_name)
);

CREATE INDEX IF NOT EXISTS idx_findings_severity    ON security.findings (severity);
CREATE INDEX IF NOT EXISTS idx_findings_status      ON security.findings (status);
CREATE INDEX IF NOT EXISTS idx_findings_image       ON security.findings (image_name);
CREATE INDEX IF NOT EXISTS idx_findings_cve         ON security.findings (cve_id);
CREATE INDEX IF NOT EXISTS idx_findings_last_seen   ON security.findings (last_seen_at DESC);

-- ─── SBOMs ────────────────────────────────────────────────────────────────────
-- Metadata record per SBOM; the full JSON artifact lives in MinIO.
CREATE TABLE IF NOT EXISTS security.sboms (
    id               SERIAL PRIMARY KEY,
    scan_run_id      INT REFERENCES security.scan_runs(id),
    image_name       VARCHAR(500) NOT NULL,
    image_tag        VARCHAR(100),
    image_digest     VARCHAR(100),
    sbom_format      VARCHAR(50),   -- 'syft-json'
    minio_path       VARCHAR(500),  -- bucket/key of the raw JSON in MinIO
    component_count  INT,
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Version drift ────────────────────────────────────────────────────────────
-- Tracks running image tag vs latest upstream release.
CREATE TABLE IF NOT EXISTS security.version_drift (
    id                SERIAL PRIMARY KEY,
    scan_run_id       INT REFERENCES security.scan_runs(id),
    service_name      VARCHAR(255) NOT NULL,
    image_name        VARCHAR(500) NOT NULL,
    running_tag       VARCHAR(100),
    latest_tag        VARCHAR(100),
    versions_behind   INT,
    release_notes_url TEXT,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (service_name, scan_run_id)
);

CREATE INDEX IF NOT EXISTS idx_version_drift_service ON security.version_drift (service_name);

-- ─── Action plans ─────────────────────────────────────────────────────────────
-- Claude-generated remediation items, one per (image, priority group).
CREATE TABLE IF NOT EXISTS security.action_plans (
    id               SERIAL PRIMARY KEY,
    scan_run_id      INT REFERENCES security.scan_runs(id),
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority         VARCHAR(20),   -- 'immediate' | 'this_week' | 'backlog'
    service_name     VARCHAR(255),
    finding_ids      INT[],
    action_type      VARCHAR(50),   -- 'patch' | 'upgrade' | 'config_change' | 'monitor'
    title            TEXT NOT NULL,
    description      TEXT NOT NULL,
    steps            JSONB,
    estimated_effort VARCHAR(50),   -- '30min' | '2hrs' | '1day' | '1week'
    status           VARCHAR(20) NOT NULL DEFAULT 'open',  -- 'open' | 'in_progress' | 'completed'
    resolved_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_action_plans_priority ON security.action_plans (priority, status);
CREATE INDEX IF NOT EXISTS idx_action_plans_scan     ON security.action_plans (scan_run_id);
