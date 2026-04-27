-- ============================================================
-- Mail Intelligence Schema
-- Run once against your PostgreSQL instance before first run.
-- ============================================================

-- Enable pgvector if you plan to use semantic search later
 CREATE EXTENSION IF NOT EXISTS vector;

-- Enable pg_trgm for fuzzy text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create mail_raw schema
create schema if not exists mail_raw;

-- ── RAW EXTRACTION TABLE ────────────────────────────────────
CREATE TABLE IF NOT EXISTS mail_raw.mail_documents (
    id                  SERIAL PRIMARY KEY,
    minio_key           TEXT NOT NULL UNIQUE,       -- e.g. inbox/2026-04/chase_statement.pdf
    file_hash           TEXT NOT NULL,              -- SHA-256, used for dedup
    file_size_bytes     BIGINT,
    page_count          INT,
    raw_text            TEXT,                       -- full extracted text
    extraction_method   TEXT,                       -- 'pdfplumber' | 'tesseract' | 'doctr'
    extraction_status   TEXT NOT NULL DEFAULT 'pending',  -- pending | complete | failed
    ingest_ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extracted_ts        TIMESTAMPTZ,

    -- Full-text search index column (populated by trigger below)
    search_vector       tsvector,

    -- Optional: semantic embedding vector (pgvector)
    -- embedding          vector(1536),

    CONSTRAINT valid_status CHECK (extraction_status IN ('pending', 'complete', 'failed'))
);

-- Full-text search index
CREATE INDEX IF NOT EXISTS mail_documents_search_idx
    ON mail_raw.mail_documents USING GIN (search_vector);

-- Trigram index for fuzzy sender/keyword matching
CREATE INDEX IF NOT EXISTS mail_documents_text_trgm_idx
    ON mail_raw.mail_documents USING GIN (raw_text gin_trgm_ops);

-- Auto-update tsvector on insert/update
CREATE OR REPLACE FUNCTION mail_raw.update_mail_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', COALESCE(NEW.raw_text, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS mail_search_vector_update ON mail_raw.mail_documents;
CREATE TRIGGER mail_search_vector_update
    BEFORE INSERT OR UPDATE OF raw_text ON mail_raw.mail_documents
    FOR EACH ROW EXECUTE FUNCTION mail_raw.update_mail_search_vector();


-- ── AI ENRICHMENT TABLE ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS mail_raw.mail_enrichments (
    id                  SERIAL PRIMARY KEY,
    document_id         INT NOT NULL REFERENCES mail_raw.mail_documents(id) ON DELETE CASCADE,
    
    -- Claude-classified fields
    document_type       TEXT,       -- 'statement' | 'eob' | 'legal' | 'personal' | 'marketing' | 'government'
    sender_raw          TEXT,       -- as Claude read it
    sender_normalized   TEXT,       -- cleaned/standardized
    document_date       DATE,
    dollar_amounts      JSONB,      -- e.g. [{"label": "amount due", "value": 240.00}]
    action_required     BOOLEAN,
    action_description  TEXT,
    summary             TEXT,
    
    -- Audit
    model_used          TEXT,
    tokens_used         INT,
    enriched_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (document_id)            -- one enrichment record per document
);

CREATE INDEX IF NOT EXISTS mail_enrichments_type_idx ON mail_raw.mail_enrichments (document_type);
CREATE INDEX IF NOT EXISTS mail_enrichments_date_idx ON mail_raw.mail_enrichments (document_date);
CREATE INDEX IF NOT EXISTS mail_enrichments_action_idx ON mail_raw.mail_enrichments (action_required) WHERE action_required = TRUE;


-- ── PROCESSING LOG ──────────────────────────────────────────
-- Lightweight audit trail — Dagster also keeps run history,
-- but this gives you queryable pipeline observability in SQL.
CREATE TABLE IF NOT EXISTS mail_raw.mail_processing_log (
    id              SERIAL PRIMARY KEY,
    document_id     INT REFERENCES mail_raw.mail_documents(id),
    stage           TEXT NOT NULL,      -- 'extraction' | 'enrichment' | 'dbt'
    status          TEXT NOT NULL,      -- 'success' | 'error'
    message         TEXT,
    dagster_run_id  TEXT,
    logged_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
