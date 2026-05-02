-- Migration: add action completion tracking to mail_enrichments
-- Run once: psql -U analytics -d analytics -f migration_action_completed.sql

ALTER TABLE mail_raw.mail_enrichments
    ADD COLUMN IF NOT EXISTS action_completed      BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS action_completed_ts   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS action_notes          TEXT;

CREATE INDEX IF NOT EXISTS mail_enrichments_action_completed_idx
    ON mail_raw.mail_enrichments (action_completed)
    WHERE action_completed = FALSE AND action_required = TRUE;
