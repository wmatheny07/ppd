-- ── Bank Statement Transactions ─────────────────────────────────────────────
-- Stores individual line-item transactions extracted from bank statements
-- by the bank_statement_transactions Dagster asset. Populated after enrichment
-- confirms document_type = 'statement'.

CREATE TABLE IF NOT EXISTS mail_raw.bank_transactions (
    id               SERIAL PRIMARY KEY,
    document_id      INT NOT NULL REFERENCES mail_raw.mail_documents(id) ON DELETE CASCADE,
    transaction_date DATE,
    payee            TEXT,
    amount           NUMERIC(12, 2),
    transaction_type TEXT CHECK (transaction_type IN ('debit', 'credit')),
    description      TEXT,
    extracted_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS bank_transactions_document_idx ON mail_raw.bank_transactions (document_id);
CREATE INDEX IF NOT EXISTS bank_transactions_payee_idx    ON mail_raw.bank_transactions USING GIN (payee gin_trgm_ops);
CREATE INDEX IF NOT EXISTS bank_transactions_date_idx     ON mail_raw.bank_transactions (transaction_date);
