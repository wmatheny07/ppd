"""
search.py

Two-mode document search:
  1. NLP: sends the user query to Claude, which generates a PostgreSQL SELECT,
     then executes it and returns the results.
  2. Keyword fallback: used when Claude fails or returns zero rows — runs a
     plainto_tsquery full-text search plus ILIKE on sender/summary.
"""

import decimal
import os
import re
from datetime import date, datetime

import anthropic

from db import fetch_all

# ── Schema context given to Claude ───────────────────────────────────────────

_SCHEMA = """
Tables (schema: mail_raw):

mail_raw.mail_documents  (alias: d)
  id               SERIAL PRIMARY KEY
  minio_key        TEXT    -- MinIO storage path, e.g. inbox/2026-04/scan.pdf
  file_size_bytes  BIGINT
  page_count       INT
  raw_text         TEXT    -- full extracted text of the document
  extraction_status TEXT   -- 'pending' | 'complete' | 'failed'
  ingest_ts        TIMESTAMPTZ  -- when the file was ingested
  extracted_ts     TIMESTAMPTZ
  search_vector    tsvector     -- GIN-indexed, used for full-text search

mail_raw.mail_enrichments  (alias: e)
  id                 SERIAL PRIMARY KEY
  document_id        INT   -- FK → mail_raw.mail_documents.id
  document_type      TEXT  -- 'statement' | 'eob' | 'legal' | 'government'
                           --  | 'insurance' | 'personal' | 'marketing'
                           --  | 'utility' | 'unknown'
  sender_raw         TEXT  -- sender exactly as read from the document
  sender_normalized  TEXT  -- cleaned organisation name, e.g. 'Chase Bank'
  document_date      DATE  -- date of the document itself (not ingestion date)
  dollar_amounts     JSONB -- array of {label: str, value: float}
  action_required      BOOLEAN
  action_description   TEXT     -- what action is needed (null if none)
  action_completed     BOOLEAN  -- true once the user has completed the action
  action_completed_ts  TIMESTAMPTZ
  action_notes         TEXT     -- user's notes on how/when they completed the action
  summary              TEXT     -- 1-2 sentence plain-English summary
  mail_owner           TEXT     -- 'household' | 'father' | 'unknown'
  enriched_ts          TIMESTAMPTZ

mail_raw.bank_transactions  (alias: bt)
  id               SERIAL PRIMARY KEY
  document_id      INT    -- FK → mail_raw.mail_documents.id
  transaction_date DATE
  payee            TEXT   -- payee or merchant name — often a truncated OCR artifact from the bank
                          --   e.g. 'THE WOODLANDS HE' instead of 'Woodlands Healthcare'
                          --   e.g. 'VA BENEF VACP TREAS' for VA benefit deposits
                          -- Always match payees loosely with multiple ILIKE alternatives:
                          --   (bt.payee ILIKE '%woodlands%' OR bt.payee ILIKE '%rehab%')
  amount           NUMERIC(12,2)  -- always positive
  transaction_type TEXT   -- 'debit' (money out) | 'credit' (money in)
  description      TEXT
  -- Join path: bt.document_id = d.id (and optionally JOIN e ON e.document_id = d.id for sender/date filters)
"""

_SYSTEM = f"""You are a PostgreSQL query generator for a personal mail intelligence database.

{_SCHEMA}

Your job: convert a natural-language search request into a single valid PostgreSQL SELECT.

Rules you must follow:
- Use mail_raw. schema prefix on all tables.
- Only generate SELECT statements — never INSERT, UPDATE, DELETE, DROP, CREATE, TRUNCATE, ALTER, GRANT, REVOKE.
- Add a LIMIT clause; never return more than 50 rows.
- Do not wrap the output in markdown fences or add any explanation. Return raw SQL only.

Choose the right query shape based on the question:

DOCUMENT queries (finding or listing documents):
  - Alias mail_raw.mail_documents as d, mail_raw.mail_enrichments as e.
  - Always JOIN mail_raw.mail_enrichments e ON e.document_id = d.id
  - SELECT exactly: d.id, d.minio_key, d.page_count, d.ingest_ts,
      e.document_type, e.sender_normalized, e.document_date,
      e.dollar_amounts, e.action_required, e.action_description,
      e.action_completed, e.action_completed_ts, e.action_notes, e.summary

TRANSACTION queries (totals, spending by payee, payment history from bank statements):
  Use ONLY when the question asks about payments/spending/deposits visible as line items
  in a bank or credit card statement (e.g. "how much paid to X", "total spent at Y").
  Do NOT use for questions about benefit amounts, document contents, or letters.
  - Alias mail_raw.bank_transactions as bt.
  - JOIN mail_raw.mail_documents d ON d.id = bt.document_id
  - JOIN mail_raw.mail_enrichments e ON e.document_id = d.id (for sender/owner filters)
  - SELECT the aggregated or filtered columns needed to answer the question
    (e.g. SUM(bt.amount), bt.payee, bt.transaction_date, e.sender_normalized)
  - Filter by transaction_type = 'debit' for payments/spending, 'credit' for deposits
  - Use e.mail_owner = 'father' to scope to the father's mail when asked about him
- Add a LIMIT clause; never return more than 50 rows.
- Do not wrap the output in markdown fences or add any explanation.
- Return the raw SQL only.
"""

# ── SQL safety validation ─────────────────────────────────────────────────────

_FORBIDDEN = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|TRUNCATE|ALTER|GRANT|REVOKE)\b',
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> str:
    sql = sql.strip().strip(";")
    # Strip accidental markdown fencing
    if sql.startswith("```"):
        sql = re.sub(r"^```[a-z]*\n?", "", sql).rstrip("`").strip()
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Generated query is not a SELECT statement")
    if _FORBIDDEN.search(sql):
        raise ValueError("Forbidden keyword detected in generated SQL")
    return sql


# ── Claude SQL generation ─────────────────────────────────────────────────────

def _claude_to_sql(query: str) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=_SYSTEM,
        messages=[{"role": "user", "content": query}],
    )
    return msg.content[0].text.strip()


# ── Keyword fallback ──────────────────────────────────────────────────────────

_KEYWORD_SQL = """
SELECT d.id, d.minio_key, d.page_count, d.ingest_ts,
       e.document_type, e.sender_normalized, e.document_date,
       e.dollar_amounts, e.action_required, e.action_description,
       e.action_completed, e.action_completed_ts, e.action_notes, e.summary
FROM mail_raw.mail_documents d
JOIN mail_raw.mail_enrichments e ON e.document_id = d.id
WHERE
    d.search_vector @@ plainto_tsquery('english', %s)
    OR e.sender_normalized ILIKE %s
    OR e.summary ILIKE %s
ORDER BY ts_rank(d.search_vector, plainto_tsquery('english', %s)) DESC
LIMIT 50
"""


def _keyword_search(query: str) -> list[dict]:
    like = f"%{query}%"
    return fetch_all(_KEYWORD_SQL, (query, like, like, query))


# ── JSON serialization ────────────────────────────────────────────────────────

def _serialize(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        record = {}
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                record[k] = v.isoformat()
            elif isinstance(v, decimal.Decimal):
                record[k] = float(v)
            else:
                record[k] = v
        out.append(record)
    return out


# ── Answer synthesis ──────────────────────────────────────────────────────────

_ANSWER_SYSTEM = """You are a helpful assistant answering questions about a personal mail archive.
You will be given the user's question and structured data extracted from relevant documents.
Answer the question directly and specifically using the document data provided.
Be concise (2-5 sentences). Cite specific values, dates, and senders where relevant.
If the documents don't contain enough information to fully answer, say so clearly."""


def _is_aggregation(docs: list[dict]) -> bool:
    """True when the result set looks like an aggregation rather than document rows."""
    if not docs:
        return False
    return "document_type" not in docs[0] and "sender_normalized" not in docs[0]


def _format_aggregation_for_synthesis(rows: list[dict]) -> str:
    import json as _json
    serialized = _serialize(rows)
    return _json.dumps(serialized, indent=2)


def _format_docs_for_synthesis(docs: list[dict]) -> str:
    lines = []
    for i, d in enumerate(docs, 1):
        amounts = ""
        if d.get("dollar_amounts"):
            amounts = ", ".join(
                f"${a.get('value', 0):.2f} ({a.get('label', '')})"
                for a in d["dollar_amounts"]
                if a.get("value") is not None
            )
        lines.append(
            f"Document {i}: {d.get('document_type', 'unknown')} from {d.get('sender_normalized', 'unknown')}"
            f" dated {d.get('document_date') or 'unknown date'}\n"
            f"  Summary: {d.get('summary') or 'none'}\n"
            + (f"  Amounts: {amounts}\n" if amounts else "")
            + (f"  Action: {d.get('action_description')}\n" if d.get('action_description') else "")
        )
    return "\n".join(lines)


def _synthesize_answer(query: str, docs: list[dict]) -> str | None:
    if not docs:
        return None
    try:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        if _is_aggregation(docs):
            data_block = f"Query results (aggregated data):\n{_format_aggregation_for_synthesis(docs)}"
        else:
            data_block = f"Relevant documents from the mail archive:\n{_format_docs_for_synthesis(docs[:10])}"
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system=_ANSWER_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"Question: {query}\n\n{data_block}",
            }],
        )
        return msg.content[0].text.strip()
    except Exception:
        return None


# ── Public interface ──────────────────────────────────────────────────────────

def search(query: str) -> tuple[list[dict], str, str | None]:
    """
    Returns (results, mode, answer).
    mode is 'nlp' when Claude-generated SQL found results,
    'keyword' when the full-text fallback was used.
    answer is a synthesized plain-English response, or None if synthesis failed.
    """
    # Try NLP path
    try:
        raw_sql = _claude_to_sql(query)
        sql = _validate_sql(raw_sql)
        rows = fetch_all(sql)
        if rows:
            results = _serialize(rows)
            return results, "nlp", _synthesize_answer(query, results)
    except Exception:
        pass

    # Keyword fallback
    rows = _keyword_search(query)
    results = _serialize(rows)
    return results, "keyword", _synthesize_answer(query, results)
