"""
enrichment.py

Dagster asset: enriched_mail_documents

Reads raw extracted text from PostgreSQL, applies PII redaction,
sends a structured prompt to the Claude API, and writes enrichment
fields back to PostgreSQL.
"""

import json
from dagster import asset, AssetExecutionContext, AssetIn

from ..resources.postgres import PostgresResource
from ..resources.anthropic import AnthropicResource
from .pii_redaction import redact_for_api


CLASSIFICATION_PROMPT = """You are a document classifier for a personal mail filing system.
Analyze the following extracted text from a piece of mail and return a JSON object only —
no explanation, no markdown, no preamble.

JSON schema:
{
  "document_type": one of ["statement", "eob", "legal", "government", "insurance", "personal", "marketing", "utility", "unknown"],
  "sender_raw": "sender name as it appears in the document",
  "sender_normalized": "cleaned organization name (e.g. 'Chase Bank', 'Aetna', 'VA Benefits')",
  "document_date": "YYYY-MM-DD or null if not found",
  "dollar_amounts": [{"label": "description", "value": 0.00}],
  "action_required": true or false,
  "action_description": "what action is needed, or null",
  "summary": "1-2 sentence plain English summary of the document"
}

Document text:
"""


@asset(
    ins={"raw_mail_documents": AssetIn()},
    required_resource_keys={"postgres", "anthropic"},
    group_name="mail_enrichment",
    description="Sends redacted document text to Claude API for classification and enrichment.",
)
def enriched_mail_documents(
    context: AssetExecutionContext,
    raw_mail_documents: dict,
    postgres: PostgresResource,
    anthropic: AnthropicResource,
) -> dict:
    """
    Depends on raw_mail_documents. Reads the extracted text, redacts PII,
    calls Claude for structured classification, writes enrichment to Postgres.
    """
    if raw_mail_documents.get("skipped"):
        context.log.info("Upstream asset was skipped — nothing to enrich.")
        return {"skipped": True}

    document_id = raw_mail_documents["document_id"]
    minio_key = raw_mail_documents["minio_key"]

    # ── Fetch raw text ───────────────────────────────────────
    row = postgres.fetch_one(
        "SELECT raw_text, extraction_status FROM mail_documents WHERE id = %s",
        (document_id,),
    )

    if not row or row["extraction_status"] != "complete":
        context.log.warning(f"Document {document_id} not ready for enrichment — skipping.")
        return {"skipped": True}

    raw_text = row["raw_text"] or ""
    if not raw_text.strip():
        context.log.warning(f"Document {document_id} has no extracted text — skipping enrichment.")
        return {"skipped": True}

    # ── PII Redaction (always before API) ────────────────────
    clean_text, redaction_meta = redact_for_api(raw_text, max_chars=6000)
    context.log.info(
        f"PII redaction: {redaction_meta['redaction_count']} items removed "
        f"({', '.join(redaction_meta['patterns_hit']) or 'none'})"
    )

    # ── Claude API Call ──────────────────────────────────────
    client = anthropic.get_client()
    context.log.info(f"Sending document {document_id} to Claude API for classification")

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[
            {
                "role": "user",
                "content": CLASSIFICATION_PROMPT + clean_text,
            }
        ],
    )

    tokens_used = message.usage.input_tokens + message.usage.output_tokens
    raw_response = message.content[0].text

    # ── Parse Structured Response ────────────────────────────
    try:
        enrichment = json.loads(raw_response)
    except json.JSONDecodeError:
        # Strip any accidental markdown fencing
        cleaned = raw_response.strip().removeprefix("```json").removesuffix("```").strip()
        enrichment = json.loads(cleaned)

    # ── Write Enrichment to Postgres ─────────────────────────
    postgres.execute(
        """
        INSERT INTO mail_enrichments
            (document_id, document_type, sender_raw, sender_normalized,
             document_date, dollar_amounts, action_required, action_description,
             summary, model_used, tokens_used)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (document_id) DO UPDATE SET
            document_type       = EXCLUDED.document_type,
            sender_raw          = EXCLUDED.sender_raw,
            sender_normalized   = EXCLUDED.sender_normalized,
            document_date       = EXCLUDED.document_date,
            dollar_amounts      = EXCLUDED.dollar_amounts,
            action_required     = EXCLUDED.action_required,
            action_description  = EXCLUDED.action_description,
            summary             = EXCLUDED.summary,
            model_used          = EXCLUDED.model_used,
            tokens_used         = EXCLUDED.tokens_used,
            enriched_ts         = NOW()
        """,
        (
            document_id,
            enrichment.get("document_type"),
            enrichment.get("sender_raw"),
            enrichment.get("sender_normalized"),
            enrichment.get("document_date"),
            json.dumps(enrichment.get("dollar_amounts", [])),
            enrichment.get("action_required", False),
            enrichment.get("action_description"),
            enrichment.get("summary"),
            "claude-sonnet-4-20250514",
            tokens_used,
        ),
    )

    # ── Dagster Asset Metadata ───────────────────────────────
    context.add_output_metadata({
        "document_id": document_id,
        "document_type": enrichment.get("document_type"),
        "sender_normalized": enrichment.get("sender_normalized"),
        "action_required": enrichment.get("action_required"),
        "tokens_used": tokens_used,
        "pii_redactions": redaction_meta["redaction_count"],
    })

    return {
        "document_id": document_id,
        "minio_key": minio_key,
        "document_type": enrichment.get("document_type"),
    }
