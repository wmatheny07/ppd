import json
import os

from dagster import asset, AssetExecutionContext, AssetIn

from ...resources.anthropic import AnthropicResource
from ...resources.postgres import PostgresResource
from .pii_redaction import redact_for_api


def _build_classification_prompt() -> str:
    household = os.environ.get("MAIL_HOUSEHOLD_NAMES", "")
    father = os.environ.get("MAIL_FATHER_NAMES", "")
    return f"""You are a document classifier for a personal mail filing system.
Analyze the following extracted text from a piece of mail and return a JSON object only —
no explanation, no markdown, no preamble.

Household members (mail_owner = "household"): {household}
Father's names (mail_owner = "father"): {father}

JSON schema:
{{
  "document_type": one of ["statement", "eob", "legal", "government", "insurance", "personal", "marketing", "utility", "unknown"],
  "sender_raw": "sender name as it appears in the document",
  "sender_normalized": "cleaned organization name (e.g. 'Chase Bank', 'Aetna', 'VA Benefits')",
  "document_date": "YYYY-MM-DD or null if not found",
  "dollar_amounts": [{{"label": "description", "value": 0.00}}],
  "action_required": true or false,
  "action_description": "what action is needed, or null",
  "summary": "1-2 sentence plain English summary of the document",
  "addressee_name": "full name of the primary recipient as it appears in the document, or null if unclear",
  "mail_owner": one of ["household", "father", "unknown"] — match addressee_name against the name lists above; use "unknown" if no addressee is identifiable
}}

Document text:
"""

MODEL = "claude-sonnet-4-6"

CLASSIFICATION_PROMPT = _build_classification_prompt()


@asset(
    ins={"raw_mail_documents": AssetIn()},
    group_name="mail_enrichment",
    description="Sends redacted document text to Claude API for classification and enrichment.",
)
def enriched_mail_documents(
    context: AssetExecutionContext,
    raw_mail_documents: dict,
    postgres: PostgresResource,
    anthropic: AnthropicResource,
) -> dict:
    if raw_mail_documents.get("skipped"):
        context.log.info("Upstream asset was skipped — nothing to enrich.")
        return {"skipped": True}

    document_id = raw_mail_documents["document_id"]
    minio_key = raw_mail_documents["minio_key"]

    row = postgres.fetch_one(
        "SELECT raw_text, extraction_status FROM mail_raw.mail_documents WHERE id = %s",
        (document_id,),
    )

    if not row or row["extraction_status"] != "complete":
        context.log.warning(f"Document {document_id} not ready for enrichment — skipping.")
        postgres.log_pipeline_event(
            stage="enrichment",
            status="error",
            document_id=document_id,
            message="Skipped — document not in complete extraction status",
            dagster_run_id=context.run_id,
        )
        return {"skipped": True}

    raw_text = row["raw_text"] or ""
    if not raw_text.strip():
        context.log.warning(f"Document {document_id} has no extracted text — skipping enrichment.")
        postgres.log_pipeline_event(
            stage="enrichment",
            status="error",
            document_id=document_id,
            message="Skipped — no extracted text available",
            dagster_run_id=context.run_id,
        )
        return {"skipped": True}

    clean_text, redaction_meta = redact_for_api(raw_text, max_chars=6000)
    context.log.info(
        f"PII redaction: {redaction_meta['redaction_count']} items removed "
        f"({', '.join(redaction_meta['patterns_hit']) or 'none'})"
    )

    client = anthropic.get_client()
    context.log.info(f"Sending document {document_id} to Claude API for classification")

    # CLASSIFICATION_PROMPT is constant across all runs — cache it to avoid
    # re-tokenizing on every document processed in the same session.
    message = client.messages.create(
        model=MODEL,
        max_tokens=1000,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": CLASSIFICATION_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {
                        "type": "text",
                        "text": clean_text,
                    },
                ],
            }
        ],
    )

    usage = message.usage
    tokens_used = usage.input_tokens + usage.output_tokens
    raw_response = message.content[0].text

    try:
        enrichment = json.loads(raw_response)
    except json.JSONDecodeError:
        cleaned = raw_response.strip().removeprefix("```json").removesuffix("```").strip()
        try:
            enrichment = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            postgres.log_pipeline_event(
                stage="enrichment",
                status="error",
                document_id=document_id,
                message=f"Claude response JSON parse failed: {exc}",
                dagster_run_id=context.run_id,
            )
            raise

    postgres.execute(
        """
        INSERT INTO mail_raw.mail_enrichments
            (document_id, document_type, sender_raw, sender_normalized,
             document_date, dollar_amounts, action_required, action_description,
             summary, addressee_name, mail_owner, model_used, tokens_used)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (document_id) DO UPDATE SET
            document_type       = EXCLUDED.document_type,
            sender_raw          = EXCLUDED.sender_raw,
            sender_normalized   = EXCLUDED.sender_normalized,
            document_date       = EXCLUDED.document_date,
            dollar_amounts      = EXCLUDED.dollar_amounts,
            action_required     = EXCLUDED.action_required,
            action_description  = EXCLUDED.action_description,
            summary             = EXCLUDED.summary,
            addressee_name      = EXCLUDED.addressee_name,
            mail_owner          = EXCLUDED.mail_owner,
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
            enrichment.get("addressee_name"),
            enrichment.get("mail_owner", "unknown"),
            MODEL,
            tokens_used,
        ),
    )

    postgres.log_pipeline_event(
        stage="enrichment",
        status="success",
        document_id=document_id,
        message=(
            f"{enrichment.get('document_type')} from {enrichment.get('sender_normalized')} "
            f"— owner: {enrichment.get('mail_owner', 'unknown')} — {tokens_used} tokens"
        ),
        dagster_run_id=context.run_id,
    )

    context.add_output_metadata({
        "document_id": document_id,
        "document_type": enrichment.get("document_type"),
        "sender_normalized": enrichment.get("sender_normalized"),
        "addressee_name": enrichment.get("addressee_name"),
        "mail_owner": enrichment.get("mail_owner", "unknown"),
        "action_required": enrichment.get("action_required"),
        "tokens_used": tokens_used,
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", 0),
        "pii_redactions": redaction_meta["redaction_count"],
    })

    return {
        "document_id": document_id,
        "minio_key": minio_key,
        "document_type": enrichment.get("document_type"),
    }
