import json

from dagster import asset, AssetExecutionContext, AssetIn

from ...resources.anthropic import AnthropicResource
from ...resources.postgres import PostgresResource
from .pii_redaction import redact_pii

MODEL = "claude-haiku-4-5-20251001"

CHUNK_SIZE = 4000
CHUNK_OVERLAP = 150

_PROMPT = """\
You are a bank statement transaction extractor.
Extract ALL individual transactions visible in the text below.
Return a JSON array only — no explanation, no markdown, no preamble.

Each item in the array:
{{"date": "YYYY-MM-DD", "payee": "cleaned payee or description", "amount": 0.00, "type": "debit" or "credit"}}

Rules:
- "debit" = money leaving the account (payments, withdrawals, purchases, fees, checks)
- "credit" = money entering the account (deposits, transfers in, refunds, interest)
- Normalize dates to YYYY-MM-DD; use {year} for the year if only month/day is shown
- amount is always a positive number
- Clean payee names: remove trailing codes, excess whitespace, and card/check numbers
- Return [] if no transactions are found in this excerpt

Statement text:
"""


def _chunks(text: str) -> list[str]:
    results = []
    i = 0
    while i < len(text):
        results.append(text[i:i + CHUNK_SIZE])
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return results


def _extract_transactions(client, text: str, year: int) -> list[dict]:
    prompt = _PROMPT.format(year=year) + text
    message = client.messages.create(
        model=MODEL,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _deduplicate(transactions: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for t in transactions:
        key = (t.get("date"), (t.get("payee") or "").lower().strip(), t.get("amount"))
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique


@asset(
    ins={"enriched_mail_documents": AssetIn()},
    group_name="mail_enrichment",
    description="Extracts individual line-item transactions from bank statements using Claude.",
)
def bank_statement_transactions(
    context: AssetExecutionContext,
    enriched_mail_documents: dict,
    postgres: PostgresResource,
    anthropic: AnthropicResource,
) -> dict:
    if enriched_mail_documents.get("skipped"):
        context.log.info("Upstream enrichment skipped — nothing to extract transactions from.")
        return {"skipped": True}

    if enriched_mail_documents.get("document_type") != "statement":
        context.log.info(
            f"Document type is '{enriched_mail_documents.get('document_type')}' — "
            "skipping transaction extraction (statements only)."
        )
        return {"skipped": True}

    document_id = enriched_mail_documents["document_id"]

    row = postgres.fetch_one(
        """
        SELECT d.raw_text, e.document_date
        FROM mail_raw.mail_documents d
        JOIN mail_raw.mail_enrichments e ON e.document_id = d.id
        WHERE d.id = %s
        """,
        (document_id,),
    )

    if not row or not row.get("raw_text"):
        context.log.warning(f"No raw text for document {document_id} — skipping.")
        return {"skipped": True}

    raw_text = row["raw_text"]
    document_date = row.get("document_date")
    year = document_date.year if document_date else 2026

    redacted = redact_pii(raw_text).redacted_text
    chunks = _chunks(redacted)
    context.log.info(
        f"Document {document_id}: {len(raw_text)} chars → {len(chunks)} chunk(s) for transaction extraction"
    )

    client = anthropic.get_client()
    all_transactions: list[dict] = []
    for i, chunk in enumerate(chunks):
        txns = _extract_transactions(client, chunk, year)
        context.log.info(f"  Chunk {i + 1}/{len(chunks)}: {len(txns)} transactions found")
        all_transactions.extend(txns)

    unique_transactions = _deduplicate(all_transactions)
    context.log.info(
        f"Document {document_id}: {len(all_transactions)} raw → {len(unique_transactions)} unique transactions"
    )

    # Delete previous extraction for this document (handles re-runs)
    postgres.execute(
        "DELETE FROM mail_raw.bank_transactions WHERE document_id = %s",
        (document_id,),
    )

    if unique_transactions:
        postgres.execute_many(
            """
            INSERT INTO mail_raw.bank_transactions
                (document_id, transaction_date, payee, amount, transaction_type, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    document_id,
                    t.get("date"),
                    t.get("payee"),
                    t.get("amount"),
                    t.get("type"),
                    t.get("description"),
                )
                for t in unique_transactions
            ],
        )

    postgres.log_pipeline_event(
        stage="bank_transactions",
        status="success",
        document_id=document_id,
        message=f"{len(unique_transactions)} transactions extracted across {len(chunks)} chunk(s)",
        dagster_run_id=context.run_id,
    )

    context.add_output_metadata({
        "document_id": document_id,
        "chunks_processed": len(chunks),
        "transactions_extracted": len(unique_transactions),
    })

    return {"document_id": document_id, "transaction_count": len(unique_transactions)}
