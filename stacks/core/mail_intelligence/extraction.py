"""
extraction.py

Dagster asset: raw_mail_documents

Downloads a scan from MinIO, extracts text via pdfplumber (digital PDFs)
or Tesseract (scanned/image-based), and writes results to PostgreSQL.
"""

import hashlib
import io
import tempfile
from pathlib import Path

import pdfplumber
import pytesseract
from PIL import Image
from dagster import asset, AssetExecutionContext, Config

from ..resources.minio import MinIOResource
from ..resources.postgres import PostgresResource


class ExtractionConfig(Config):
    minio_key: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _extract_with_pdfplumber(data: bytes) -> tuple[str, int]:
    """Extract text from a digital (non-scanned) PDF."""
    text_parts = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n\n".join(text_parts), page_count


def _extract_with_tesseract(data: bytes) -> tuple[str, int]:
    """
    OCR a scanned PDF or image file via Tesseract.
    Converts each page to an image first using PyMuPDF (fitz).
    """
    import fitz  # PyMuPDF

    text_parts = []
    doc = fitz.open(stream=data, filetype="pdf")
    page_count = len(doc)

    for page in doc:
        # Render page to image at 300 DPI for good OCR accuracy
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        page_text = pytesseract.image_to_string(img, lang="eng")
        text_parts.append(page_text)

    doc.close()
    return "\n\n".join(text_parts), page_count


def _choose_extraction_method(data: bytes) -> tuple[str, int, str]:
    """
    Try pdfplumber first (faster, more accurate for digital PDFs).
    Fall back to Tesseract if extracted text is suspiciously short
    (likely a scanned document).
    """
    text, page_count = _extract_with_pdfplumber(data)
    method = "pdfplumber"

    # Heuristic: if less than 50 chars per page on average, likely scanned
    if len(text.strip()) < (page_count * 50):
        text, page_count = _extract_with_tesseract(data)
        method = "tesseract"

    return text, page_count, method


@asset(
    required_resource_keys={"minio", "postgres"},
    group_name="mail_extraction",
    description="Downloads a scan from MinIO and extracts raw text into PostgreSQL.",
)
def raw_mail_documents(
    context: AssetExecutionContext,
    config: ExtractionConfig,
    minio: MinIOResource,
    postgres: PostgresResource,
) -> dict:
    """
    Materializes one row in mail_documents for the given MinIO key.
    Returns metadata dict consumed by the downstream enrichment asset.
    """
    key = config.minio_key
    context.log.info(f"Downloading: {key}")

    # ── Download ─────────────────────────────────────────────
    file_bytes = minio.download_bytes(key)
    file_hash = _sha256(file_bytes)

    # ── Dedup check ──────────────────────────────────────────
    existing = postgres.fetch_one(
        "SELECT id FROM mail_documents WHERE file_hash = %s",
        (file_hash,),
    )
    if existing:
        context.log.info(f"Duplicate detected (hash match) — skipping: {key}")
        return {"document_id": existing["id"], "skipped": True}

    # ── Extract ──────────────────────────────────────────────
    context.log.info(f"Extracting text from {key}")
    try:
        raw_text, page_count, method = _choose_extraction_method(file_bytes)
        status = "complete"
    except Exception as e:
        context.log.error(f"Extraction failed for {key}: {e}")
        raw_text, page_count, method, status = "", 0, "failed", "failed"

    # ── Write to Postgres ────────────────────────────────────
    row = postgres.fetch_one(
        """
        INSERT INTO mail_documents
            (minio_key, file_hash, file_size_bytes, page_count,
             raw_text, extraction_method, extraction_status, extracted_ts)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (minio_key) DO UPDATE SET
            raw_text = EXCLUDED.raw_text,
            extraction_method = EXCLUDED.extraction_method,
            extraction_status = EXCLUDED.extraction_status,
            extracted_ts = NOW()
        RETURNING id
        """,
        (key, file_hash, len(file_bytes), page_count, raw_text, method, status),
    )

    document_id = row["id"]

    # ── Dagster asset metadata (visible in UI) ───────────────
    context.add_output_metadata({
        "document_id": document_id,
        "minio_key": key,
        "page_count": page_count,
        "extraction_method": method,
        "char_count": len(raw_text),
        "status": status,
    })

    return {"document_id": document_id, "minio_key": key, "skipped": False}
