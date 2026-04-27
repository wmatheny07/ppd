import hashlib
import io

import pdfplumber
import pytesseract
from PIL import Image, ImageFilter, ImageOps
from dagster import asset, AssetExecutionContext, Config

from ...resources.minio import MinIOResource
from ...resources.postgres import PostgresResource


class ExtractionConfig(Config):
    minio_key: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _extract_with_pdfplumber(data: bytes) -> tuple[str, int]:
    text_parts = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n\n".join(text_parts), page_count


def _preprocess_for_ocr(img: Image.Image) -> Image.Image:
    img = img.convert("L")
    img = ImageOps.autocontrast(img, cutoff=2)
    img = img.filter(ImageFilter.SHARPEN)
    return img


def _tesseract_config() -> str:
    # OEM 3 = LSTM + legacy engine; PSM 3 = fully automatic page segmentation
    return "--oem 3 --psm 3"


def _ocr_image(img: Image.Image) -> tuple[str, float]:
    data_df = pytesseract.image_to_data(
        img, lang="eng", config=_tesseract_config(),
        output_type=pytesseract.Output.DATAFRAME,
    )
    text = pytesseract.image_to_string(img, lang="eng", config=_tesseract_config())
    conf_values = data_df.loc[data_df["conf"] > 0, "conf"]
    confidence = float(conf_values.mean()) if not conf_values.empty else 0.0
    return text, confidence


_MIN_PAGE_IMAGE_PX = 500  # embedded images smaller than this in either dimension are decorative


def _extract_with_tesseract(data: bytes) -> tuple[str, int, float]:
    import fitz  # PyMuPDF — imported lazily, only needed for scanned docs

    text_parts = []
    confidences = []
    doc = fitz.open(stream=data, filetype="pdf")
    page_count = len(doc)

    for page in doc:
        embedded_images = page.get_images()
        has_text_layer = len(page.get_text("blocks")) > 0

        img = None
        if embedded_images and not has_text_layer:
            # Only use the embedded image directly if it's large enough to be a
            # page scan. Small images (< 500px) are logos or decorative elements.
            xref = embedded_images[0][0]
            raw = doc.extract_image(xref)
            candidate = Image.open(io.BytesIO(raw["image"]))
            if min(candidate.size) >= _MIN_PAGE_IMAGE_PX:
                img = candidate

        if img is None:
            # Render the full page — handles vector PDFs, mixed content, and any
            # case where embedded images are too small to be the page content.
            mat = fitz.Matrix(400 / 72, 400 / 72)
            pix = page.get_pixmap(matrix=mat)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        img = _preprocess_for_ocr(img)
        page_text, confidence = _ocr_image(img)
        text_parts.append(page_text)
        if confidence > 0:
            confidences.append(confidence)

    doc.close()
    avg_confidence = float(sum(confidences) / len(confidences)) if confidences else 0.0
    return "\n\n".join(text_parts), page_count, avg_confidence


def _choose_extraction_method(data: bytes) -> tuple[str, int, str]:
    text, page_count = _extract_with_pdfplumber(data)
    method = "pdfplumber"

    # Heuristic: fewer than 50 chars/page on average suggests a scanned document
    if len(text.strip()) < (page_count * 50):
        text, page_count, confidence = _extract_with_tesseract(data)
        method = "tesseract"
        # Flag low-confidence extractions — likely a poor scan quality
        if confidence < 60.0:
            method = "tesseract_low_confidence"

    return text, page_count, method


@asset(
    group_name="mail_extraction",
    description="Downloads a scan from MinIO and extracts raw text into PostgreSQL.",
)
def raw_mail_documents(
    context: AssetExecutionContext,
    config: ExtractionConfig,
    minio: MinIOResource,
    postgres: PostgresResource,
) -> dict:
    key = config.minio_key
    context.log.info(f"Downloading: {key}")

    file_bytes = minio.download_bytes(key)
    file_hash = _sha256(file_bytes)

    existing = postgres.fetch_one(
        "SELECT id FROM mail_raw.mail_documents WHERE file_hash = %s",
        (file_hash,),
    )
    if existing:
        context.log.info(f"Duplicate detected (hash match) — skipping: {key}")
        postgres.log_pipeline_event(
            stage="extraction",
            status="success",
            document_id=existing["id"],
            message=f"Duplicate skipped — hash match for {key}",
            dagster_run_id=context.run_id,
        )
        return {"document_id": existing["id"], "skipped": True}

    context.log.info(f"Extracting text from {key}")
    extraction_error: str | None = None
    try:
        raw_text, page_count, method = _choose_extraction_method(file_bytes)
        status = "complete"
        if method == "tesseract_low_confidence":
            context.log.warning(f"Low OCR confidence for {key} — scan quality may be poor")
    except Exception as e:
        context.log.error(f"Extraction failed for {key}: {e}")
        raw_text, page_count, method, status = "", 0, "failed", "failed"
        extraction_error = str(e)

    row = postgres.fetch_one(
        """
        INSERT INTO mail_raw.mail_documents
            (minio_key, file_hash, file_size_bytes, page_count,
             raw_text, extraction_method, extraction_status, extracted_ts)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (minio_key) DO UPDATE SET
            raw_text           = EXCLUDED.raw_text,
            extraction_method  = EXCLUDED.extraction_method,
            extraction_status  = EXCLUDED.extraction_status,
            extracted_ts       = NOW()
        RETURNING id
        """,
        (key, file_hash, len(file_bytes), page_count, raw_text, method, status),
    )

    document_id = row["id"]

    postgres.log_pipeline_event(
        stage="extraction",
        status="success" if status == "complete" else "error",
        document_id=document_id,
        message=extraction_error or f"{method} — {len(raw_text)} chars, {page_count} pages",
        dagster_run_id=context.run_id,
    )

    context.add_output_metadata({
        "document_id": document_id,
        "minio_key": key,
        "page_count": page_count,
        "extraction_method": method,
        "char_count": len(raw_text),
        "status": status,
        "low_confidence_ocr": method == "tesseract_low_confidence",
    })

    return {"document_id": document_id, "minio_key": key, "skipped": False}
