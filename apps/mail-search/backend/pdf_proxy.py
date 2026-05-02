import os

import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.responses import Response

from db import fetch_one


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def get_pdf_response(document_id: int) -> Response:
    # Require the document to have a completed enrichment record — this ensures
    # only legitimately processed documents are serveable, not arbitrary IDs.
    row = fetch_one(
        """
        SELECT d.minio_key
        FROM mail_raw.mail_documents d
        JOIN mail_raw.mail_enrichments e ON e.document_id = d.id
        WHERE d.id = %s AND d.extraction_status = 'complete'
        """,
        (document_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    bucket = os.environ["MINIO_MAIL_BUCKET"]
    key = row["minio_key"]
    filename = key.split("/")[-1]

    try:
        obj = _s3_client().get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        raise HTTPException(status_code=404, detail=f"MinIO error {code}: {key}") from exc

    return Response(
        content=data,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=300",
        },
    )
