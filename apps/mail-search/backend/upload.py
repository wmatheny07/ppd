import os
import uuid
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException, UploadFile


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


async def upload_scan(file: UploadFile) -> str:
    """Validate, upload to MinIO inbox/, return the minio_key."""
    data = await file.read()

    if not data.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="File must be a PDF")

    if len(data) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File exceeds 50 MB limit")

    month = datetime.now().strftime("%Y-%m")
    stem = os.path.splitext(file.filename or "scan")[0]
    key = f"inbox/{month}/{stem}-{uuid.uuid4().hex[:8]}.pdf"

    bucket = os.environ["MINIO_MAIL_BUCKET"]
    try:
        _s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Storage error: {e}") from e

    return key
