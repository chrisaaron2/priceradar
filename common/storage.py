"""S3 helpers. Credentials come from the standard AWS environment/credential chain."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from common.log import get_logger

logger = get_logger(__name__)


def bucket_name() -> str | None:
    return os.getenv("S3_BUCKET_NAME") or None


def s3_client():
    import boto3

    return boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))


def upload_json(records: Any, prefix: str) -> str | None:
    """Write records as JSON to s3://<bucket>/<prefix>/<UTC timestamp>.json.

    Returns the S3 URI, or None when no bucket is configured or the upload fails.
    S3 is the raw archive, so a failure here is logged but never stops the run.
    """
    bucket = bucket_name()
    if not bucket:
        logger.warning("S3_BUCKET_NAME not set; skipping raw archive for %s", prefix)
        return None

    key = f"{prefix.strip('/')}/{datetime.now(timezone.utc):%Y-%m-%dT%H-%M-%SZ}.json"
    try:
        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(records, default=str).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:
        logger.exception("S3 upload failed for s3://%s/%s", bucket, key)
        return None

    uri = f"s3://{bucket}/{key}"
    logger.info("Archived raw data to %s", uri)
    return uri


def upload_bytes(body: bytes, key: str, content_type: str = "application/octet-stream") -> str:
    """Upload bytes to the configured bucket and return the S3 URI. Raises on failure."""
    bucket = bucket_name()
    if not bucket:
        raise RuntimeError("S3_BUCKET_NAME is not set")
    s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    return f"s3://{bucket}/{key}"
