"""Helpers for the MinIO data lake (S3 API)."""

from __future__ import annotations

import boto3
from botocore.config import Config

from bikeshare.config import env, lake_bucket


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=env("S3_ACCESS_KEY"),
        aws_secret_access_key=env("S3_SECRET_KEY"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


def object_exists(key: str, bucket: str | None = None) -> bool:
    client = s3_client()
    try:
        client.head_object(Bucket=bucket or lake_bucket(), Key=key)
        return True
    except client.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def delete_prefix(prefix: str, bucket: str | None = None) -> int:
    client = s3_client()
    bucket = bucket or lake_bucket()
    deleted = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            deleted += len(objects)
    return deleted
