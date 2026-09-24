"""Discover the batch sub-folders for a job, and the files within one batch."""
from typing import List, Optional

from app.aws_clients import S3Client


def list_batch_prefixes(s3_client: S3Client, bucket: str, start_prefix: str) -> List[str]:
    """List the immediate "folder" prefixes directly under ``start_prefix``.

    Each job uploads its raw batches into sibling S3 "folders" (really just
    common key prefixes) under one job prefix, e.g.
    ``jobs/JOB123/batch-1/``, ``jobs/JOB123/batch-2/``, ... This returns
    each such prefix so the dispatcher can fan out one worker invocation per
    batch.
    """
    response = s3_client.list_objects(Bucket=bucket, Prefix=start_prefix, Delimiter="/")
    return [entry["Prefix"] for entry in response.get("CommonPrefixes", [])]


def list_batch_files(s3_client: S3Client, bucket: str, prefix: str, exclude_key: Optional[str] = None) -> List[str]:
    """List every object key directly under ``prefix``, optionally excluding one key."""
    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    keys = [entry["Key"] for entry in response.get("Contents", [])]
    return [key for key in keys if key != exclude_key]
