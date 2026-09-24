"""Merges the per-file transform outputs for one batch into a single file.

Each worker invocation in the transform fan-out writes its own output rows
to a per-input-file key (e.g. ``.../reading-1.json.out``). Once every worker
for a batch finishes, the dispatcher merges all of those per-file outputs
into one combined file so downstream consumers only have to read one object
per batch instead of one per input file.
"""
import json
from typing import List

from app.aws_clients import S3Client
from app.s3_prefixes import list_batch_files


def merge_outputs(s3_client: S3Client, bucket: str, prefix: str, merged_key: str) -> int:
    """Concatenate every ``.out`` file under ``prefix`` into one JSON array
    written to ``merged_key``.

    Returns:
        The total number of rows written to the merged file.
    """
    output_keys = [key for key in list_batch_files(s3_client, bucket, prefix) if key.endswith(".out")]

    merged: List[dict] = []
    for key in sorted(output_keys):
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        merged.extend(json.loads(body))

    s3_client.put_object(Bucket=bucket, Key=merged_key, Body=json.dumps(merged).encode("utf-8"))
    return len(merged)
