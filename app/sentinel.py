"""Sentinel-file coordination between pipeline stages.

Lambda invocations in this pipeline don't share memory or a direct call
stack, so "is the previous stage done yet?" is answered by checking for a
marker (sentinel) object in S3 rather than, say, blocking on a future. A
producer writes ``<prefix>COMPLETE`` once its output is fully written; a
consumer polls for that key to appear (bounded by a max attempt count) before
reading the data it depends on.
"""
import time
from typing import Callable

from app.aws_clients import S3Client

SENTINEL_COMPLETE = "COMPLETE"
SENTINEL_IN_PROGRESS = "INPROGRESS"


def _key_exists(s3_client: S3Client, bucket: str, key: str) -> bool:
    try:
        s3_client.get_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 -- any lookup failure means "not found yet"
        return False


def write_sentinel(s3_client: S3Client, bucket: str, prefix: str, name: str = SENTINEL_COMPLETE) -> str:
    """Write an (empty) sentinel object at ``<prefix><name>`` and return its key."""
    key = f"{prefix}{name}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"")
    return key


def wait_for_sentinel(
    s3_client: S3Client,
    bucket: str,
    prefix: str,
    name: str = SENTINEL_COMPLETE,
    attempts: int = 40,
    wait_seconds: float = 15.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> bool:
    """Poll for ``<prefix><name>`` to appear, sleeping ``wait_seconds`` between
    attempts. Returns True as soon as it's found, False if ``attempts`` is
    exhausted first. ``sleep_fn`` is injected purely so tests don't have to
    burn real wall-clock time waiting on a fake clock.
    """
    key = f"{prefix}{name}"
    for attempt in range(attempts):
        if _key_exists(s3_client, bucket, key):
            return True
        if attempt < attempts - 1:
            sleep_fn(wait_seconds)
    return False
