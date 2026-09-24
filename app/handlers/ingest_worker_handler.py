"""Per-batch ingest worker: wait for the batch's completion sentinel, then
load its rows.
"""
import os
from typing import Any, Callable, Dict, Optional

from app.aws_clients import DataLoader, S3Client
from app.sentinel import wait_for_sentinel


class SentinelTimeoutError(TimeoutError):
    pass


def handle(event: dict, s3_client: S3Client, loader: DataLoader, attempts: int = 40, wait_seconds: float = 15.0,
           sleep_fn: Optional[Callable[[float], None]] = None) -> Dict[str, Any]:
    """Business logic for the ingest worker, independent of the Lambda runtime.

    Args:
        event: Must contain ``bucket`` and ``prefix`` for this one batch.
        s3_client: Injected S3 client used to poll for the sentinel.
        loader: Injected :class:`~app.aws_clients.DataLoader`.
        attempts: Max sentinel-poll attempts before giving up.
        wait_seconds: Delay between poll attempts.
        sleep_fn: Optional sleep function override (tests inject a no-op).

    Returns:
        ``{"status": "OK", "rowsLoaded": n}``.

    Raises:
        SentinelTimeoutError: If the batch's completion sentinel never appears.
    """
    bucket, prefix = event["bucket"], event["prefix"]

    if sleep_fn is None:
        found = wait_for_sentinel(s3_client, bucket, prefix, attempts=attempts, wait_seconds=wait_seconds)
    else:
        found = wait_for_sentinel(
            s3_client, bucket, prefix, attempts=attempts, wait_seconds=wait_seconds, sleep_fn=sleep_fn
        )
    if not found:
        raise SentinelTimeoutError(f"{bucket}/{prefix} never produced a completion sentinel")

    rows_loaded = loader.load_batch(bucket, prefix)
    return {"status": "OK", "rowsLoaded": rows_loaded}


def lambda_handler(event: dict, context: Any) -> dict:
    import boto3

    s3_client = boto3.client("s3")

    class _RowCountingLoader:
        """Reference DataLoader: counts newline-delimited JSON rows under the
        batch prefix instead of writing anywhere. Swap in a real database
        loader for production use."""

        def load_batch(self, bucket: str, prefix: str) -> int:
            response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
            return len(response.get("Contents", []))

    try:
        result = handle(
            event,
            s3_client,
            _RowCountingLoader(),
            attempts=int(os.environ.get("SENTINEL_ATTEMPTS", "40")),
            wait_seconds=float(os.environ.get("SENTINEL_WAIT_SECONDS", "15")),
        )
        return {"statusCode": 200, "body": result}
    except Exception as exc:  # noqa: BLE001
        return {"errorType": type(exc).__name__, "errorMessage": str(exc)}
