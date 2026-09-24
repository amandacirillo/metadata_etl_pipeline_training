"""Per-file transform worker: read one input file's metadata tags, route it
to the correct registered transform, and write the result rows next to it.
"""
import json
from typing import Any, Dict

from app.aws_clients import S3Client
from app.router import dispatch


def handle(event: dict, s3_client: S3Client) -> Dict[str, Any]:
    """Business logic for the transform worker, independent of the Lambda runtime.

    Args:
        event: Must contain ``bucket`` and ``key`` for one input file, plus
            ``jobId``, ``runUser``, ``runDate`` (passed through from the
            batch's context file by the dispatcher).
        s3_client: Injected S3 client.

    Returns:
        ``{"status": "OK", "outputKey": ..., "rowCount": n}``.
    """
    bucket, key = event["bucket"], event["key"]

    response = s3_client.get_object(Bucket=bucket, Key=key)
    record_type = response["Metadata"]["record-type"]
    record_subtype = response["Metadata"]["record-subtype"]
    rows = json.loads(response["Body"].read())

    payload = {
        "rows": rows,
        "job_id": event["jobId"],
        "run_user": event["runUser"],
        "run_date": event["runDate"],
        "threshold": event.get("threshold", 0),
    }
    output_rows = dispatch(record_type, record_subtype, payload)

    output_key = f"{key}.out"
    s3_client.put_object(Bucket=bucket, Key=output_key, Body=json.dumps(output_rows).encode("utf-8"))
    return {"status": "OK", "outputKey": output_key, "rowCount": len(output_rows)}


def lambda_handler(event: dict, context: Any) -> dict:
    import boto3

    s3_client = boto3.client("s3")

    try:
        result = handle(event, s3_client)
        return {"statusCode": 200, "body": result}
    except Exception as exc:  # noqa: BLE001
        return {"errorType": type(exc).__name__, "errorMessage": str(exc)}
