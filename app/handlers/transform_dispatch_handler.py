"""S3-event-triggered dispatcher for the transform phase.

Triggered (in the real deployment) whenever a job's "context" file lands in
S3 -- the context file signals that every raw file for that batch has
finished uploading. Reads the context file for run metadata, fans out one
transform-worker invocation per sibling file in the batch, then merges all
of the workers' outputs and writes the batch's completion sentinel.
"""
import json
from typing import Any, Dict
from urllib.parse import unquote_plus

from app.aws_clients import LambdaInvoker, S3Client
from app.fanout import fan_out, has_errors
from app.merge import merge_outputs
from app.s3_prefixes import list_batch_files
from app.sentinel import write_sentinel


def handle(event: dict, s3_client: S3Client, invoker: LambdaInvoker, worker_function_name: str) -> Dict[str, Any]:
    """Business logic for the transform dispatcher, independent of the Lambda runtime.

    Args:
        event: An S3 ``ObjectCreated`` event for a batch's context file.
        s3_client: Injected S3 client.
        invoker: Injected Lambda invoker used to fan out to the worker.
        worker_function_name: Name/ARN of the transform-worker Lambda.

    Returns:
        ``{"status": "OK"|"ERROR", "rowsMerged": n, "results": {...}}``.
    """
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    context_key = unquote_plus(record["object"]["key"])
    prefix = context_key.rsplit("/", 1)[0] + "/"

    context_body = json.loads(s3_client.get_object(Bucket=bucket, Key=context_key)["Body"].read())
    job_id, run_user, run_date = context_body["jobId"], context_body["runUser"], context_body["runDate"]

    file_keys = list_batch_files(s3_client, bucket, prefix, exclude_key=context_key)
    items = [
        {"bucket": bucket, "key": key, "jobId": job_id, "runUser": run_user, "runDate": run_date}
        for key in file_keys
    ]
    results = fan_out(worker_function_name, items, invoker)

    if has_errors(results):
        return {"status": "ERROR", "results": results}

    merged_key = f"{prefix}merged.json"
    rows_merged = merge_outputs(s3_client, bucket, prefix, merged_key)
    write_sentinel(s3_client, bucket, prefix)

    return {"status": "OK", "rowsMerged": rows_merged, "results": results}


def lambda_handler(event: dict, context: Any) -> dict:
    import os

    import boto3

    s3_client = boto3.client("s3")
    lambda_client = boto3.client("lambda")

    class _BotoLambdaInvoker:
        def invoke(self, function_name: str, payload: dict) -> dict:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            return json.load(response["Payload"])

    worker_function_name = os.environ["TRANSFORM_WORKER_FUNCTION"]
    result = handle(event, s3_client, _BotoLambdaInvoker(), worker_function_name)
    return {"statusCode": 200, "body": json.dumps(result)}
