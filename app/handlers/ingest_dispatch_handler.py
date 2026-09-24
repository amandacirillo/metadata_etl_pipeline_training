"""SQS-triggered dispatcher for the ingest phase.

Triggered (in the real deployment) by a Step Functions ``waitForTaskToken``
state sending a single SQS message. Discovers each batch sub-prefix under
the job's S3 prefix, fans out one ingest-worker invocation per batch, and
publishes a callback so the waiting Step Functions execution resumes.
"""
import json
import os
from typing import Any, Dict

from app.aws_clients import LambdaInvoker, S3Client, SnsPublisher
from app.callback import build_callback_message, publish_callback
from app.fanout import fan_out
from app.s3_prefixes import list_batch_prefixes


def handle(
    event: dict,
    s3_client: S3Client,
    invoker: LambdaInvoker,
    sns_client: SnsPublisher,
    worker_function_name: str,
    topic_arn: str,
) -> Dict[str, Any]:
    """Business logic for the ingest dispatcher, independent of the Lambda runtime.

    Args:
        event: An SQS event; ``event["Records"][0]["body"]`` must be a JSON
            string with ``jobId``, ``taskToken``, ``bucket``, and ``prefix``.
        s3_client: Injected S3 client used to discover batch prefixes.
        invoker: Injected Lambda invoker used to fan out to the worker.
        sns_client: Injected SNS client used to publish the callback.
        worker_function_name: Name/ARN of the ingest-worker Lambda.
        topic_arn: SNS topic ARN the Step Functions callback is subscribed to.

    Returns:
        The callback message that was published (useful for tests/logging).
    """
    body = json.loads(event["Records"][0]["body"])
    job_id, task_token, bucket, prefix = body["jobId"], body["taskToken"], body["bucket"], body["prefix"]

    batch_prefixes = list_batch_prefixes(s3_client, bucket, prefix)
    items = [{"bucket": bucket, "prefix": batch_prefix} for batch_prefix in batch_prefixes]
    results = fan_out(worker_function_name, items, invoker)

    message = build_callback_message(job_id, task_token, results)
    publish_callback(sns_client, topic_arn, message)
    return message


def lambda_handler(event: dict, context: Any) -> dict:
    import boto3

    s3_client = boto3.client("s3")
    lambda_client = boto3.client("lambda")
    sns_client = boto3.client("sns")

    class _BotoLambdaInvoker:
        def invoke(self, function_name: str, payload: dict) -> dict:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )
            return json.load(response["Payload"])

    class _BotoSnsPublisher:
        def publish(self, topic_arn: str, message: dict) -> None:
            sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps({"default": json.dumps(message)}),
                MessageStructure="json",
            )

    message = handle(
        event,
        s3_client,
        _BotoLambdaInvoker(),
        _BotoSnsPublisher(),
        worker_function_name=os.environ["INGEST_WORKER_FUNCTION"],
        topic_arn=os.environ["STATUS_TOPIC_ARN"],
    )
    return {"statusCode": 200, "body": json.dumps(message)}
