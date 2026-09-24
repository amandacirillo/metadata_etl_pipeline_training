import json

from app.handlers import ingest_dispatch_handler, ingest_worker_handler
from app.handlers.ingest_worker_handler import SentinelTimeoutError
from tests.fakes import FakeDataLoader, FakeLambdaInvoker, FakeS3Client, FakeSnsPublisher


def _sqs_event(job_id="JOB1", task_token="token-abc", bucket="bucket", prefix="jobs/JOB1/"):
    body = {"jobId": job_id, "taskToken": task_token, "bucket": bucket, "prefix": prefix}
    return {"Records": [{"body": json.dumps(body)}]}


def test_ingest_dispatch_fans_out_to_each_batch_and_publishes_complete():
    s3 = FakeS3Client()
    s3.put("jobs/JOB1/batch-1/reading-1.json", b"[]")
    s3.put("jobs/JOB1/batch-2/reading-1.json", b"[]")

    invoker = FakeLambdaInvoker({"ingest-worker": lambda payload: {"status": "OK", "rowsLoaded": 3}})
    sns = FakeSnsPublisher()

    message = ingest_dispatch_handler.handle(_sqs_event(), s3, invoker, sns, "ingest-worker", "topic-arn")

    assert message["status"] == "COMPLETE"
    assert message["jobId"] == "JOB1"
    assert len(invoker.calls) == 2
    assert sns.published == [("topic-arn", message)]


def test_ingest_dispatch_reports_error_when_a_batch_fails():
    s3 = FakeS3Client()
    s3.put("jobs/JOB1/batch-1/reading-1.json", b"[]")

    def failing_worker(payload):
        raise RuntimeError("sentinel never appeared")

    invoker = FakeLambdaInvoker({"ingest-worker": failing_worker})
    sns = FakeSnsPublisher()

    message = ingest_dispatch_handler.handle(_sqs_event(), s3, invoker, sns, "ingest-worker", "topic-arn")

    assert message["status"] == "ERROR"
    assert "sentinel never appeared" in message["errorDetails"][0]["message"]


def test_ingest_worker_loads_batch_once_sentinel_is_present():
    s3 = FakeS3Client()
    s3.put("jobs/JOB1/batch-1/COMPLETE", b"")
    loader = FakeDataLoader(rows_loaded=7)

    result = ingest_worker_handler.handle(
        {"bucket": "bucket", "prefix": "jobs/JOB1/batch-1/"}, s3, loader, attempts=1, sleep_fn=lambda s: None
    )

    assert result == {"status": "OK", "rowsLoaded": 7}
    assert loader.calls == [("bucket", "jobs/JOB1/batch-1/")]


def test_ingest_worker_raises_when_sentinel_never_appears():
    s3 = FakeS3Client()
    loader = FakeDataLoader(rows_loaded=7)

    try:
        ingest_worker_handler.handle(
            {"bucket": "bucket", "prefix": "jobs/JOB1/never/"}, s3, loader, attempts=2, sleep_fn=lambda s: None
        )
        assert False, "expected SentinelTimeoutError"
    except SentinelTimeoutError:
        pass
    assert loader.calls == []
