import json

from app.handlers import transform_dispatch_handler, transform_worker_handler
from tests.fakes import FakeLambdaInvoker, FakeS3Client


def _s3_event(bucket="bucket", key="jobs/JOB1/batch-1/context.json"):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def test_transform_worker_routes_by_metadata_and_writes_output():
    s3 = FakeS3Client()
    rows = [{"sensor_id": "S1", "temperature": 21.5}]
    s3.put("jobs/JOB1/batch-1/reading-1.json", json.dumps(rows).encode(), metadata={
        "record-type": "READING", "record-subtype": "RAW",
    })

    event = {
        "bucket": "bucket",
        "key": "jobs/JOB1/batch-1/reading-1.json",
        "jobId": "JOB1",
        "runUser": "trainer",
        "runDate": "2026-01-01",
    }
    result = transform_worker_handler.handle(event, s3)

    assert result["status"] == "OK"
    assert result["outputKey"] == "jobs/JOB1/batch-1/reading-1.json.out"
    output = json.loads(s3.get_object(Bucket="bucket", Key=result["outputKey"])["Body"].read())
    assert output == [{"code": "S1", "name": "temperature", "value": 21.5}]


def test_transform_dispatch_fans_out_merges_and_writes_sentinel():
    s3 = FakeS3Client()
    context_body = {"jobId": "JOB1", "runUser": "trainer", "runDate": "2026-01-01"}
    s3.put("jobs/JOB1/batch-1/context.json", json.dumps(context_body).encode())
    s3.put("jobs/JOB1/batch-1/reading-1.json", b"[]", metadata={"record-type": "READING", "record-subtype": "RAW"})
    s3.put("jobs/JOB1/batch-1/reading-2.json", b"[]", metadata={"record-type": "READING", "record-subtype": "RAW"})

    def fake_worker(payload):
        result = transform_worker_handler.handle(payload, s3)
        return result

    invoker = FakeLambdaInvoker({"transform-worker": fake_worker})

    result = transform_dispatch_handler.handle(_s3_event(), s3, invoker, "transform-worker")

    assert result["status"] == "OK"
    assert result["rowsMerged"] == 0  # both input files are empty lists in this test
    assert len(invoker.calls) == 2
    assert s3.get_object(Bucket="bucket", Key="jobs/JOB1/batch-1/COMPLETE")["Body"].read() == b""


def test_transform_dispatch_reports_error_status_without_merging():
    s3 = FakeS3Client()
    context_body = {"jobId": "JOB1", "runUser": "trainer", "runDate": "2026-01-01"}
    s3.put("jobs/JOB1/batch-1/context.json", json.dumps(context_body).encode())
    s3.put("jobs/JOB1/batch-1/reading-1.json", b"not valid metadata")

    def failing_worker(payload):
        raise RuntimeError("missing metadata tags")

    invoker = FakeLambdaInvoker({"transform-worker": failing_worker})

    result = transform_dispatch_handler.handle(_s3_event(), s3, invoker, "transform-worker")

    assert result["status"] == "ERROR"
    assert "missing metadata tags" in list(result["results"].values())[0]["errorMessage"]
