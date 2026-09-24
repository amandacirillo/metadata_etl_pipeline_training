import json

from app.merge import merge_outputs
from tests.fakes import FakeS3Client


def test_merge_outputs_concatenates_all_out_files_in_order():
    s3 = FakeS3Client()
    s3.put("jobs/JOB1/batch-1/reading-1.json.out", json.dumps([{"code": "S1", "value": 1}]).encode())
    s3.put("jobs/JOB1/batch-1/reading-2.json.out", json.dumps([{"code": "S2", "value": 2}]).encode())
    s3.put("jobs/JOB1/batch-1/context.json", b"{}")  # not a .out file, should be ignored

    row_count = merge_outputs(s3, "bucket", "jobs/JOB1/batch-1/", "jobs/JOB1/batch-1/merged.json")

    assert row_count == 2
    merged = json.loads(s3.get_object(Bucket="bucket", Key="jobs/JOB1/batch-1/merged.json")["Body"].read())
    assert merged == [{"code": "S1", "value": 1}, {"code": "S2", "value": 2}]


def test_merge_outputs_empty_when_no_out_files_present():
    s3 = FakeS3Client()
    row_count = merge_outputs(s3, "bucket", "jobs/JOB1/batch-1/", "jobs/JOB1/batch-1/merged.json")
    assert row_count == 0
