from app.sentinel import wait_for_sentinel, write_sentinel
from tests.fakes import FakeS3Client


def test_write_sentinel_creates_marker_object():
    s3 = FakeS3Client()
    key = write_sentinel(s3, "my-bucket", "jobs/JOB1/batch-1/")
    assert key == "jobs/JOB1/batch-1/COMPLETE"
    assert s3.get_object(Bucket="my-bucket", Key=key)["Body"].read() == b""


def test_wait_for_sentinel_returns_true_immediately_when_present():
    s3 = FakeS3Client()
    write_sentinel(s3, "my-bucket", "jobs/JOB1/batch-1/")

    found = wait_for_sentinel(s3, "my-bucket", "jobs/JOB1/batch-1/", attempts=1, sleep_fn=lambda seconds: None)

    assert found is True


def test_wait_for_sentinel_polls_until_it_appears():
    s3 = FakeS3Client()
    poll_count = {"n": 0}

    def sleep_and_write(seconds):
        poll_count["n"] += 1
        if poll_count["n"] == 2:
            write_sentinel(s3, "my-bucket", "jobs/JOB1/batch-1/")

    found = wait_for_sentinel(
        s3, "my-bucket", "jobs/JOB1/batch-1/", attempts=5, wait_seconds=0, sleep_fn=sleep_and_write
    )

    assert found is True
    assert poll_count["n"] == 2


def test_wait_for_sentinel_gives_up_after_max_attempts():
    s3 = FakeS3Client()
    found = wait_for_sentinel(s3, "my-bucket", "jobs/JOB1/never/", attempts=3, sleep_fn=lambda seconds: None)
    assert found is False
