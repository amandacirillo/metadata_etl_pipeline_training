from app.s3_prefixes import list_batch_files, list_batch_prefixes
from tests.fakes import FakeS3Client


def _seeded_s3():
    s3 = FakeS3Client()
    s3.put("jobs/JOB1/batch-1/reading-1.json", b"[]")
    s3.put("jobs/JOB1/batch-1/reading-2.json", b"[]")
    s3.put("jobs/JOB1/batch-1/context.json", b"{}")
    s3.put("jobs/JOB1/batch-2/reading-1.json", b"[]")
    return s3


def test_list_batch_prefixes_finds_each_batch_folder():
    s3 = _seeded_s3()
    prefixes = list_batch_prefixes(s3, "bucket", "jobs/JOB1/")
    assert sorted(prefixes) == ["jobs/JOB1/batch-1/", "jobs/JOB1/batch-2/"]


def test_list_batch_prefixes_empty_when_nothing_matches():
    s3 = FakeS3Client()
    assert list_batch_prefixes(s3, "bucket", "jobs/NOPE/") == []


def test_list_batch_files_lists_everything_directly_under_prefix():
    s3 = _seeded_s3()
    files = list_batch_files(s3, "bucket", "jobs/JOB1/batch-1/")
    assert sorted(files) == [
        "jobs/JOB1/batch-1/context.json",
        "jobs/JOB1/batch-1/reading-1.json",
        "jobs/JOB1/batch-1/reading-2.json",
    ]


def test_list_batch_files_can_exclude_one_key():
    s3 = _seeded_s3()
    files = list_batch_files(s3, "bucket", "jobs/JOB1/batch-1/", exclude_key="jobs/JOB1/batch-1/context.json")
    assert "jobs/JOB1/batch-1/context.json" not in files
    assert len(files) == 2
