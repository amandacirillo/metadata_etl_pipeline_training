"""S3 client, Lambda invoker, and SNS publisher protocols.

Every handler in this training takes these as explicit dependencies instead
of constructing ``boto3.client(...)`` inline, so the fan-out/callback/sentinel
logic can be unit tested with in-memory fakes -- no real AWS access needed.
"""
from typing import Protocol


class S3Client(Protocol):
    def get_object(self, Bucket: str, Key: str) -> dict:
        """Return a dict shaped like boto3's ``get_object`` response, with at
        least a ``Body`` (an object exposing ``.read()`` returning bytes) and
        a ``Metadata`` dict of the object's user-defined metadata."""
        ...

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> dict:
        ...

    def list_objects(self, Bucket: str, Prefix: str, Delimiter: str = "") -> dict:
        """Return a dict shaped like boto3's (non-paginated, single-page)
        ``list_objects`` response: ``{"CommonPrefixes": [{"Prefix": "..."}],
        "Contents": [{"Key": "..."}]}``."""
        ...


class LambdaInvoker(Protocol):
    def invoke(self, function_name: str, payload: dict) -> dict:
        """Synchronously invoke a Lambda function and return its parsed
        JSON response payload."""
        ...


class SnsPublisher(Protocol):
    def publish(self, topic_arn: str, message: dict) -> None: ...


class DataLoader(Protocol):
    def load_batch(self, bucket: str, prefix: str) -> int:
        """Load every record under ``<prefix>`` into wherever this batch's
        data ultimately lives, and return the number of rows loaded.

        A real implementation would bulk-load into a database (the original
        system this training is based on does this); that's intentionally
        out of scope here, so this training ships only an in-memory fake for
        tests and a trivial "count the rows" reference implementation.
        """
        ...
