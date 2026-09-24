"""In-memory fakes for the AWS client protocols used across this pipeline's
tests -- no real AWS access, no moto, no network."""
import io
from typing import Callable, Dict, List, Optional


class FakeS3Client:
    """A tiny in-memory S3 stand-in: `store` maps key -> (body_bytes, metadata_dict)."""

    def __init__(self, store: Optional[Dict[str, tuple]] = None):
        self.store: Dict[str, tuple] = store or {}

    def put(self, key: str, body: bytes, metadata: Optional[dict] = None) -> None:
        """Test helper for seeding an object directly (bypasses put_object)."""
        self.store[key] = (body, metadata or {})

    def get_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.store:
            raise KeyError(f"no such key: {Key}")
        body, metadata = self.store[Key]
        return {"Body": io.BytesIO(body), "Metadata": metadata}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> dict:
        self.store[Key] = (Body, {})
        return {}

    def list_objects(self, Bucket: str, Prefix: str, Delimiter: str = "") -> dict:
        contents = []
        common_prefixes = set()
        for key in self.store:
            if not key.startswith(Prefix):
                continue
            remainder = key[len(Prefix):]
            if Delimiter and Delimiter in remainder:
                common_prefixes.add(Prefix + remainder.split(Delimiter)[0] + Delimiter)
            else:
                contents.append({"Key": key})

        result: dict = {}
        if contents:
            result["Contents"] = contents
        if common_prefixes:
            result["CommonPrefixes"] = [{"Prefix": p} for p in sorted(common_prefixes)]
        return result


class FakeLambdaInvoker:
    """Routes `invoke(function_name, payload)` straight to a handler callable,
    simulating a synchronous Lambda-to-Lambda invocation without any AWS access."""

    def __init__(self, handlers: Dict[str, Callable[[dict], dict]]):
        self._handlers = handlers
        self.calls: List[tuple] = []

    def invoke(self, function_name: str, payload: dict) -> dict:
        self.calls.append((function_name, payload))
        return self._handlers[function_name](payload)


class FakeSnsPublisher:
    def __init__(self) -> None:
        self.published: List[tuple] = []

    def publish(self, topic_arn: str, message: dict) -> None:
        self.published.append((topic_arn, message))


class FakeDataLoader:
    def __init__(self, rows_loaded: int = 0, error: Optional[Exception] = None):
        self.rows_loaded = rows_loaded
        self.error = error
        self.calls: List[tuple] = []

    def load_batch(self, bucket: str, prefix: str) -> int:
        self.calls.append((bucket, prefix))
        if self.error is not None:
            raise self.error
        return self.rows_loaded
