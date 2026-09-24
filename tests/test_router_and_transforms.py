import pytest

from app.router import DuplicateTransformError, UnknownRecordTypeError, dispatch, register, registered_types
from app.transforms import transform_context, transform_counts, transform_flags, transform_summary  # noqa: F401
from app.transforms import transform_wide_to_long  # noqa: F401


def test_registered_types_includes_all_transform_modules():
    types = registered_types()
    assert ("CONTEXT", "METADATA") in types
    assert ("READING", "RAW") in types
    assert ("READING", "SUMMARY") in types
    assert ("READING", "COUNTS") in types
    assert ("READING", "FLAGS") in types


def test_dispatch_routes_to_the_registered_transform():
    payload = {"rows": [{"sensor_id": "S1", "temperature": 21.5}]}
    result = dispatch("READING", "RAW", payload)
    assert result == [{"code": "S1", "name": "temperature", "value": 21.5}]


def test_dispatch_raises_for_unknown_record_type():
    with pytest.raises(UnknownRecordTypeError):
        dispatch("NOPE", "NOPE", {})


def test_register_raises_on_duplicate_registration():
    @register("TEST_ONLY", "A")
    def _first(payload):
        return []

    with pytest.raises(DuplicateTransformError):

        @register("TEST_ONLY", "A")
        def _second(payload):
            return []


def test_transform_context_builds_single_row():
    from app.transforms.transform_context import transform_context as fn

    payload = {"job_id": "JOB1", "run_user": "trainer", "run_date": "2026-01-01"}
    assert fn(payload) == [
        {"code": "CONTEXT", "name": "JOB1", "value": "trainer", "created_by": "trainer", "created_at": "2026-01-01"}
    ]


def test_transform_wide_to_long_pivots_each_field():
    from app.transforms.transform_wide_to_long import transform_wide_to_long as fn

    payload = {"rows": [{"sensor_id": "S1", "temperature": 21.5, "humidity": 40.0}]}
    result = fn(payload)

    assert {"code": "S1", "name": "temperature", "value": 21.5} in result
    assert {"code": "S1", "name": "humidity", "value": 40.0} in result
    assert len(result) == 2


def test_transform_summary_computes_stats_per_sensor():
    from app.transforms.transform_summary import transform_summary as fn

    payload = {"rows": [{"sensor_id": "S1", "value": 1.0}, {"sensor_id": "S1", "value": 3.0}]}
    result = fn(payload)

    assert result == [{"code": "S1", "name": "SUMMARY", "count": 2, "min": 1.0, "max": 3.0, "mean": 2.0}]


def test_transform_counts_counts_categories():
    from app.transforms.transform_counts import transform_counts as fn

    payload = {"rows": [{"category": "NORMAL"}, {"category": "NORMAL"}, {"category": "ALERT"}]}
    result = fn(payload)

    result_by_code = {row["code"]: row["value"] for row in result}
    assert result_by_code == {"NORMAL": 2, "ALERT": 1}


def test_transform_flags_only_returns_rows_over_threshold():
    from app.transforms.transform_flags import transform_flags as fn

    payload = {"threshold": 10, "rows": [{"sensor_id": "S1", "value": 5}, {"sensor_id": "S2", "value": 15}]}
    result = fn(payload)

    assert result == [{"code": "S2", "name": "FLAG", "value": 15}]
