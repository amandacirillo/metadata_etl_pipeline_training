"""Reshapes one wide row-per-sensor record into several long rows (one per
field). Registered for ``("READING", "RAW")``.
"""
from typing import List

from app.router import register

_NON_FIELD_KEYS = {"sensor_id"}


@register("READING", "RAW")
def transform_wide_to_long(payload: dict) -> List[dict]:
    """Pivot each wide reading row into one output row per field.

    Args:
        payload: Must contain ``rows``, a list of dicts each shaped like
            ``{"sensor_id": "S1", "temperature": 21.5, "humidity": 40.0}``.

    Returns:
        One row per (sensor, field) pair: ``{"code": sensor_id, "name":
        field_name, "value": field_value}``.
    """
    output = []
    for row in payload["rows"]:
        sensor_id = row["sensor_id"]
        for field_name, field_value in row.items():
            if field_name in _NON_FIELD_KEYS:
                continue
            output.append({"code": sensor_id, "name": field_name, "value": field_value})
    return output
