"""Flags readings whose value crosses a configured threshold.
Registered for ``("READING", "FLAGS")``.
"""
from typing import List

from app.router import register


@register("READING", "FLAGS")
def transform_flags(payload: dict) -> List[dict]:
    """Flag any reading whose ``value`` exceeds ``payload["threshold"]``.

    Args:
        payload: Must contain ``rows`` (list of ``{"sensor_id", "value"}``
            dicts) and ``threshold`` (a number).

    Returns:
        One row per flagged reading: ``{"code": sensor_id, "name": "FLAG",
        "value": value}``. Readings at or below the threshold are omitted.
    """
    threshold = payload["threshold"]
    return [
        {"code": row["sensor_id"], "name": "FLAG", "value": row["value"]}
        for row in payload["rows"]
        if row["value"] > threshold
    ]
