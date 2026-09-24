"""Computes a simple per-sensor summary (count/min/max/mean) over a batch of
long-format readings. Registered for ``("READING", "SUMMARY")``.
"""
from collections import defaultdict
from typing import List

from app.router import register


@register("READING", "SUMMARY")
def transform_summary(payload: dict) -> List[dict]:
    """Summarize readings by sensor: count, min, max, and mean of ``value``.

    Args:
        payload: Must contain ``rows``, a list of dicts each shaped like
            ``{"sensor_id": "S1", "value": 21.5}``.

    Returns:
        One summary row per distinct ``sensor_id``: ``{"code": sensor_id,
        "name": "SUMMARY", "count", "min", "max", "mean"}``.
    """
    values_by_sensor: dict = defaultdict(list)
    for row in payload["rows"]:
        values_by_sensor[row["sensor_id"]].append(row["value"])

    summaries = []
    for sensor_id, values in values_by_sensor.items():
        summaries.append(
            {
                "code": sensor_id,
                "name": "SUMMARY",
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
            }
        )
    return summaries
