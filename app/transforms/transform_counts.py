"""Counts how many readings fall into each category for a batch.
Registered for ``("READING", "COUNTS")``.
"""
from collections import Counter
from typing import List

from app.router import register


@register("READING", "COUNTS")
def transform_counts(payload: dict) -> List[dict]:
    """Count occurrences of each ``category`` value in a batch of readings.

    Args:
        payload: Must contain ``rows``, a list of dicts each shaped like
            ``{"category": "NORMAL"}``.

    Returns:
        One row per distinct category: ``{"code": category, "name":
        "COUNT", "value": count}``.
    """
    counts = Counter(row["category"] for row in payload["rows"])
    return [{"code": category, "name": "COUNT", "value": count} for category, count in counts.items()]
