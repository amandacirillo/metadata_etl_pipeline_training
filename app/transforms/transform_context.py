"""Builds the single "context" record summarizing a batch's run metadata.

Registered for ``("CONTEXT", "METADATA")``.
"""
from typing import List

from app.router import register


@register("CONTEXT", "METADATA")
def transform_context(payload: dict) -> List[dict]:
    """Build one context row out of a batch's run metadata.

    Args:
        payload: Must contain ``job_id``, ``run_user``, and ``run_date``.

    Returns:
        A single-row list: ``[{"code": "CONTEXT", "name": job_id, "value":
        run_user, "created_by": run_user, "created_at": run_date}]``.
    """
    return [
        {
            "code": "CONTEXT",
            "name": payload["job_id"],
            "value": payload["run_user"],
            "created_by": payload["run_user"],
            "created_at": payload["run_date"],
        }
    ]
