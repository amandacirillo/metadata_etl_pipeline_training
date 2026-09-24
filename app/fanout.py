"""Fan out a batch of work items to a downstream worker Lambda in parallel.

The original this is based on used ``multiprocessing.Process`` per item and a
``multiprocessing.Manager().dict()`` to collect results, invoked directly
against another Lambda's ARN (as opposed to going back through SQS, which is
a different fan-out shape than a queue-per-item pipeline). This version uses
a plain ``ThreadPoolExecutor`` instead -- invoking another Lambda is an I/O
wait, not CPU-bound work, so threads are sufficient and considerably easier
to unit test deterministically (no process pickling, no manager process).
"""
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from app.aws_clients import LambdaInvoker


def fan_out(
    function_name: str,
    items: List[dict],
    invoker: LambdaInvoker,
    max_workers: int = 10,
) -> Dict[int, dict]:
    """Invoke ``function_name`` once per item in ``items``, in parallel.

    Args:
        function_name: The downstream worker Lambda's name/ARN.
        items: One payload dict per invocation.
        invoker: Injected :class:`~app.aws_clients.LambdaInvoker`.
        max_workers: Maximum number of concurrent invocations.

    Returns:
        A dict mapping each item's index (in ``items``) to its result. A
        successful invocation's result is whatever the worker returned; a
        failed invocation's result is ``{"errorType": ..., "errorMessage": ...}``
        -- one item failing never prevents the others from completing.
    """
    results: Dict[int, dict] = {}

    def _invoke_one(index: int, payload: dict) -> None:
        try:
            results[index] = invoker.invoke(function_name, payload)
        except Exception as exc:  # noqa: BLE001
            results[index] = {"errorType": type(exc).__name__, "errorMessage": str(exc)}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_invoke_one, index, item) for index, item in enumerate(items)]
        for future in futures:
            future.result()

    return results


def has_errors(results: Dict[int, dict]) -> bool:
    """True if any result in a `fan_out` response dict looks like a failure."""
    return any("errorMessage" in result or result.get("status") == "ERROR" for result in results.values())
