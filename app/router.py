"""Registry-based dispatch from a record's ``(record_type, record_subtype)``
metadata tag to the transform function that knows how to reshape it.

The original system this is modeled on routed to one of ~25 domain-specific
transform functions via a long chain of ``if stat_type == 'X': ... elif
stat_type == 'Y' and stat_type_sub == 'Z': ...``. That works, but every new
record type means editing that one giant function, and nothing stops two
branches from silently overlapping. This version uses an explicit registry
instead: each transform module registers itself against the
``(record_type, record_subtype)`` pair it handles, and ``dispatch`` is a
single dict lookup. Registering the same pair twice is a startup-time error
instead of a silent "whichever branch comes first wins".
"""
from typing import Callable, Dict, List, Tuple

TransformFn = Callable[[dict], List[dict]]

_REGISTRY: Dict[Tuple[str, str], TransformFn] = {}


class DuplicateTransformError(ValueError):
    pass


class UnknownRecordTypeError(ValueError):
    pass


def register(record_type: str, record_subtype: str) -> Callable[[TransformFn], TransformFn]:
    """Class/function decorator registering a transform for a record type pair.

    Args:
        record_type: The record's primary type tag (e.g. ``"READING"``).
        record_subtype: The record's secondary type tag (e.g. ``"SUMMARY"``).

    Raises:
        DuplicateTransformError: If a transform is already registered for
            this exact ``(record_type, record_subtype)`` pair.
    """

    def decorator(fn: TransformFn) -> TransformFn:
        key = (record_type, record_subtype)
        if key in _REGISTRY:
            raise DuplicateTransformError(f"A transform is already registered for {key}")
        _REGISTRY[key] = fn
        return fn

    return decorator


def dispatch(record_type: str, record_subtype: str, payload: dict) -> List[dict]:
    """Route ``payload`` to the transform registered for this record type pair.

    Raises:
        UnknownRecordTypeError: If no transform is registered for
            ``(record_type, record_subtype)``.
    """
    key = (record_type, record_subtype)
    transform = _REGISTRY.get(key)
    if transform is None:
        raise UnknownRecordTypeError(f"No transform registered for {key}")
    return transform(payload)


def registered_types() -> List[Tuple[str, str]]:
    """Return every currently-registered ``(record_type, record_subtype)`` pair."""
    return list(_REGISTRY.keys())
