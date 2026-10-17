"""Durable FIFO boundaries for physical-visit ready publication."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from itertools import pairwise
from json import JSONDecodeError, loads
from pathlib import Path
from typing import cast

from .collector_state_db import StateDB, StateDBError, StateKind, StateTransaction


class ReadyClosureError(ValueError):
    """The durable closure queue is malformed or cannot advance safely."""


@dataclass(frozen=True, slots=True)
class ReadyClosure:
    """One physical visit frontier waiting for ready publication."""

    next_sequence: int
    reason: str


_VERSION = 1
_FIELDS = frozenset({"version", "closures"})
_CLOSURE_FIELDS = frozenset({"next_sequence", "reason"})


def legacy_closures(path: Path) -> dict[str, object] | None:
    """Read and validate the old file-backed closure queue for migration."""
    decoded = _read_legacy_document(path)
    return None if decoded is None else _closure_payload(decoded)


def _read_legacy_document(path: Path) -> object | None:
    try:
        if path.is_symlink():
            raise ReadyClosureError("ready closure queue must not be a symlink")
        return cast(object, loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return None
    except (OSError, JSONDecodeError, UnicodeDecodeError) as error:
        raise ReadyClosureError("ready closure queue is unreadable") from error


def _closure_payload(decoded: object) -> dict[str, object]:
    if not isinstance(decoded, dict) or any(not isinstance(key, str) for key in decoded):
        raise ReadyClosureError("ready closure queue schema is invalid")
    value = cast(dict[str, object], decoded)
    if set(value) != _FIELDS or value.get("version") != _VERSION:
        raise ReadyClosureError("ready closure queue schema is invalid")
    raw_closures = value.get("closures")
    if not isinstance(raw_closures, list):
        raise ReadyClosureError("ready closure queue schema is invalid")
    closures = tuple(_parse_closure(item) for item in raw_closures)
    _require_ordered(closures)
    return {
        "version": _VERSION,
        "closures": [{"next_sequence": item.next_sequence, "reason": item.reason} for item in closures],
    }


def _require_ordered(closures: tuple[ReadyClosure, ...]) -> None:
    if any(left.next_sequence > right.next_sequence for left, right in pairwise(closures)):
        raise ReadyClosureError("ready closure queue is out of order")


def load(db: StateDB) -> tuple[ReadyClosure, ...]:
    """Read the strict database queue; a missing row means no closed visit exists."""
    try:
        value = db.get(StateKind.READY_CLOSURE, "current")
    except (OSError, ValueError, StateDBError) as error:
        raise ReadyClosureError("ready closure queue is unreadable") from error
    return _parse_document(value)


def _load(transaction: StateTransaction) -> tuple[ReadyClosure, ...]:
    return _parse_document(transaction.get(StateKind.READY_CLOSURE, "current"))


def _parse_document(value: dict[str, object] | None) -> tuple[ReadyClosure, ...]:
    if value is None:
        return ()
    if not isinstance(value, dict) or set(value) != _FIELDS or value.get("version") != _VERSION:
        raise ReadyClosureError("ready closure queue schema is invalid")
    raw_closures = value.get("closures")
    if not isinstance(raw_closures, list):
        raise ReadyClosureError("ready closure queue schema is invalid")
    closures = tuple(_parse_closure(item) for item in raw_closures)
    if any(left.next_sequence > right.next_sequence for left, right in pairwise(closures)):
        raise ReadyClosureError("ready closure queue is out of order")
    return closures


def append(
    db: StateDB,
    next_sequence: int,
    reason: str,
    *,
    on_commit: Callable[[StateTransaction], None] | None = None,
) -> ReadyClosure:
    """Keep only the newest durable watermark, upgrading an equal drain."""
    closure = _make_closure(next_sequence, reason)
    try:
        with db.transaction() as transaction:
            closures = _load(transaction)
            if closures and next_sequence < closures[-1].next_sequence:
                raise ReadyClosureError("ready closure frontier regressed")
            if closures and closures[-1].next_sequence == next_sequence:
                previous = closures[-1]
                if previous.reason == "drained" or reason != "drained":
                    closure = previous
            if closures != (closure,):
                _put(transaction, (closure,))
            if on_commit is not None:
                on_commit(transaction)
    except (OSError, ValueError, StateDBError) as error:
        if isinstance(error, ReadyClosureError):
            raise
        raise ReadyClosureError("ready closure queue could not be written") from error
    return closure


def coalesce(db: StateDB) -> tuple[ReadyClosure, ...]:
    """Persist only the latest watermark from a legacy FIFO queue."""
    closures = load(db)
    if len(closures) > 1:
        _write(db, (closures[-1],))
        return (closures[-1],)
    return closures


def begin_visit(db: StateDB) -> ReadyClosure | None:
    """Durably revoke an earlier drain permit before another visit starts."""
    closures = coalesce(db)
    if not closures:
        return None
    previous = closures[-1]
    if previous.reason != "drained":
        return previous
    collecting = ReadyClosure(previous.next_sequence, "collecting")
    _write(db, (collecting,))
    return collecting


def remove(db: StateDB, closure: ReadyClosure) -> None:
    """Remove only the queue head after its ready/source work completed."""
    closures = load(db)
    if not closures or closures[0] != closure:
        raise ReadyClosureError("ready closure queue head changed")
    _write(db, closures[1:])


def _parse_closure(value: object) -> ReadyClosure:
    if not isinstance(value, dict) or set(value) != _CLOSURE_FIELDS:
        raise ReadyClosureError("ready closure entry schema is invalid")
    return _make_closure(value["next_sequence"], value["reason"])


def _make_closure(next_sequence: object, reason: object) -> ReadyClosure:
    if isinstance(next_sequence, bool) or not isinstance(next_sequence, int) or next_sequence < 0:
        raise ReadyClosureError("ready closure frontier is invalid")
    if not isinstance(reason, str) or not reason:
        raise ReadyClosureError("ready closure reason is invalid")
    return ReadyClosure(next_sequence, reason)


def _write(db: StateDB, closures: tuple[ReadyClosure, ...]) -> None:
    try:
        with db.transaction() as transaction:
            _put(transaction, closures)
    except (OSError, ValueError, StateDBError) as error:
        raise ReadyClosureError("ready closure queue could not be written") from error


def _put(transaction: StateTransaction, closures: tuple[ReadyClosure, ...]) -> None:
    transaction.put(
        StateKind.READY_CLOSURE,
        "current",
        {
            "closures": [{"next_sequence": item.next_sequence, "reason": item.reason} for item in closures],
            "version": _VERSION,
        },
    )
