"""Durable FIFO boundaries for physical-visit ready publication."""

from __future__ import annotations

import os
from contextlib import suppress
from dataclasses import dataclass
from json import JSONDecodeError, dumps, loads
from pathlib import Path
from typing import cast
from uuid import uuid4


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


def load(path: Path) -> tuple[ReadyClosure, ...]:
    """Read the strict queue; a missing queue means no closed visit exists."""
    try:
        if path.is_symlink():
            raise ReadyClosureError("ready closure queue must not be a symlink")
        value = cast(object, loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return ()
    except (OSError, JSONDecodeError, UnicodeDecodeError) as error:
        raise ReadyClosureError("ready closure queue is unreadable") from error
    if not isinstance(value, dict) or set(value) != _FIELDS or value.get("version") != _VERSION:
        raise ReadyClosureError("ready closure queue schema is invalid")
    raw_closures = value.get("closures")
    if not isinstance(raw_closures, list):
        raise ReadyClosureError("ready closure queue schema is invalid")
    return tuple(_parse_closure(item) for item in raw_closures)


def append(path: Path, next_sequence: int, reason: str) -> ReadyClosure:
    """Append one frontier durably, coalescing a replay of the same frontier."""
    closure = _make_closure(next_sequence, reason)
    closures = load(path)
    if closures and closures[-1].next_sequence == next_sequence:
        return closures[-1]
    _write(path, (*closures, closure))
    return closure


def remove(path: Path, closure: ReadyClosure) -> None:
    """Remove only the queue head after its ready/source work completed."""
    closures = load(path)
    if not closures or closures[0] != closure:
        raise ReadyClosureError("ready closure queue head changed")
    _write(path, closures[1:])


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


def _write(path: Path, closures: tuple[ReadyClosure, ...]) -> None:
    if path.is_symlink():
        raise ReadyClosureError("ready closure queue must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dumps(
        {
            "closures": [{"next_sequence": item.next_sequence, "reason": item.reason} for item in closures],
            "version": _VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        raise ReadyClosureError("ready closure queue could not be written") from error
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()
