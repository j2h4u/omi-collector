"""Bounded durable ledger for confirmed device-cursor loss intervals."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

from ..domain.ring_protocol import RECORD_SIZE
from .staging_contract import StagingError

_SCHEMA_VERSION = 1
_MAX_LEDGER_BYTES = 1_048_576
_U64_MAX = (1 << 64) - 1
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}\Z")
_LOSS_ID = re.compile(r"[0-9a-f]{64}\Z")
_REASON = "device_cursor_advanced_before_host_durable_prefix"
_FACT_FIELDS = {
    "attempt_id",
    "end_sequence",
    "loss_id",
    "missing_raw_bytes",
    "missing_record_count",
    "occurred_at",
    "reason",
    "start_sequence",
}


class ConfirmedLossError(StagingError):
    """A confirmed loss fact could not be safely initialized or persisted."""


@dataclass(frozen=True, slots=True)
class ConfirmedLossFact:
    loss_id: str
    attempt_id: str
    start_sequence: int
    end_sequence: int
    occurred_at: str
    missing_record_count: int
    missing_raw_bytes: int
    reason: str = _REASON

    def as_dict(self) -> dict[str, object]:
        return {
            "attempt_id": self.attempt_id,
            "end_sequence": self.end_sequence,
            "loss_id": self.loss_id,
            "missing_raw_bytes": self.missing_raw_bytes,
            "missing_record_count": self.missing_record_count,
            "occurred_at": self.occurred_at,
            "reason": self.reason,
            "start_sequence": self.start_sequence,
        }


class ConfirmedLossLedger:
    """Atomic bounded snapshot ledger; callers serialize writers with the spool lease."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def initialize(self, *, allow_create: bool) -> None:
        """Create an empty first-use ledger, never recreating one after status was initialized."""
        ledger_exists = os.path.lexists(self.path)
        if ledger_exists:
            self._read_ledger()
            return
        if not allow_create:
            raise ConfirmedLossError("confirmed-loss ledger is missing after status initialization")
        self._write_ledger(())

    def read(self) -> tuple[ConfirmedLossFact, ...]:
        return self._read_ledger()

    def record(self, attempt_id: str, start_sequence: int, end_sequence: int, occurred_at: str) -> str:
        facts = self._read_ledger()
        loss_id = _loss_id(attempt_id, start_sequence, end_sequence)
        count = end_sequence - start_sequence
        fact = ConfirmedLossFact(
            loss_id=loss_id,
            attempt_id=attempt_id,
            start_sequence=start_sequence,
            end_sequence=end_sequence,
            occurred_at=occurred_at,
            missing_record_count=count,
            missing_raw_bytes=count * RECORD_SIZE,
        )
        _validate_fact(fact)
        if any(existing.loss_id == loss_id for existing in facts):
            return loss_id
        self._write_ledger((*facts, fact))
        return loss_id

    def _read_ledger(self) -> tuple[ConfirmedLossFact, ...]:
        payload = _read_bounded_regular_file(self.path, _MAX_LEDGER_BYTES)
        try:
            document = cast(object, json.loads(payload))
            if not isinstance(document, dict):
                raise ValueError
            ledger = cast(dict[str, object], document)
            if set(ledger) != {"schema_version", "losses"}:
                raise ValueError
            if type(ledger["schema_version"]) is not int or ledger["schema_version"] != _SCHEMA_VERSION:
                raise ValueError
            raw_facts = ledger["losses"]
            if not isinstance(raw_facts, list):
                raise ValueError
            facts = tuple(_decode_fact(cast(object, raw)) for raw in raw_facts)
            if len({fact.loss_id for fact in facts}) != len(facts):
                raise ValueError
            return facts
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise ConfirmedLossError("confirmed-loss ledger is malformed") from error

    def _write_ledger(self, facts: tuple[ConfirmedLossFact, ...]) -> None:
        self._atomic_write(
            self.path,
            json.dumps(
                {"schema_version": _SCHEMA_VERSION, "losses": [fact.as_dict() for fact in facts]},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n",
        )

    def _atomic_write(self, target: Path, payload: bytes) -> None:
        temporary: str | None = None
        try:
            if len(payload) > _MAX_LEDGER_BYTES:
                raise ConfirmedLossError("confirmed-loss ledger reached its configured size limit")
            target.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
            descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
            with os.fdopen(descriptor, "wb") as stream:
                os.fchmod(stream.fileno(), 0o640)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            Path(temporary).replace(target)
            directory = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except ConfirmedLossError:
            raise
        except OSError as error:
            if temporary is not None:
                with suppress(OSError):
                    Path(temporary).unlink()
            raise ConfirmedLossError("confirmed-loss ledger write failed") from error


def read_confirmed_losses(path: Path, *, allow_missing: bool = False) -> tuple[ConfirmedLossFact, ...]:
    """Read only initialized, bounded, strict durable evidence."""
    if not os.path.lexists(path):
        if allow_missing:
            return ()
        raise ConfirmedLossError("confirmed-loss ledger is missing")
    return ConfirmedLossLedger(path).read()


def _read_bounded_regular_file(path: Path, maximum: int) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise ConfirmedLossError("confirmed-loss ledger is missing or unsafe") from error
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ConfirmedLossError("confirmed-loss ledger is unsafe or oversized")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            payload = stream.read(maximum + 1)
    except OSError as error:
        raise ConfirmedLossError("confirmed-loss ledger is unreadable") from error
    finally:
        os.close(descriptor)
    if len(payload) > maximum:
        raise ConfirmedLossError("confirmed-loss ledger is oversized")
    return payload


def _decode_fact(value: object) -> ConfirmedLossFact:
    if not isinstance(value, dict):
        raise ValueError("loss fact must be an object")
    document = cast(dict[str, object], value)
    if set(document) != _FACT_FIELDS:
        raise ValueError("loss fact fields are invalid")
    fact = ConfirmedLossFact(
        loss_id=_string(document, "loss_id"),
        attempt_id=_string(document, "attempt_id"),
        start_sequence=_integer(document, "start_sequence"),
        end_sequence=_integer(document, "end_sequence"),
        occurred_at=_string(document, "occurred_at"),
        missing_record_count=_integer(document, "missing_record_count"),
        missing_raw_bytes=_integer(document, "missing_raw_bytes"),
        reason=_string(document, "reason"),
    )
    _validate_fact(fact)
    return fact


def _validate_fact(fact: ConfirmedLossFact) -> None:
    if _ATTEMPT_ID.fullmatch(fact.attempt_id) is None or _LOSS_ID.fullmatch(fact.loss_id) is None:
        raise ValueError("loss identity is invalid")
    if fact.start_sequence < 0 or fact.end_sequence <= fact.start_sequence or fact.end_sequence > _U64_MAX:
        raise ValueError("loss interval is invalid")
    count = fact.end_sequence - fact.start_sequence
    if fact.missing_record_count != count or fact.missing_raw_bytes != count * RECORD_SIZE:
        raise ValueError("loss interval counts are inconsistent")
    if fact.reason != _REASON or fact.loss_id != _loss_id(fact.attempt_id, fact.start_sequence, fact.end_sequence):
        raise ValueError("loss fact identity or reason is invalid")
    try:
        timestamp = datetime.fromisoformat(fact.occurred_at)
    except ValueError as error:
        raise ValueError("loss timestamp is invalid") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("loss timestamp must include an offset")


def _loss_id(attempt_id: str, start_sequence: int, end_sequence: int) -> str:
    if _ATTEMPT_ID.fullmatch(attempt_id) is None:
        raise ConfirmedLossError("confirmed-loss attempt identity is invalid")
    if isinstance(start_sequence, bool) or not isinstance(start_sequence, int):
        raise ConfirmedLossError("confirmed-loss interval is invalid")
    if isinstance(end_sequence, bool) or not isinstance(end_sequence, int):
        raise ConfirmedLossError("confirmed-loss interval is invalid")
    if start_sequence < 0 or end_sequence <= start_sequence or end_sequence > _U64_MAX:
        raise ConfirmedLossError("confirmed-loss interval is invalid")
    material = f"v1:{attempt_id}:{start_sequence}:{end_sequence}".encode()
    return sha256(material).hexdigest()


def _string(document: dict[str, object], key: str) -> str:
    value = document[key]
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _integer(document: dict[str, object], key: str) -> int:
    value = document[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value
