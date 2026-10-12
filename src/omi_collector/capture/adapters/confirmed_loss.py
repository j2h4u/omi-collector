"""Bounded durable ledger for confirmed device-cursor loss intervals."""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

from ..domain.ring_protocol import RECORD_SIZE
from .collector_state_db import StateDB, StateDBError, StateKind, StateTransaction
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

    def __init__(self, db: StateDB) -> None:
        self.db = db

    def initialize(self, *, allow_create: bool) -> None:
        """Create an empty first-use ledger, never recreating one after status was initialized."""
        try:
            with self.db.transaction() as transaction:
                document = transaction.get(StateKind.CONFIRMED_LOSS, "ledger")
                if document is not None:
                    self._decode_ledger(document)
                    return
                if not allow_create:
                    raise ConfirmedLossError("confirmed-loss ledger is missing after status initialization")
                self._write_ledger((), transaction)
        except StateDBError as error:
            raise ConfirmedLossError("confirmed-loss database initialization failed") from error

    def read(self) -> tuple[ConfirmedLossFact, ...]:
        try:
            return self._read_ledger()
        except StateDBError as error:
            raise ConfirmedLossError("confirmed-loss database read failed") from error

    def record(self, attempt_id: str, start_sequence: int, end_sequence: int, occurred_at: str) -> str:
        try:
            with self.db.transaction() as transaction:
                facts = self._read_ledger(transaction)
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
                self._write_ledger((*facts, fact), transaction)
                return loss_id
        except StateDBError as error:
            raise ConfirmedLossError("confirmed-loss database write failed") from error

    def _read_ledger(self, transaction: StateTransaction | None = None) -> tuple[ConfirmedLossFact, ...]:
        document = (
            transaction.get(StateKind.CONFIRMED_LOSS, "ledger")
            if transaction is not None
            else self.db.get(StateKind.CONFIRMED_LOSS, "ledger")
        )
        if document is None:
            raise ConfirmedLossError("confirmed-loss ledger is missing")
        return self._decode_ledger(document)

    @staticmethod
    def _decode_ledger(ledger: dict[str, object]) -> tuple[ConfirmedLossFact, ...]:
        try:
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

    def _write_ledger(self, facts: tuple[ConfirmedLossFact, ...], transaction: StateTransaction) -> None:
        document = {"schema_version": _SCHEMA_VERSION, "losses": [fact.as_dict() for fact in facts]}
        if len(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()) > _MAX_LEDGER_BYTES:
            raise ConfirmedLossError("confirmed-loss ledger reached its configured size limit")
        transaction.put(StateKind.CONFIRMED_LOSS, "ledger", document)


def read_confirmed_losses(db: StateDB, *, allow_missing: bool = False) -> tuple[ConfirmedLossFact, ...]:
    """Read only initialized, bounded, strict durable evidence."""
    try:
        if db.get(StateKind.CONFIRMED_LOSS, "ledger") is None:
            if allow_missing:
                return ()
            raise ConfirmedLossError("confirmed-loss ledger is missing")
        return ConfirmedLossLedger(db).read()
    except StateDBError as error:
        raise ConfirmedLossError("confirmed-loss database read failed") from error


def parse_legacy_confirmed_loss(path: Path) -> tuple[ConfirmedLossFact, ...]:
    """Strictly decode the old bounded ledger for the offline state importer."""
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_LEDGER_BYTES:
                raise ValueError("legacy loss ledger is unsafe or oversized")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(_MAX_LEDGER_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(payload) > _MAX_LEDGER_BYTES:
            raise ValueError("legacy loss ledger is oversized")
        document = cast(object, json.loads(payload))
        if not isinstance(document, dict):
            raise ValueError("legacy ledger must be an object")
        return ConfirmedLossLedger._decode_ledger(cast(dict[str, object], document))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise ConfirmedLossError("legacy confirmed-loss ledger is malformed or unsafe") from error


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
