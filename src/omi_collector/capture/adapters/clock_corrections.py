"""Crash-safe clock corrections and timestamp normalization."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import cast
from uuid import uuid4

_SCHEMA_VERSION = 2


class ClockCorrectionError(RuntimeError):
    """A clock operation could not be persisted or applied safely."""


@dataclass(frozen=True, slots=True)
class ClockCorrection:
    version: int
    operation_id: str
    state: str
    observed_epoch: int
    target_epoch: int
    drift_seconds: float
    boundary_sequence_min: int
    boundary_sequence_max: int | None = None
    verified_epoch: int | None = None


class ClockCorrectionStore:
    """Persist intent before touching the pendant and completion afterward."""

    def __init__(self, device_state_path: Path, attempts_root: Path | None = None) -> None:
        self._root = device_state_path.parent / "clock-corrections"
        self._pending_attempts = attempts_root or device_state_path.parent / "attempts"

    def prepare(
        self,
        observed_epoch: int,
        target_epoch: int,
        drift_seconds: float,
        boundary_sequence_min: int,
    ) -> ClockCorrection:
        if self._has_active_attempt():
            raise ClockCorrectionError("clock correction waits for the active audio attempt")
        correction = ClockCorrection(
            2,
            uuid4().hex,
            "prepared",
            observed_epoch,
            target_epoch,
            drift_seconds,
            boundary_sequence_min,
        )
        path = self._path(correction)
        self._ensure_directory(path.parent)
        self._write_new(path, correction)
        return correction

    def finish(
        self,
        correction: ClockCorrection,
        *,
        state: str,
        boundary_sequence_max: int | None,
        verified_epoch: int | None,
    ) -> ClockCorrection:
        if state not in {"not_written", "unresolved", "not_applied", "applied", "resolved"}:
            raise ValueError("invalid clock correction state")
        current = self._read(self._path(correction))
        if current != correction or correction.state not in {"prepared", "unresolved", "applied"}:
            raise ClockCorrectionError("clock correction transition conflicts with durable state")
        allowed = {
            "prepared": {"not_written", "unresolved"},
            "unresolved": {"unresolved", "not_applied", "applied"},
            "applied": {"resolved"},
        }
        if state not in allowed[correction.state]:
            raise ClockCorrectionError("clock correction state transition is invalid")
        completed = replace(
            correction,
            state=state,
            boundary_sequence_max=boundary_sequence_max,
            verified_epoch=verified_epoch,
        )
        self._write_atomic(self._path(correction), completed)
        return completed

    def mark_unresolved(self, correction: ClockCorrection) -> ClockCorrection:
        """Persist ambiguity before the BLE write can possibly take effect."""
        if correction.state != "prepared":
            raise ClockCorrectionError("clock correction intent is not prepared")
        unresolved = replace(correction, state="unresolved")
        self._write_atomic(self._path(correction), unresolved)
        return unresolved

    def confirmed(self) -> tuple[ClockCorrection, ...]:
        if not self._root.exists():
            return ()
        corrections = tuple(
            correction
            for path in sorted(self._root.glob("*.json"))
            if (correction := self._read(path)).state in {"applied", "resolved"}
        )
        return tuple(sorted(corrections, key=lambda item: item.boundary_sequence_min))

    def _path(self, correction: ClockCorrection) -> Path:
        return self._root / f"{correction.operation_id}.json"

    def _attempts_root(self) -> Path:
        return self._pending_attempts

    def _has_active_attempt(self) -> bool:
        root = self._attempts_root()
        if not root.exists():
            return False
        return any(path.is_dir() and not (path / "terminal-retired.json").exists() for path in root.iterdir())

    @staticmethod
    def _payload(correction: ClockCorrection) -> bytes:
        return (json.dumps(asdict(correction), sort_keys=True, separators=(",", ":")) + "\n").encode()

    def _write_new(self, path: Path, correction: ClockCorrection) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(self._payload(correction))
                stream.flush()
                os.fsync(stream.fileno())
            if path.exists():
                raise FileExistsError(path)
            temporary.replace(path)
            self._sync_directory(path.parent)
        except OSError as error:
            temporary.unlink(missing_ok=True)
            raise ClockCorrectionError("clock correction intent is not durable") from error

    def _ensure_directory(self, path: Path) -> None:
        missing: list[Path] = []
        current = path
        while not current.exists():
            missing.append(current)
            current = current.parent
        try:
            for directory in reversed(missing):
                directory.mkdir(mode=0o750)
                self._sync_directory(directory.parent)
        except OSError as error:
            raise ClockCorrectionError("clock correction directory is not durable") from error

    def _write_atomic(self, path: Path, correction: ClockCorrection) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(self._payload(correction))
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            self._sync_directory(path.parent)
        except OSError as error:
            temporary.unlink(missing_ok=True)
            raise ClockCorrectionError("clock correction result is not durable") from error

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _read(path: Path) -> ClockCorrection:
        try:
            value = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
            if (
                set(value)
                != {
                    "version",
                    "operation_id",
                    "state",
                    "observed_epoch",
                    "target_epoch",
                    "drift_seconds",
                    "boundary_sequence_min",
                    "boundary_sequence_max",
                    "verified_epoch",
                }
                or value.get("version") != _SCHEMA_VERSION
            ):
                raise ValueError("clock correction schema is invalid")
            boundary_max = value.get("boundary_sequence_max")
            verified_epoch = value.get("verified_epoch")
            return ClockCorrection(
                _integer(value, "version"),
                _text(value, "operation_id"),
                _text(value, "state"),
                _integer(value, "observed_epoch"),
                _integer(value, "target_epoch"),
                _number(value, "drift_seconds"),
                _integer(value, "boundary_sequence_min"),
                None if boundary_max is None else _integer(value, "boundary_sequence_max"),
                None if verified_epoch is None else _integer(value, "verified_epoch"),
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ClockCorrectionError("clock correction ledger is invalid") from error


def _integer(value: dict[str, object], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"{key} is not an integer")
    return item


def _text(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is not text")
    return item


def _number(value: dict[str, object], key: str) -> float:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int | float):
        raise ValueError(f"{key} is not numeric")
    return float(item)
