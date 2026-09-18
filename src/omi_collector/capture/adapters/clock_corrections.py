"""Crash-safe clock corrections and timestamp normalization."""

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import cast
from uuid import uuid4

_SCHEMA_VERSION = 2
_VALID_STATES = frozenset({"prepared", "not_written", "unresolved", "not_applied", "applied", "resolved"})
_VALID_FINISH_STATES = frozenset({"not_written", "unresolved", "not_applied", "applied", "resolved"})
_ALLOWED_TRANSITIONS = {
    "prepared": frozenset({"not_written", "unresolved"}),
    "unresolved": frozenset({"unresolved", "not_applied", "applied", "resolved"}),
    "applied": frozenset({"resolved"}),
}


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
        if self._has_pending_correction():
            raise ClockCorrectionError("clock correction waits for a pending correction")
        if not math.isfinite(drift_seconds) or boundary_sequence_min < 0:
            raise ClockCorrectionError("clock correction intent values are invalid")
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
        current = self._read(self._path(correction))
        if current != correction or correction.state not in {"prepared", "unresolved", "applied"}:
            raise ClockCorrectionError("clock correction transition conflicts with durable state")
        _validate_finish_transition(correction, state, boundary_sequence_max, verified_epoch)
        completed = replace(
            correction,
            state=state,
            boundary_sequence_max=boundary_sequence_max,
            verified_epoch=verified_epoch,
        )
        self._write_atomic(self._path(correction), completed)
        return completed

    def resolve_applied(self, correction: ClockCorrection) -> ClockCorrection:
        """Resolve an applied operation after its raw ambiguity interval is checked."""
        if correction.state != "applied":
            raise ClockCorrectionError("clock correction is not applied")
        return self.finish(
            correction,
            state="resolved",
            boundary_sequence_max=correction.boundary_sequence_max,
            verified_epoch=correction.verified_epoch,
        )

    def reconcile_observation(
        self,
        observed_epoch: int,
        drift_seconds: float,
        boundary_sequence_max: int,
        *,
        near_zero_threshold: float,
    ) -> tuple[ClockCorrection, ...]:
        """Reconcile unresolved writes from a later numeric clock observation."""
        if near_zero_threshold <= 0:
            raise ValueError("near-zero threshold must be positive")
        if boundary_sequence_max < 0:
            raise ValueError("clock observation boundary must be non-negative")
        records = self.records()
        pending = tuple(
            correction
            for correction in records
            if correction.state == "unresolved" and boundary_sequence_max >= correction.boundary_sequence_min
        )
        if len(pending) != 1:
            return ()
        current = pending[0]
        if any(
            correction.state in {"applied", "resolved"}
            and current.boundary_sequence_min <= correction.boundary_sequence_min <= boundary_sequence_max
            for correction in records
        ):
            return ()
        if abs(drift_seconds) <= near_zero_threshold:
            state = "applied"
            verified_epoch = observed_epoch
        elif math.isclose(drift_seconds, current.drift_seconds, rel_tol=0.0, abs_tol=near_zero_threshold):
            state = "not_applied"
            verified_epoch = None
        else:
            return ()
        completed = replace(
            current,
            state=state,
            boundary_sequence_max=boundary_sequence_max,
            verified_epoch=verified_epoch,
        )
        self._write_atomic(self._path(current), completed)
        return (completed,)

    def recover_prepared(self) -> tuple[ClockCorrection, ...]:
        """Atomically settle intents that crashed before ambiguity was opened."""
        recovered: list[ClockCorrection] = []
        for correction in self.records():
            if correction.state != "prepared":
                continue
            recovered.append(
                self.finish(
                    correction,
                    state="not_written",
                    boundary_sequence_max=None,
                    verified_epoch=None,
                )
            )
        return tuple(recovered)

    def records(self) -> tuple[ClockCorrection, ...]:
        """Read every durable clock operation in stable path order."""
        if not self._root.exists():
            return ()
        return tuple(self._read(path) for path in sorted(self._root.glob("*.json")))

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

    def _has_pending_correction(self) -> bool:
        return any(correction.state in {"prepared", "unresolved", "applied"} for correction in self.records())

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
            correction = ClockCorrection(
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
            if correction.boundary_sequence_min < 0 or (
                correction.boundary_sequence_max is not None
                and correction.boundary_sequence_max < correction.boundary_sequence_min
            ):
                raise ValueError("clock correction boundary is invalid")
            if correction.state not in _VALID_STATES:
                raise ValueError("clock correction state is invalid")
            if correction.state in {"prepared", "not_written", "unresolved"} and (
                correction.boundary_sequence_max is not None or correction.verified_epoch is not None
            ):
                raise ValueError("clock correction state fields are invalid")
            if correction.state == "not_applied" and correction.verified_epoch is not None:
                raise ValueError("clock correction state fields are invalid")
            if correction.state in {"applied", "resolved"} and (
                correction.boundary_sequence_max is None or correction.verified_epoch is None
            ):
                raise ValueError("confirmed clock correction has no verified evidence")
            return correction
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ClockCorrectionError("clock correction ledger is invalid") from error


def _validate_finish_transition(
    correction: ClockCorrection,
    state: str,
    boundary_sequence_max: int | None,
    verified_epoch: int | None,
) -> None:
    if state not in _VALID_FINISH_STATES:
        raise ValueError("invalid clock correction state")
    _validate_finish_values(correction, boundary_sequence_max)
    _validate_finish_state(correction, state)
    _validate_finish_fields(state, boundary_sequence_max, verified_epoch)
    _validate_resolved_boundary(correction, state, boundary_sequence_max)


def _validate_finish_values(correction: ClockCorrection, boundary_sequence_max: int | None) -> None:
    if not math.isfinite(correction.drift_seconds) or correction.boundary_sequence_min < 0:
        raise ClockCorrectionError("clock correction values are invalid")
    if boundary_sequence_max is not None and boundary_sequence_max < correction.boundary_sequence_min:
        raise ClockCorrectionError("clock correction boundary is invalid")


def _validate_finish_state(correction: ClockCorrection, state: str) -> None:
    if state not in _ALLOWED_TRANSITIONS[correction.state]:
        raise ClockCorrectionError("clock correction state transition is invalid")


def _validate_finish_fields(
    state: str,
    boundary_sequence_max: int | None,
    verified_epoch: int | None,
) -> None:
    if state in {"prepared", "not_written", "unresolved"} and (
        boundary_sequence_max is not None or verified_epoch is not None
    ):
        raise ClockCorrectionError("clock correction state fields are invalid")
    if state == "not_applied" and verified_epoch is not None:
        raise ClockCorrectionError("clock correction state fields are invalid")
    if state in {"applied", "resolved"} and (boundary_sequence_max is None or verified_epoch is None):
        raise ClockCorrectionError("confirmed clock correction has no verified evidence")


def _validate_resolved_boundary(
    correction: ClockCorrection,
    state: str,
    boundary_sequence_max: int | None,
) -> None:
    if (
        state == "resolved"
        and correction.state == "unresolved"
        and boundary_sequence_max != correction.boundary_sequence_min
    ):
        raise ClockCorrectionError("resolved clock correction has an ambiguous boundary")


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
    number = float(item)
    if not math.isfinite(number):
        raise ValueError(f"{key} is not finite")
    return number
