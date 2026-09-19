"""Crash-safe clock corrections and timestamp normalization."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TextIO, cast
from uuid import uuid4

from .clock_observations import ClockObservationStore

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows is not a supported runtime.
    fcntl = None  # type: ignore[assignment]

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
        self._observations = ClockObservationStore(device_state_path)

    def append(self, **values: object) -> object:
        """Expose the evidence sink beside correction operations."""
        append = cast(Callable[..., object], self._observations.append)
        return append(**values)

    @property
    def observation_store(self) -> ClockObservationStore:
        return self._observations

    def prepare(
        self,
        observed_epoch: int,
        target_epoch: int,
        drift_seconds: float,
        boundary_sequence_min: int,
        operation_id: str | None = None,
    ) -> ClockCorrection:
        if not math.isfinite(drift_seconds) or boundary_sequence_min < 0:
            raise ClockCorrectionError("clock correction intent values are invalid")
        if operation_id is not None:
            existing_path = self._root / f"{operation_id}.json"
            if existing_path.exists():
                existing = self._read(existing_path)
                expected = ClockCorrection(
                    2,
                    operation_id,
                    "prepared",
                    observed_epoch,
                    target_epoch,
                    drift_seconds,
                    boundary_sequence_min,
                )
                if existing != expected:
                    raise ClockCorrectionError("clock correction identity conflicts with durable state")
                return existing
        if self._has_active_attempt():
            raise ClockCorrectionError("clock correction waits for the active audio attempt")
        if self._has_pending_correction():
            raise ClockCorrectionError("clock correction waits for a pending correction")
        correction = ClockCorrection(
            2,
            operation_id or uuid4().hex,
            "prepared",
            observed_epoch,
            target_epoch,
            drift_seconds,
            boundary_sequence_min,
        )
        path = self._path(correction)
        self._ensure_directory(path.parent)
        return self._write_new(path, correction)

    def finish(
        self,
        correction: ClockCorrection,
        *,
        state: str,
        boundary_sequence_max: int | None,
        verified_epoch: int | None,
    ) -> ClockCorrection:
        current = self._read(self._path(correction))
        if (
            current.state == state
            and current.boundary_sequence_max == boundary_sequence_max
            and current.verified_epoch == verified_epoch
        ):
            return current
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

    def reconcile_observation(  # noqa: PLR0913 - explicit causal reconciliation inputs
        self,
        observed_epoch: int,
        drift_seconds: float,
        boundary_sequence_max: int,
        *,
        near_zero_threshold: float,
        effective_boundary_sequence: int | None = None,
        observation_id: str | None = None,
        operation_id: str | None = None,
    ) -> tuple[ClockCorrection, ...]:
        """Reconcile unresolved writes from a causal clock observation.

        ``boundary_sequence_max`` is the observation frontier.  When historical
        evidence identifies the exact transition, ``effective_boundary_sequence``
        is persisted instead; a late INFO observation must not expand the raw
        ambiguity interval.
        """
        _validate_reconcile_inputs(
            boundary_sequence_max,
            near_zero_threshold,
            effective_boundary_sequence,
        )
        referenced_operation = self._validate_observation_reference(
            observation_id, observed_epoch, boundary_sequence_max, operation_id
        )
        if operation_id is None:
            operation_id = referenced_operation
        if operation_id is not None and observation_id is None:
            return ()
        if observation_id is None and self._observations.records():
            return ()
        records = self.records()
        current = _pending_correction(records, boundary_sequence_max, operation_id)
        if current is None:
            return ()
        if _confirmed_boundary_conflicts(records, current, boundary_sequence_max, operation_id):
            return ()
        state, verified_epoch = _reconciliation_state(current, observed_epoch, drift_seconds, near_zero_threshold)
        if state is None:
            return ()
        completed = replace(
            current,
            state=state,
            boundary_sequence_max=(
                effective_boundary_sequence if effective_boundary_sequence is not None else boundary_sequence_max
            ),
            verified_epoch=verified_epoch,
        )
        self._write_atomic(self._path(current), completed)
        return (completed,)

    def _validate_observation_reference(
        self, observation_id: str | None, observed_epoch: int, boundary_sequence_max: int, operation_id: str | None
    ) -> str | None:
        if observation_id is None:
            return None
        evidence = next((item for item in self._observations.records() if item.observation_id == observation_id), None)
        if evidence is None:
            raise ClockCorrectionError("clock observation reference is not durable")
        if evidence.device_epoch != observed_epoch or evidence.info_sequence_max != boundary_sequence_max:
            raise ClockCorrectionError("clock observation reference conflicts with supplied evidence")
        if operation_id is not None and evidence.operation_id != operation_id:
            raise ClockCorrectionError("clock observation operation reference conflicts")
        return evidence.operation_id

    def reconcile_causal_observation(
        self,
        observation: object,
        *,
        near_zero_threshold: float,
    ) -> tuple[ClockCorrection, ...]:
        """Apply one typed observation, preserving its causal boundary."""
        records = self._observations.records()
        observed_epoch, boundary, effective, operation_id, observation_id, drift = _causal_inputs(observation, records)
        return self.reconcile_observation(
            observed_epoch,
            drift,
            boundary,
            near_zero_threshold=near_zero_threshold,
            effective_boundary_sequence=effective,
            observation_id=observation_id,
            operation_id=operation_id,
        )

    def replay_observations(self, *, near_zero_threshold: float) -> tuple[ClockCorrection, ...]:
        """Replay durable later observations after a process restart."""
        if near_zero_threshold <= 0:
            raise ValueError("near-zero threshold must be positive")
        changed: list[ClockCorrection] = []
        for correction in self.records():
            if correction.state != "unresolved":
                continue
            observations = self._observations.for_operation(correction.operation_id)
            observation = next(
                (
                    item
                    for item in reversed(observations)
                    if item.observation_role == "later" and _replayable_observation(item, observations)
                ),
                None,
            )
            if observation is not None:
                changed.extend(self.reconcile_causal_observation(observation, near_zero_threshold=near_zero_threshold))
        return tuple(changed)

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
        current = self._read(self._path(correction))
        if current.state == "unresolved":
            return current
        if current != correction or correction.state != "prepared":
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

    def _write_new(self, path: Path, correction: ClockCorrection) -> ClockCorrection:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        with _file_lock(path.parent / ".lock"):
            if path.exists():
                current = self._read(path)
                if current != correction:
                    raise ClockCorrectionError("clock correction identity conflicts with durable state")
                return current
            try:
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(self._payload(correction))
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(path)
                self._sync_directory(path.parent)
            except OSError as error:
                temporary.unlink(missing_ok=True)
                raise ClockCorrectionError("clock correction intent is not durable") from error
        return correction

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
        with _file_lock(path.parent / ".lock"):
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


def _validate_reconcile_inputs(
    boundary_sequence_max: int,
    near_zero_threshold: float,
    effective_boundary_sequence: int | None,
) -> None:
    if near_zero_threshold <= 0:
        raise ValueError("near-zero threshold must be positive")
    if boundary_sequence_max < 0:
        raise ValueError("clock observation boundary must be non-negative")
    if effective_boundary_sequence is not None and not 0 <= effective_boundary_sequence <= boundary_sequence_max:
        raise ValueError("effective correction boundary must be within observation frontier")


def _pending_correction(
    records: tuple[ClockCorrection, ...], boundary_sequence_max: int, operation_id: str | None = None
) -> ClockCorrection | None:
    pending = tuple(
        correction
        for correction in records
        if correction.state == "unresolved"
        and boundary_sequence_max >= correction.boundary_sequence_min
        and (operation_id is None or correction.operation_id == operation_id)
    )
    return pending[0] if len(pending) == 1 else None


def _causal_inputs(observation: object, records: tuple[object, ...]) -> tuple[int, int, int | None, str, str, float]:
    if getattr(observation, "evidence_kind", None) not in {"native_trusted", "anchored_monotonic"}:
        raise ClockCorrectionError("clock observation evidence is not trusted")
    observed_epoch = getattr(observation, "device_epoch", None)
    boundary = getattr(observation, "info_sequence_max", None)
    effective = getattr(observation, "effective_boundary_sequence", None)
    if isinstance(observed_epoch, bool) or not isinstance(observed_epoch, int):
        raise ClockCorrectionError("clock observation epoch is invalid")
    if isinstance(boundary, bool) or not isinstance(boundary, int):
        raise ClockCorrectionError("clock observation boundary is invalid")
    operation_id = getattr(observation, "operation_id", None)
    observation_id = getattr(observation, "observation_id", None)
    if not isinstance(operation_id, str) or not operation_id:
        raise ClockCorrectionError("clock observation has no causal operation reference")
    if not isinstance(observation_id, str) or not observation_id:
        raise ClockCorrectionError("clock observation has no durable reference")
    stored = next((item for item in records if getattr(item, "observation_id", None) == observation_id), None)
    if stored is None:
        raise ClockCorrectionError("clock observation is not durable")
    _validate_causal_order(stored, operation_id, records)
    start = getattr(observation, "host_realtime_start", None)
    end = getattr(observation, "host_realtime_end", None)
    if not isinstance(start, int | float) or not isinstance(end, int | float):
        raise ClockCorrectionError("clock observation host interval is invalid")
    return (
        observed_epoch,
        boundary,
        effective,
        operation_id,
        observation_id,
        float(observed_epoch) - ((float(start) + float(end)) / 2.0),
    )


def _replayable_observation(observation: object, records: tuple[object, ...]) -> bool:
    parent_id = getattr(observation, "parent_observation_id", None)
    if parent_id is None:
        return False
    parent = next((item for item in records if getattr(item, "observation_id", None) == parent_id), None)
    return bool(
        parent is not None
        and getattr(parent, "operation_id", None) in {None, getattr(observation, "operation_id", None)}
        and getattr(parent, "observation_role", None) in {"initial", "standalone"}
        and getattr(parent, "causal_order", -1) < getattr(observation, "causal_order", -1)
    )


def _validate_causal_order(stored: object, operation_id: str, records: tuple[object, ...]) -> None:
    if getattr(stored, "observation_role", None) != "later":
        raise ClockCorrectionError("clock observation is not a later causal observation")
    parent_id = getattr(stored, "parent_observation_id", None)
    if parent_id is None:
        raise ClockCorrectionError("clock observation has no initial causal parent")
    parent = next((item for item in records if getattr(item, "observation_id", None) == parent_id), None)
    if parent is None:
        raise ClockCorrectionError("clock observation causal parent is not durable")
    if getattr(parent, "operation_id", None) not in {None, operation_id}:
        raise ClockCorrectionError("clock observation causal operation conflicts")
    if getattr(parent, "observation_role", None) not in {"initial", "standalone"}:
        raise ClockCorrectionError("clock observation causal parent role is invalid")
    if getattr(parent, "causal_order", -1) >= getattr(stored, "causal_order", -1):
        raise ClockCorrectionError("clock observation causal order is invalid")


def _confirmed_boundary_conflicts(
    records: tuple[ClockCorrection, ...],
    current: ClockCorrection,
    boundary_sequence_max: int,
    operation_id: str | None,
) -> bool:
    for correction in records:
        if correction.state not in {"applied", "resolved"}:
            continue
        if not current.boundary_sequence_min <= correction.boundary_sequence_min <= boundary_sequence_max:
            continue
        if operation_id is None or correction.operation_id == operation_id:
            return True
    return any(
        correction.state == "unresolved"
        and correction.operation_id != current.operation_id
        and current.boundary_sequence_min <= correction.boundary_sequence_min <= boundary_sequence_max
        for correction in records
    )


def _reconciliation_state(
    correction: ClockCorrection,
    observed_epoch: int,
    drift_seconds: float,
    near_zero_threshold: float,
) -> tuple[str | None, int | None]:
    if abs(drift_seconds) <= near_zero_threshold:
        return "applied", observed_epoch
    if math.isclose(drift_seconds, correction.drift_seconds, rel_tol=0.0, abs_tol=near_zero_threshold):
        return "not_applied", None
    return None, None


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


class _file_lock:
    """Advisory lock for one correction transition."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._stream: TextIO | None = None

    def __enter__(self) -> _file_lock:
        if fcntl is None:
            return self
        stream = self._path.open("a+", encoding="ascii")
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        self._stream = stream
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self._stream is not None and fcntl is not None:
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            self._stream.close()
