"""Atomic, invocation-scoped operational status snapshot."""

from __future__ import annotations

import asyncio
import errno
import fcntl
import json
import os
import re
import stat
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import cast

from ..domain.operational_status_machine import (
    OperationalDimension,
    OperationalSignal,
    OperationalState,
    PublicationOutcome,
    transition,
)

_MAX_SNAPSHOT_BYTES = 8_192
_BOOT_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_INVOCATION_ID = re.compile(r"[0-9a-f]{32}\Z")
_DIMENSIONS = tuple(OperationalDimension)
_PUBLICATION_SIGNALS = {
    PublicationOutcome.BLOCKED: OperationalSignal.BLOCK,
    PublicationOutcome.PUBLISHED: OperationalSignal.CLEAR,
    PublicationOutcome.TRANSIENT: OperationalSignal.UNKNOWN,
    PublicationOutcome.WAITING: OperationalSignal.CLEAR,
}


class OperationalStatusError(RuntimeError):
    """The service could not safely read or persist operational status."""


@dataclass(frozen=True, slots=True)
class OperationalIdentity:
    boot_id: str
    invocation_id: str | None

    def __post_init__(self) -> None:
        if _BOOT_ID.fullmatch(self.boot_id) is None:
            raise OperationalStatusError("operational status boot identity is invalid")
        if self.invocation_id is not None and _INVOCATION_ID.fullmatch(self.invocation_id) is None:
            raise OperationalStatusError("operational status invocation identity is invalid")


class OperationalStatusStore:
    """Thread-safe service writer for one bounded, atomic status snapshot."""

    def __init__(self, path: Path, identity: OperationalIdentity) -> None:
        self.path = Path(path)
        self.identity = identity
        self._lock = Lock()
        self._lease_fd = _acquire_writer_lease(self.path.with_name(f"{self.path.name}.lock"))
        self._closed = False
        self._states: dict[OperationalDimension, OperationalState] = dict.fromkeys(
            _DIMENSIONS, OperationalState.UNKNOWN
        )
        self._failure: OperationalStatusError | None = None
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[OperationalStatusError]]] = []

    def initialize(self) -> None:
        """Load only a safe prior snapshot, then publish this invocation's state."""
        with self._lock:
            if self._closed:
                raise OperationalStatusError("operational status writer lease is closed")
            previous = _read_snapshot(self.path)
            if previous is not None:
                prior_identity, prior_states = previous
                if prior_identity == self.identity:
                    self._states = prior_states
                    return
                self._states = {
                    name: state if state is OperationalState.BLOCKED else OperationalState.UNKNOWN
                    for name, state in prior_states.items()
                }
            self._persist()

    def update(self, dimension: OperationalDimension, signal: OperationalSignal) -> None:
        with self._lock:
            if self._closed or self._failure is not None:
                return
            previous = self._states[dimension]
            try:
                state = transition(self._states[dimension], signal)
                if state is self._states[dimension]:
                    return
                self._states[dimension] = state
                self._persist()
            except OperationalStatusError as error:
                self._states[dimension] = previous
                self._latch_failure(error)
                return

    def record_publication_outcome(self, outcome: PublicationOutcome) -> None:
        if _PUBLICATION_SIGNALS.keys() != set(PublicationOutcome):
            raise AssertionError("publication outcome table is incomplete")
        self.update(OperationalDimension.QUALITY, OperationalSignal.UNCHANGED)
        self.update(OperationalDimension.CLOCK, OperationalSignal.UNCHANGED)
        signal = _PUBLICATION_SIGNALS[outcome]
        if outcome is PublicationOutcome.TRANSIENT:
            signal = (
                OperationalSignal.BLOCK
                if self.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.BLOCKED.value
                else signal
            )
        self.update(OperationalDimension.PUBLICATION, signal)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            fcntl.flock(self._lease_fd, fcntl.LOCK_UN)
            os.close(self._lease_fd)

    def __enter__(self) -> OperationalStatusStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def failure(self) -> OperationalStatusError | None:
        with self._lock:
            return self._failure

    async def wait_failure(self) -> OperationalStatusError:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[OperationalStatusError] = loop.create_future()
        with self._lock:
            if self._failure is not None:
                future.set_result(self._failure)
            else:
                self._waiters.append((loop, future))
        try:
            return await future
        finally:
            with self._lock:
                self._waiters = [
                    (waiting_loop, waiter) for waiting_loop, waiter in self._waiters if waiter is not future
                ]

    def as_dict(self) -> dict[str, str]:
        with self._lock:
            return {name.value: state.value for name, state in self._states.items()}

    def _latch_failure(self, error: OperationalStatusError) -> None:
        self._failure = error
        waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_set_failure, future, error)

    def _persist(self) -> None:
        document = {
            "boot_id": self.identity.boot_id,
            "invocation_id": self.identity.invocation_id,
            "schema_version": 1,
            "states": {name.value: state.value for name, state in self._states.items()},
        }
        _write_snapshot(self.path, json.dumps(document, sort_keys=True, separators=(",", ":")).encode() + b"\n")


def read_operational_status(path: Path, identity: OperationalIdentity) -> dict[str, str]:
    """Read a bounded snapshot, degrading stale clear evidence to unknown."""
    previous = _read_snapshot(path)
    if previous is None:
        return {name.value: OperationalState.UNKNOWN.value for name in _DIMENSIONS}
    prior_identity, states = previous
    if prior_identity == identity:
        return {name.value: state.value for name, state in states.items()}
    return {
        name.value: (state if state is OperationalState.BLOCKED else OperationalState.UNKNOWN).value
        for name, state in states.items()
    }


def _read_snapshot(
    path: Path,
) -> tuple[OperationalIdentity, dict[OperationalDimension, OperationalState]] | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return None
    except OSError as error:
        raise OperationalStatusError("operational status snapshot is unsafe or unreadable") from error
    try:
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_SNAPSHOT_BYTES:
                raise OperationalStatusError("operational status snapshot is unsafe or oversized")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(_MAX_SNAPSHOT_BYTES + 1)
        except OSError as error:
            raise OperationalStatusError("operational status snapshot is unsafe or unreadable") from error
    finally:
        os.close(descriptor)
    if len(payload) > _MAX_SNAPSHOT_BYTES:
        raise OperationalStatusError("operational status snapshot is oversized")
    try:
        document = _decode_snapshot_document(payload)
        identity = _decode_snapshot_identity(document)
        states = _decode_snapshot_states(document["states"])
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise OperationalStatusError("operational status snapshot is malformed") from error
    return identity, states


def _decode_snapshot_document(payload: bytes) -> dict[str, object]:
    decoded = cast(object, json.loads(payload))
    if not isinstance(decoded, dict):
        raise ValueError("snapshot must be an object")
    document = cast(dict[str, object], decoded)
    _validate_snapshot_shape(document)
    _validate_snapshot_identity(document)
    _validate_snapshot_state_keys(document)
    return document


def _validate_snapshot_shape(document: dict[str, object]) -> None:
    expected_keys = {"boot_id", "invocation_id", "schema_version", "states"}
    if set(document) != expected_keys:
        raise ValueError("snapshot fields are invalid")
    if type(document["schema_version"]) is not int:
        raise ValueError("snapshot schema version must be an integer")
    if document["schema_version"] != 1:
        raise ValueError("snapshot schema version is unsupported")


def _validate_snapshot_identity(document: dict[str, object]) -> None:
    boot_id = document["boot_id"]
    if not isinstance(boot_id, str) or _BOOT_ID.fullmatch(boot_id) is None:
        raise ValueError("snapshot boot identity is invalid")
    invocation_id = document["invocation_id"]
    if invocation_id is None:
        return
    if not isinstance(invocation_id, str) or _INVOCATION_ID.fullmatch(invocation_id) is None:
        raise ValueError("snapshot invocation identity is invalid")


def _validate_snapshot_state_keys(document: dict[str, object]) -> None:
    states = document["states"]
    if not isinstance(states, dict):
        raise ValueError("snapshot states must be an object")
    if set(states) != {name.value for name in _DIMENSIONS}:
        raise ValueError("snapshot state dimensions are invalid")


def _decode_snapshot_states(document: object) -> dict[OperationalDimension, OperationalState]:
    if not isinstance(document, dict):
        raise ValueError("snapshot states must be an object")
    decoded: dict[OperationalDimension, OperationalState] = {}
    for dimension in _DIMENSIONS:
        value = document[dimension.value]
        if not isinstance(value, str):
            raise ValueError("snapshot state must be a string")
        decoded[dimension] = OperationalState(value)
    return decoded


def _decode_snapshot_identity(document: dict[str, object]) -> OperationalIdentity:
    boot_id = document["boot_id"]
    invocation_id = document["invocation_id"]
    assert isinstance(boot_id, str)
    assert invocation_id is None or isinstance(invocation_id, str)
    return OperationalIdentity(boot_id, invocation_id)


def _write_snapshot(path: Path, payload: bytes) -> None:
    temporary: str | None = None
    try:
        path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o640)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).replace(path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as error:
        if temporary is not None:
            with suppress(OSError):
                Path(temporary).unlink()
        raise OperationalStatusError("operational status snapshot write failed") from error


def _acquire_writer_lease(path: Path) -> int:
    try:
        path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o640,
        )
    except OSError as error:
        raise OperationalStatusError("operational status writer lease is unsafe or unavailable") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OperationalStatusError("operational status writer lease is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        os.close(descriptor)
        if error.errno in {errno.EACCES, errno.EAGAIN}:
            raise OperationalStatusError("operational status writer is already active") from error
        raise OperationalStatusError("operational status writer lease is unsafe or unavailable") from error
    except OperationalStatusError:
        os.close(descriptor)
        raise
    return descriptor


def _set_failure(future: asyncio.Future[OperationalStatusError], error: OperationalStatusError) -> None:
    if not future.done():
        future.set_result(error)
