"""Runtime health tracking for passive BLE scan activity."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
from typing import Literal, Protocol

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ScanRadioActivity:
    """One scanner callback, regardless of whether it matches the pendant."""

    observed_at: float


@dataclass(frozen=True, slots=True)
class ScanIncidentContext:
    """Bounded in-memory facts captured when scanner health fails."""

    generation: int
    reason: Literal["scan_silence", "scan_start_failed", "scan_stop_failed"]
    active_seconds: float
    bleak_callbacks: int
    last_callback_age_seconds: float | None
    threshold_seconds: float
    error_kind: str | None = None


class ScanDiagnostics(Protocol):
    """Passive diagnostic capture controlled by scanner lifecycle."""

    async def prepare(self, generation: int) -> None: ...

    async def close(self, generation: int) -> None: ...

    async def snapshot(self, generation: int) -> dict[str, object]: ...


class ScanIncidentSink(Protocol):
    """Persist scanner health incidents and resolve them after recovery."""

    async def active(self) -> bool: ...

    async def open(self, context: ScanIncidentContext) -> None: ...

    async def resolve(self, generation: int, reason: str) -> None: ...


class ScanHealthMonitor:
    """Detect sustained all-device scan silence without restarting BLE."""

    def __init__(
        self,
        sink: ScanIncidentSink,
        diagnostics: ScanDiagnostics,
        threshold_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            isinstance(threshold_seconds, bool)
            or not isinstance(threshold_seconds, (int, float))
            or not isfinite(threshold_seconds)
            or threshold_seconds <= 0
        ):
            raise ValueError("scan silence threshold must be positive")
        self._sink = sink
        self._diagnostics = diagnostics
        self._threshold = float(threshold_seconds)
        self._clock = clock
        self._initialized = False
        self._incident_state_unknown = False
        self._incident_active = False
        self._incident_generation: int | None = None
        self._open_context: ScanIncidentContext | None = None
        self._resolution_pending = False
        self._retry_at: float | None = None
        self._retry_delay = 1.0
        self._generation: int | None = None
        self._started_at: float | None = None
        self._active_seconds = 0.0
        self._last_callback_at: float | None = None
        self._callback_count = 0
        self._failure_latched = False

    @property
    def next_deadline(self) -> float | None:
        """Return the next retry or active scan's all-device silence deadline."""
        if self._retry_at is not None:
            return self._retry_at
        if self._failure_latched or self._incident_active or self._started_at is None:
            return None
        baseline = self._last_callback_at if self._last_callback_at is not None else self._started_at
        return max(self._started_at, baseline) + self._threshold

    async def initialize(self) -> None:
        """Read persisted incident state once, retrying if storage is unavailable."""
        if self._initialized:
            return
        try:
            self._incident_active = await self._sink.active()
        except Exception as error:  # noqa: BLE001 - persistence availability cannot disable runtime detection
            self._incident_state_unknown = True
            _LOGGER.warning("BLE scan incident state lookup failed (%s)", type(error).__name__)
            return
        self._incident_state_unknown = False
        self._initialized = True
        if not self._incident_active and self._open_context is None and self._resolution_pending:
            self._resolution_pending = False
            self._incident_generation = None
            self._failure_latched = False

    async def prepare(self, generation: int) -> None:
        """Start passive capture before asking Bleak to start scanning."""
        await self.initialize()
        self._generation = generation
        self._started_at = None
        self._active_seconds = 0.0
        self._last_callback_at = None
        self._callback_count = 0
        try:
            await self._diagnostics.prepare(generation)
        except Exception as error:  # noqa: BLE001 - missing diagnostics must not prevent scan monitoring
            _LOGGER.warning("BLE scan diagnostics could not start (%s)", type(error).__name__)

    def scan_started(self, generation: int, at: float | None = None) -> None:
        if generation == self._generation:
            self._started_at = self._clock() if at is None else at

    def scan_stopping(self, generation: int) -> None:
        if generation != self._generation:
            return
        if self._started_at is not None:
            self._active_seconds = max(0.0, self._clock() - self._started_at)
            self._started_at = None

    async def scan_start_failed(self, generation: int, error: Exception) -> None:
        if generation != self._generation:
            return
        self._start_open(generation, "scan_start_failed", error)
        try:
            await self._attempt_persistence()
        finally:
            await self._close_diagnostics(generation)
            self._started_at = None

    async def scan_stopped(self, generation: int) -> None:
        if generation != self._generation:
            return
        await self._close_diagnostics(generation)
        self._started_at = None
        self._generation = None

    async def scan_stop_failed(self, generation: int, error: Exception) -> None:
        if generation != self._generation:
            return
        if not self._failure_latched:
            self._start_open(generation, "scan_stop_failed", error)
        try:
            if self._retry_at is None or self._retry_at <= self._clock():
                await self._attempt_persistence()
        finally:
            await self._close_diagnostics(generation)

    def radio_activity(self, generation: int, at: float) -> bool:
        if generation != self._generation:
            return False
        self._callback_count += 1
        self._last_callback_at = at
        if self._incident_active or self._incident_state_unknown or self._open_context is not None:
            if self._resolution_pending:
                return False
            self._resolution_pending = True
            self._incident_generation = generation
            return (self._incident_active or self._incident_state_unknown) and (
                self._retry_at is None or self._retry_at <= self._clock()
            )
        self._failure_latched = False
        return False

    async def flush_recovery(self) -> None:
        """Persist the single pending recovery transition, when needed."""
        await self._attempt_persistence()

    async def check_deadline(self, now: float | None = None) -> bool:
        """Open or retry one incident when its monotonic deadline is reached."""
        current = self._clock() if now is None else now
        if self._retry_at is not None:
            if current < self._retry_at:
                return False
            await self._attempt_persistence()
            return True
        deadline = self.next_deadline
        generation = self._generation
        if deadline is None or generation is None or current < deadline:
            return False
        self._start_open(generation, "scan_silence", None)
        await self._attempt_persistence()
        return True

    def _start_open(
        self,
        generation: int,
        reason: Literal["scan_silence", "scan_start_failed", "scan_stop_failed"],
        error: Exception | None,
    ) -> None:
        if self._failure_latched:
            return
        now = self._clock()
        active_seconds = max(0.0, now - self._started_at) if self._started_at is not None else self._active_seconds
        age = None if self._last_callback_at is None else max(0.0, now - self._last_callback_at)
        self._open_context = ScanIncidentContext(
            generation=generation,
            reason=reason,
            active_seconds=active_seconds,
            bleak_callbacks=self._callback_count,
            last_callback_age_seconds=age,
            threshold_seconds=self._threshold,
            error_kind=None if error is None else type(error).__name__,
        )
        self._incident_generation = generation
        self._failure_latched = True

    async def _attempt_persistence(self) -> None:
        try:
            if self._open_context is not None:
                await self._sink.open(self._open_context)
                self._incident_active = True
                self._open_context = None
                self._retry_at = None
                self._retry_delay = 1.0
            if self._resolution_pending and (self._incident_active or self._incident_state_unknown):
                generation = self._incident_generation
                if generation is not None:
                    await self._sink.resolve(generation, "advertisements_resumed")
                    self._incident_active = False
                    self._incident_state_unknown = False
                    self._incident_generation = None
                    self._resolution_pending = False
                    self._failure_latched = False
                    self._retry_at = None
                    self._retry_delay = 1.0
        except Exception as error:  # noqa: BLE001 - persistence failures stay pending for bounded retry
            self._retry_at = self._clock() + self._retry_delay
            _LOGGER.warning(
                "BLE scan incident persistence failed (%s); retrying in %.1f seconds",
                type(error).__name__,
                self._retry_delay,
            )
            self._retry_delay = min(self._retry_delay * 2, 30.0)

    async def _close_diagnostics(self, generation: int) -> None:
        try:
            await self._diagnostics.close(generation)
        except Exception as error:  # noqa: BLE001 - diagnostics close cannot stop capture lifecycle
            _LOGGER.warning("BLE scan diagnostics could not stop (%s)", type(error).__name__)
