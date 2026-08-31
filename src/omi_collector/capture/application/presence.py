"""Fresh BLE presence observation and bounded opportunistic wake scheduling.

The scheduler deliberately has no GATT knowledge.  It only decides when a
single caller may begin an attempt, and guarantees that the observer has been
stopped before returning that decision.  Advertisement observations are
advisory: the connected session must still obtain authoritative INFO.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG

PresenceCallback = Callable[[object], object]


class PresenceObserver(Protocol):
    """Minimal active-scan boundary used by the scheduler and tests."""

    async def start(self, callback: PresenceCallback) -> object:
        """Start active observation; callbacks are fresh scanner events only."""

    async def stop(self) -> object:
        """Stop observation and release scanner resources."""


@dataclass(frozen=True, slots=True)
class PresencePolicy:
    """Hysteresis and independent fallback bounds, expressed in seconds."""

    absence_seconds: float = DEFAULT_CONFIG.presence.absence_seconds
    fallback_seconds: float = DEFAULT_CONFIG.presence.fallback_seconds
    drained_fallback_seconds: float = DEFAULT_CONFIG.presence.drained_fallback_seconds
    rapid_backoff: tuple[float, ...] = DEFAULT_CONFIG.retry.rapid_backoff
    scan_transition_seconds: float = DEFAULT_CONFIG.presence.scan_transition_seconds

    def __post_init__(self) -> None:
        if (
            self.absence_seconds <= 0
            or self.fallback_seconds <= 0
            or self.drained_fallback_seconds <= 0
            or self.scan_transition_seconds <= 0
        ):
            raise ValueError("presence policy bounds must be positive")
        if self.fallback_seconds > DEFAULT_CONFIG.presence.max_fallback_seconds:
            raise ValueError(
                f"ordinary fallback must be at most {DEFAULT_CONFIG.presence.max_fallback_seconds:g} seconds"
            )
        if self.drained_fallback_seconds > DEFAULT_CONFIG.presence.max_drained_fallback_seconds:
            raise ValueError(
                f"drained fallback must be at most {DEFAULT_CONFIG.presence.max_drained_fallback_seconds:g} seconds"
            )
        if not self.rapid_backoff or any(delay <= 0 for delay in self.rapid_backoff):
            raise ValueError("rapid retry delays must be positive")


@dataclass(frozen=True, slots=True)
class PresenceWake:
    """Why an attempt was released from the presence scheduler."""

    reason: str
    candidate: object | None = None
    observed_at: float | None = None


class PresenceScanTransitionError(RuntimeError):
    """A scanner transition remained live after bounded cancellation."""


class PresenceScanStopError(PresenceScanTransitionError):
    """The scanner could not be proven stopped before a GATT attempt."""


class PresenceScheduler:
    """Serialize fresh observation, cooldown, fallback, and rapid retry.

    ``wait_for_attempt`` is the only method that releases the GATT caller.  It
    stops the active observer first, so observer callbacks cannot overlap
    provider construction or a live GATT session.
    """

    def __init__(
        self,
        observer: PresenceObserver,
        *,
        policy: PresencePolicy | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], object] = asyncio.sleep,
    ) -> None:
        self._observer = observer
        self._policy = policy or PresencePolicy()
        self._clock = clock
        self._sleep = sleep
        now = clock()
        # The first attempt is intentionally released by the bounded fallback;
        # active scanning starts immediately and may release it sooner.
        self._fallback_deadline = now + self._policy.fallback_seconds
        self._retry_deadline: float | None = None
        self._retry_number = 0
        self._last_matching: float | None = None
        self._latest_candidate: object | None = None
        self._latest_candidate_at: float | None = None
        self._confirmed_absent = True
        self._suppress_continuous_ad_wake = False
        self._drained_mode = False
        self._wake = asyncio.Event()
        self._scan_active = False
        self._closed = False

    @property
    def last_matching_age(self) -> float | None:
        """Return current observation age without exposing the BLE address."""
        if self._last_matching is None:
            return None
        return max(0.0, self._clock() - self._last_matching)

    @property
    def policy(self) -> PresencePolicy:
        """Return the immutable timing policy shared with the coordinator."""
        return self._policy

    @property
    def drained_cooldown_remaining_seconds(self) -> float:
        """Return the monotonic delay until the current clean-drain wake."""
        if not self._drained_mode:
            return 0.0
        return max(0.0, self._fallback_deadline - self._clock())

    def invalidate_candidate(self) -> None:
        """Discard a stale BlueZ candidate and require a fresh observation."""
        self._latest_candidate = None
        self._latest_candidate_at = None
        self._last_matching = None
        self._confirmed_absent = True
        self._retry_deadline = None
        self._retry_number = 0
        self._wake.clear()

    async def wait_for_attempt(self) -> PresenceWake:
        """Wait for an advisory wake or fallback, stopping scan before return."""
        if self._closed:
            raise RuntimeError("presence scheduler is closed")
        await self._start_scan()
        while True:
            now = self._clock()
            self._confirm_absence_if_due(now)
            wake_reason = self._due_reason(now)
            if wake_reason is not None:
                await self._stop_scan()
                self._wake.clear()
                candidate, observed_at = self._fresh_candidate_snapshot()
                return PresenceWake(wake_reason, candidate, observed_at)

            event_task = asyncio.create_task(self._wake.wait())
            timer_task = asyncio.create_task(self._wait_until(self._next_deadline(now)))
            try:
                done, _ = await asyncio.wait({event_task, timer_task}, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                await _cancel(event_task)
                await _cancel(timer_task)
                with suppress(BaseException):
                    await self._stop_scan()
                raise
            finally:
                pending = tuple(
                    cast(asyncio.Task[object], task) for task in (event_task, timer_task) if not task.done()
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            if event_task in done:
                self._wake.clear()
                if self._ad_wake_allowed():
                    await self._stop_scan()
                    candidate, observed_at = self._fresh_candidate_snapshot()
                    return PresenceWake("advertisement", candidate, observed_at)
                continue
            self._confirm_absence_if_due(self._clock())

    async def attempt_finished(self, outcome: str) -> None:
        """Schedule the next independent fallback after a completed attempt."""
        valid_outcomes = {
            "clean",
            "retry",
            "retry_after_progress",
            "connected_interrupted",
            "connected_interrupted_after_progress",
        }
        if outcome not in valid_outcomes:
            raise ValueError(f"unknown presence attempt outcome: {outcome}")
        if self._closed:
            return
        now = self._clock()
        self._fallback_deadline = now + self._policy.fallback_seconds
        if outcome == "clean":
            self._retry_deadline = None
            self._retry_number = 0
            # A completed GATT session is proof of presence even when startup
            # was released by the immediate fallback and no advertisement was
            # observed.  Anchor hysteresis at this completion instead of
            # treating the first nearby advertisement as a new arrival.
            self._last_matching = now
            self._confirmed_absent = False
            self._suppress_continuous_ad_wake = True
            self._fallback_deadline = now + self._policy.drained_fallback_seconds
            self._drained_mode = True
        else:
            self._drained_mode = False
            if outcome in {"retry_after_progress", "connected_interrupted_after_progress"}:
                self._retry_number = 0
            if outcome.startswith("connected_interrupted"):
                self._last_matching = now
                self._confirmed_absent = False
                self._suppress_continuous_ad_wake = False
            if self.last_matching_age is not None and self.last_matching_age <= self._policy.absence_seconds:
                delay = self._policy.rapid_backoff[min(self._retry_number, len(self._policy.rapid_backoff) - 1)]
                self._retry_deadline = now + delay
                self._retry_number += 1
            else:
                self._retry_deadline = None
        await self._start_scan()

    async def close(self) -> None:
        """Stop the active scan; safe on success, failure, and cancellation."""
        self._closed = True
        self._wake.set()
        try:
            await self._stop_scan()
        except Exception:  # noqa: BLE001 - do not mask the operation being cleaned up
            return

    def _on_matching_advertisement(self, candidate: object) -> None:
        now = self._clock()
        previous = self._last_matching
        self._last_matching = now
        self._latest_candidate = candidate
        self._latest_candidate_at = now
        gap = float("inf") if previous is None else max(0.0, now - previous)
        was_absent = self._confirmed_absent or gap >= self._policy.absence_seconds
        self._confirmed_absent = False
        if self._drained_mode and now >= self._fallback_deadline:
            self._suppress_continuous_ad_wake = False
        if was_absent:
            self._suppress_continuous_ad_wake = False
            self._retry_deadline = None
            self._retry_number = 0
        if not self._suppress_continuous_ad_wake and self._retry_deadline is None:
            self._wake.set()

    def _fresh_candidate_snapshot(self) -> tuple[object | None, float | None]:
        """Return a coherent fresh candidate/timestamp pair, or two Nones."""
        candidate = self._latest_candidate
        observed_at = self._latest_candidate_at
        if candidate is None or observed_at is None:
            return None, None
        if self._clock() - observed_at > self._policy.absence_seconds:
            return None, None
        return candidate, observed_at

    async def _start_scan(self) -> None:
        if self._closed or self._scan_active:
            return
        if self._due_reason(self._clock()) is not None:
            return
        self._scan_active = True
        try:
            await _transition(
                self._observer.start(self._on_matching_advertisement),
                self._policy.scan_transition_seconds,
                "start",
            )
        except asyncio.CancelledError:
            with suppress(Exception):
                await self._stop_scan()
            raise
        except PresenceScanTransitionError:
            with suppress(Exception):
                await self._stop_scan()
            raise
        except Exception:  # noqa: BLE001 - fallback remains authoritative if scanning is unavailable
            await self._stop_scan()
            return

    async def _stop_scan(self) -> None:
        if not self._scan_active:
            return
        try:
            await _transition(
                self._observer.stop(),
                self._policy.scan_transition_seconds,
                "stop",
            )
        except (TimeoutError, PresenceScanTransitionError) as error:
            raise PresenceScanStopError(str(error)) from error
        self._scan_active = False

    def _confirm_absence_if_due(self, now: float) -> None:
        if self._last_matching is not None and now - self._last_matching >= self._policy.absence_seconds:
            self._confirmed_absent = True
            self._suppress_continuous_ad_wake = False
            self._retry_deadline = None

    def _ad_wake_allowed(self) -> bool:
        if self._drained_mode and self._clock() < self._fallback_deadline:
            return False
        return (not self._suppress_continuous_ad_wake or self._confirmed_absent) and self._retry_deadline is None

    def _due_reason(self, now: float) -> str | None:
        if self._retry_deadline is not None and now >= self._retry_deadline:
            if self.last_matching_age is not None and self.last_matching_age <= self._policy.absence_seconds:
                self._retry_deadline = None
                return "rapid_retry"
            self._retry_deadline = None
        if now >= self._fallback_deadline and (
            not self._drained_mode
            or (self.last_matching_age is not None and self.last_matching_age <= self._policy.absence_seconds)
        ):
            return "fallback"
        return None

    def _next_deadline(self, now: float) -> float:
        cooldown_expired_while_absent = (
            self._drained_mode
            and now >= self._fallback_deadline
            and (
                self._last_matching is None
                or self.last_matching_age is None
                or self.last_matching_age > self._policy.absence_seconds
            )
        )
        deadlines = [] if cooldown_expired_while_absent else [self._fallback_deadline]
        if self._retry_deadline is not None:
            deadlines.append(self._retry_deadline)
        if self._last_matching is not None and self._retry_deadline is not None:
            deadlines.append(self._last_matching + self._policy.absence_seconds)
        if cooldown_expired_while_absent:
            deadlines.append(now + self._policy.absence_seconds)
        return min(deadlines, default=now)

    def _next_attempt_deadline(self) -> float:
        if self._retry_deadline is None:
            return self._fallback_deadline
        return min(self._fallback_deadline, self._retry_deadline)

    async def _wait_until(self, deadline: float) -> None:
        delay = max(0.0, deadline - self._clock())
        result = self._sleep(delay)
        if inspect.isawaitable(result):
            await result


async def _cancel(task: asyncio.Task[object]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _transition(awaitable: object, timeout: float, operation: str) -> None:
    """Bound a scanner transition; uncertain cancellation fails closed."""
    if not inspect.isawaitable(awaitable):
        return
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        if not await _cancel_bounded(cast(asyncio.Task[object], task), timeout):
            task.add_done_callback(_consume_task)
        raise
    if not done:
        if not await _cancel_bounded(cast(asyncio.Task[object], task), timeout):
            task.add_done_callback(_consume_task)
            raise PresenceScanTransitionError(f"presence scanner {operation} cancellation is uncertain")
        raise TimeoutError(f"presence scanner {operation} timed out after {timeout:g}s")
    task.result()


async def _cancel_bounded(task: asyncio.Task[object], timeout: float) -> bool:
    """Cancel a backend transition without waiting forever for bad teardown."""
    task.cancel()
    presence_config = DEFAULT_CONFIG.presence
    grace = min(
        presence_config.scan_cancel_grace_max_seconds,
        max(presence_config.scan_cancel_grace_min_seconds, timeout * presence_config.scan_cancel_grace_fraction),
    )
    done, _ = await asyncio.wait({task}, timeout=grace)
    return bool(done)


def _consume_task(task: asyncio.Task[object]) -> None:
    """Consume a detached backend transition result after cancellation."""
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception:  # noqa: BLE001 - detached backend task result is intentionally consumed
        return
