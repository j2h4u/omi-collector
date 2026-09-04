"""Async interpreter for the pure presence scheduling policy.

Bluetooth lifecycle belongs here; product scheduling belongs exclusively to
``presence_machine``. Scanner generations are transport facts, never timer
epochs or product state.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG
from . import presence_machine as machine


@dataclass(frozen=True, slots=True)
class PresenceAdvertisement:
    """One advisory scanner observation retaining only candidate and RSSI."""

    candidate: object = field(repr=False)
    rssi_dbm: int | None = None


PresenceCallback = Callable[[object], object]


class PresenceObserver(Protocol):
    """Minimal active-scan boundary used by the scheduler and tests."""

    async def start(self, callback: PresenceCallback) -> object:
        """Start active observation; callbacks are fresh scanner events only."""

    async def stop(self) -> object:
        """Stop observation and release scanner resources."""


@dataclass(frozen=True, slots=True)
class PresencePolicy:
    """Validated interpreter timings, expressed in seconds."""

    absence_seconds: float = DEFAULT_CONFIG.presence.absence_seconds
    fallback_seconds: float = DEFAULT_CONFIG.presence.fallback_seconds
    drained_fallback_seconds: float = DEFAULT_CONFIG.presence.drained_fallback_seconds
    rapid_backoff: tuple[float, ...] = DEFAULT_CONFIG.retry.rapid_backoff
    scan_transition_seconds: float = DEFAULT_CONFIG.presence.scan_transition_seconds
    scan_cancel_grace_min_seconds: float = DEFAULT_CONFIG.presence.scan_cancel_grace_min_seconds
    scan_cancel_grace_max_seconds: float = DEFAULT_CONFIG.presence.scan_cancel_grace_max_seconds
    scan_cancel_grace_fraction: float = DEFAULT_CONFIG.presence.scan_cancel_grace_fraction

    def __post_init__(self) -> None:
        if any(
            value <= 0
            for value in (
                self.absence_seconds,
                self.fallback_seconds,
                self.drained_fallback_seconds,
                self.scan_transition_seconds,
                self.scan_cancel_grace_min_seconds,
                self.scan_cancel_grace_max_seconds,
            )
        ):
            raise ValueError("presence policy bounds must be positive")
        if self.scan_cancel_grace_min_seconds > self.scan_cancel_grace_max_seconds:
            raise ValueError("scan cancellation minimum must not exceed its maximum")
        if not 0 <= self.scan_cancel_grace_fraction <= 1:
            raise ValueError("scan cancellation fraction must be between zero and one")
        if not self.rapid_backoff or any(delay <= 0 for delay in self.rapid_backoff):
            raise ValueError("rapid retry delays must be positive")

    def machine_policy(self) -> machine.PresenceMachinePolicy:
        """Project validated runtime configuration into the pure policy."""
        return machine.PresenceMachinePolicy(
            absence_seconds=self.absence_seconds,
            fallback_seconds=self.fallback_seconds,
            drained_fallback_seconds=self.drained_fallback_seconds,
            rapid_backoff=self.rapid_backoff,
        )


@dataclass(frozen=True, slots=True)
class PresenceWake:
    """A redeemed attempt permit for the session lifecycle."""

    reason: str
    candidate: object | None = field(default=None, repr=False)
    observed_at: float | None = None
    advertisement_rssi_dbm: int | None = None


class PresenceScanTransitionError(RuntimeError):
    """A scanner transition remained live after bounded cancellation."""


class PresenceScanStopError(PresenceScanTransitionError):
    """The scanner could not be proven stopped before a GATT attempt."""


class PresenceScheduler:
    """Interpret immutable presence decisions without scanner/GATT overlap."""

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
        self._machine_policy = self._policy.machine_policy()
        self._clock = clock
        self._sleep = sleep
        self._state: machine.PresenceState = machine.initial_state(clock(), self._machine_policy)
        self._advertisements: asyncio.Queue[tuple[int, machine.Advertisement]] = asyncio.Queue()
        self._changed = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._next_scanner_generation = 0
        self._active_scanner_generation: int | None = None
        self._scanner_transition_lock = asyncio.Lock()
        self._waiter_active = False
        self._force_stop_required = False

    @property
    def policy(self) -> PresencePolicy:
        """Return the immutable timing policy shared with the coordinator."""
        return self._policy

    @property
    def drained_cooldown_remaining_seconds(self) -> float:
        """Return operator telemetry without exposing product state."""
        return machine.drained_cooldown_remaining_seconds(self._state, at=self._clock())

    async def wait_for_attempt(self) -> PresenceWake:
        """Return one permit only after the matching scanner has stopped."""
        if isinstance(self._state, machine.Closed):
            raise RuntimeError("presence scheduler is closed")
        if isinstance(self._state, machine.Attempting):
            raise RuntimeError("an attempt permit is already outstanding")
        if self._waiter_active:
            raise RuntimeError("only one presence waiter may be active")
        self._waiter_active = True
        self._loop = asyncio.get_running_loop()
        try:
            return await self._wait_for_attempt()
        except asyncio.CancelledError:
            await self._cancel_wait()
            raise
        finally:
            self._waiter_active = False

    async def attempt_finished(self, outcome: machine.AttemptOutcome) -> None:
        """Submit the single typed outcome for an outstanding permit."""
        try:
            result = self._apply(machine.AttemptFinished(at=self._clock(), outcome=outcome))
        except machine.UnexpectedAttemptOutcomeError as error:
            self._state = error.closed_state
            self._changed.set()
            await self._best_effort_stop(force=True)
            raise
        if isinstance(result.directive, machine.Observe):
            await self._start_scan()

    async def close(self) -> None:
        """Idempotently close product state and make one bounded stop attempt."""
        self._close_state()
        await self._best_effort_stop(force=self._force_stop_required)

    async def _wait_for_attempt(self) -> PresenceWake:
        while True:
            if isinstance(self._state, machine.Closed):
                raise RuntimeError("presence scheduler is closed")
            await self._start_scan()
            if isinstance(self._state, machine.Closed):
                raise RuntimeError("presence scheduler is closed")
            advertisement = self._next_advertisement()
            if advertisement is not None:
                wake = await self._handle_advertisement(advertisement)
                if wake is not None:
                    return wake
                continue
            deadline = machine.armed_deadline(_waiting_state(self._state))
            if self._clock() >= deadline:
                wake = await self._handle_timer(deadline)
                if wake is not None:
                    return wake
                continue
            wake = await self._wait_until_event(deadline)
            if wake is not None:
                return wake

    async def _wait_until_event(self, deadline: float) -> PresenceWake | None:
        self._changed.clear()
        advertisement_task = asyncio.create_task(self._advertisements.get())
        timer_task = asyncio.create_task(self._sleep_until(deadline))
        changed_task = asyncio.create_task(self._changed.wait())
        try:
            done, _ = await asyncio.wait(
                {advertisement_task, timer_task, changed_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if changed_task in done:
                return None
            if advertisement_task in done:
                _, advertisement = advertisement_task.result()
                return await self._handle_advertisement(advertisement)
            await _cancel_task(advertisement_task)
            advertisement = self._next_advertisement()
            if advertisement is not None:
                return await self._handle_advertisement(advertisement)
            return await self._handle_timer(deadline)
        finally:
            await _cancel_task(advertisement_task)
            await _cancel_task(timer_task)
            await _cancel_task(changed_task)

    async def _handle_advertisement(self, advertisement: machine.Advertisement) -> PresenceWake | None:
        result = self._apply(machine.AdvertisementObserved(advertisement))
        return await self._redeem(result.directive)

    async def _handle_timer(self, deadline: float) -> PresenceWake | None:
        state = _waiting_state(self._state)
        result = self._apply(machine.TimerFired(at=self._clock(), deadline=deadline, timer_epoch=state.timer_epoch))
        return await self._redeem(result.directive)

    async def _redeem(self, directive: machine.PresenceDirective) -> PresenceWake | None:
        if not isinstance(directive, machine.StopAndBeginAttempt):
            return None
        try:
            await self._stop_scan()
        except asyncio.CancelledError:
            await self._close_after_stop_failure()
            raise
        except Exception:
            await self._close_after_stop_failure()
            raise
        if isinstance(self._state, machine.Closed):
            raise RuntimeError("presence scheduler is closed")
        if not isinstance(self._state, machine.Attempting):
            raise RuntimeError("presence scheduler lost its outstanding attempt permit")
        return _wake(directive.trigger)

    def _apply(self, event: machine.PresenceEvent) -> machine.TransitionResult:
        result = machine.transition(self._state, event, self._machine_policy)
        self._state = result.state
        self._changed.set()
        return result

    async def _start_scan(self) -> bool:
        async with self._scanner_transition_lock:
            return await self._start_scan_locked()

    async def _start_scan_locked(self) -> bool:
        if self._active_scanner_generation is not None:
            return True
        if isinstance(self._state, machine.Closed):
            return False
        generation = self._next_scanner_generation
        self._next_scanner_generation += 1
        self._active_scanner_generation = generation
        try:
            await _transition(
                self._observer.start(lambda observation: self._receive_advertisement(generation, observation)),
                policy=self._policy,
                operation="start",
            )
        except asyncio.CancelledError:
            self._active_scanner_generation = None
            self._discard_advertisements()
            await self._best_effort_stop_locked(force=True)
            raise
        except PresenceScanTransitionError:
            self._active_scanner_generation = None
            self._force_stop_required = True
            self._discard_advertisements()
            self._close_state()
            await self._best_effort_stop_locked(force=True)
            raise
        except Exception:  # noqa: BLE001 - a normal scanner-start refusal is soft
            self._active_scanner_generation = None
            self._discard_advertisements()
            await self._best_effort_stop_locked(force=True)
            return False
        if isinstance(self._state, machine.Closed):
            await self._best_effort_stop_locked(force=True)
            return False
        return True

    async def _stop_scan(self, *, force: bool = False) -> None:
        async with self._scanner_transition_lock:
            await self._stop_scan_locked(force=force)

    async def _stop_scan_locked(self, *, force: bool) -> None:
        if self._active_scanner_generation is None and not force:
            return
        self._active_scanner_generation = None
        self._discard_advertisements()
        try:
            await _transition(self._observer.stop(), policy=self._policy, operation="stop")
        except asyncio.CancelledError:
            self._force_stop_required = True
            self._close_state()
            raise
        except (TimeoutError, PresenceScanTransitionError) as error:
            self._force_stop_required = True
            raise PresenceScanStopError(str(error)) from error
        except Exception as error:
            self._force_stop_required = True
            raise PresenceScanStopError("presence scanner stop failed") from error
        self._force_stop_required = False

    def _receive_advertisement(self, generation: int, observation: object) -> None:
        advertisement = (
            observation if isinstance(observation, PresenceAdvertisement) else PresenceAdvertisement(observation)
        )
        event = machine.Advertisement(
            candidate=advertisement.candidate,
            observed_at=self._clock(),
            rssi_dbm=advertisement.rssi_dbm,
        )
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._buffer_advertisement, generation, event)

    def _buffer_advertisement(self, generation: int, advertisement: machine.Advertisement) -> None:
        if generation != self._active_scanner_generation or isinstance(self._state, machine.Closed):
            return
        self._advertisements.put_nowait((generation, advertisement))

    def _next_advertisement(self) -> machine.Advertisement | None:
        while not self._advertisements.empty():
            generation, advertisement = self._advertisements.get_nowait()
            if generation == self._active_scanner_generation:
                return advertisement
        return None

    def _discard_advertisements(self) -> None:
        while not self._advertisements.empty():
            self._advertisements.get_nowait()

    async def _cancel_wait(self) -> None:
        try:
            await self._stop_scan()
        except asyncio.CancelledError:
            await self._close_after_stop_failure()
        except Exception:  # noqa: BLE001 - cancelled waiting must leave no live scanner
            await self._close_after_stop_failure()

    async def _close_after_stop_failure(self) -> None:
        self._close_state()
        await self._best_effort_stop(force=True)

    def _close_state(self) -> None:
        result = machine.transition(self._state, machine.Shutdown(at=self._clock()), self._machine_policy)
        self._state = result.state
        self._changed.set()

    async def _best_effort_stop(self, *, force: bool) -> None:
        async with self._scanner_transition_lock:
            await self._best_effort_stop_locked(force=force)

    async def _best_effort_stop_locked(self, *, force: bool) -> None:
        try:
            await self._stop_scan_locked(force=force)
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001 - close is intentionally best effort
            return

    async def _sleep_until(self, deadline: float) -> None:
        result = self._sleep(max(0.0, deadline - self._clock()))
        if inspect.isawaitable(result):
            await result


def _waiting_state(state: machine.PresenceState) -> machine.WaitingState:
    if isinstance(state, machine.Attempting | machine.Closed):
        raise RuntimeError("presence scheduler has no waiting deadline")
    return state


def _wake(trigger: machine.AttemptTrigger) -> PresenceWake:
    if isinstance(trigger, machine.AdvertisementTrigger):
        reason = "advertisement"
    elif isinstance(trigger, machine.RapidRetryTrigger):
        reason = "rapid_retry"
    else:
        reason = "fallback"
    advertisement = trigger.advertisement
    if advertisement is None:
        return PresenceWake(reason=reason)
    return PresenceWake(
        reason=reason,
        candidate=advertisement.candidate,
        observed_at=advertisement.observed_at,
        advertisement_rssi_dbm=advertisement.rssi_dbm,
    )


async def _transition(awaitable: object, *, policy: PresencePolicy, operation: str) -> None:
    """Bound scanner transitions and detach only cancellation-uncertain work."""
    if not inspect.isawaitable(awaitable):
        return
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=policy.scan_transition_seconds)
    except asyncio.CancelledError:
        if not await _cancel_bounded(cast(asyncio.Task[object], task), policy):
            task.add_done_callback(_consume_task)
            raise PresenceScanTransitionError(f"presence scanner {operation} cancellation is uncertain") from None
        raise
    if not done:
        if not await _cancel_bounded(cast(asyncio.Task[object], task), policy):
            task.add_done_callback(_consume_task)
            raise PresenceScanTransitionError(f"presence scanner {operation} cancellation is uncertain")
        raise TimeoutError(f"presence scanner {operation} timed out after {policy.scan_transition_seconds:g}s")
    task.result()


async def _cancel_bounded(task: asyncio.Task[object], policy: PresencePolicy) -> bool:
    task.cancel()
    grace = min(
        policy.scan_cancel_grace_max_seconds,
        max(
            policy.scan_cancel_grace_min_seconds,
            policy.scan_transition_seconds * policy.scan_cancel_grace_fraction,
        ),
    )
    done, _ = await asyncio.wait({task}, timeout=grace)
    return bool(done)


async def _cancel_task(task: asyncio.Task[object]) -> None:
    if task.done():
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def _consume_task(task: asyncio.Task[object]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.result()
