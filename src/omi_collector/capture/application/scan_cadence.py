"""Pure scanner cadence states for idle presence searches."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True, slots=True)
class ScanCadencePolicy:
    """Active scan and confirmed-stop pause durations, in seconds."""

    active_seconds: float
    pause_seconds: float

    def __post_init__(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0
            for value in (self.active_seconds, self.pause_seconds)
        ):
            raise ValueError("scan cadence durations must be positive")


@dataclass(frozen=True, slots=True)
class ScanReady:
    """No scanner is running and no pause is armed."""


@dataclass(frozen=True, slots=True)
class ScanStarting:
    """A scanner start is pending and may already deliver radio activity."""

    generation: int
    saw_radio_activity: bool = False


@dataclass(frozen=True, slots=True)
class ScanActive:
    """One confirmed scanner generation and its active-window state."""

    generation: int
    started_at: float
    saw_radio_activity: bool


@dataclass(frozen=True, slots=True)
class ScanPaused:
    """The observer stop completed and its bounded pause is in progress."""

    until: float


type ScanCadenceState = ScanReady | ScanStarting | ScanActive | ScanPaused


def scan_deadline(state: ScanCadenceState, policy: ScanCadencePolicy) -> float | None:
    """Return a deadline only when this active generation has seen radio activity."""
    if isinstance(state, ScanActive) and state.saw_radio_activity:
        return state.started_at + policy.active_seconds
    return None


def record_radio_activity(state: ScanCadenceState, generation: int) -> ScanCadenceState | None:
    """Return an updated state only for the first callback in this generation."""
    if isinstance(state, ScanStarting) and state.generation == generation and not state.saw_radio_activity:
        return ScanStarting(generation, True)
    if isinstance(state, ScanActive) and state.generation == generation and not state.saw_radio_activity:
        return ScanActive(generation, state.started_at, True)
    return None


def can_pause(
    state: ScanCadenceState,
    *,
    now: float,
    searching: bool,
    fresh_candidate: bool,
    policy: ScanCadencePolicy,
) -> bool:
    """Check whether an idle Searching scan may enter its pause."""
    deadline = scan_deadline(state, policy)
    return deadline is not None and now >= deadline and searching and not fresh_candidate
