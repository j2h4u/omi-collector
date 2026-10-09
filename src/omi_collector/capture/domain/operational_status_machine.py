"""Finite reducer for the service's durable operational status."""

from __future__ import annotations

from enum import StrEnum
from itertools import product


class OperationalState(StrEnum):
    UNKNOWN = "unknown"
    CLEAR = "clear"
    BLOCKED = "blocked"


class OperationalDimension(StrEnum):
    QUALITY = "quality"
    PUBLICATION = "publication"
    CLOCK = "clock"


class PublicationOutcome(StrEnum):
    BLOCKED = "blocked"
    PUBLISHED = "published"
    TRANSIENT = "transient"
    WAITING = "waiting"


class OperationalSignal(StrEnum):
    BLOCK = "block"
    CLEAR = "clear"
    CONFIGURED = "configured"
    UNKNOWN = "unknown"
    UNCHANGED = "unchanged"


_TRANSITIONS = {
    (OperationalState.UNKNOWN, OperationalSignal.BLOCK): OperationalState.BLOCKED,
    (OperationalState.UNKNOWN, OperationalSignal.CLEAR): OperationalState.CLEAR,
    (OperationalState.UNKNOWN, OperationalSignal.CONFIGURED): OperationalState.CLEAR,
    (OperationalState.UNKNOWN, OperationalSignal.UNKNOWN): OperationalState.UNKNOWN,
    (OperationalState.UNKNOWN, OperationalSignal.UNCHANGED): OperationalState.UNKNOWN,
    (OperationalState.CLEAR, OperationalSignal.BLOCK): OperationalState.BLOCKED,
    (OperationalState.CLEAR, OperationalSignal.CLEAR): OperationalState.CLEAR,
    (OperationalState.CLEAR, OperationalSignal.CONFIGURED): OperationalState.CLEAR,
    (OperationalState.CLEAR, OperationalSignal.UNKNOWN): OperationalState.UNKNOWN,
    (OperationalState.CLEAR, OperationalSignal.UNCHANGED): OperationalState.CLEAR,
    (OperationalState.BLOCKED, OperationalSignal.BLOCK): OperationalState.BLOCKED,
    (OperationalState.BLOCKED, OperationalSignal.CLEAR): OperationalState.CLEAR,
    (OperationalState.BLOCKED, OperationalSignal.CONFIGURED): OperationalState.BLOCKED,
    (OperationalState.BLOCKED, OperationalSignal.UNKNOWN): OperationalState.UNKNOWN,
    (OperationalState.BLOCKED, OperationalSignal.UNCHANGED): OperationalState.BLOCKED,
}


def check_transition_completeness() -> None:
    """Fail when a new state or signal has no explicit transition."""
    expected = set(product(OperationalState, OperationalSignal))
    if _TRANSITIONS.keys() != expected:
        raise AssertionError("operational status transition table is incomplete")


def transition(state: OperationalState, signal: OperationalSignal) -> OperationalState:
    check_transition_completeness()
    return _TRANSITIONS[state, signal]
