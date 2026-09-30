"""Pure lifecycle decisions for one quarantined evidence directory."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class QuarantineState(StrEnum):
    RECOVERING = "recovering"
    RETRYABLE = "retryable"
    INVALID_EVIDENCE = "invalid_evidence"
    OUTPUT_DURABLE_UNMARKED = "output_durable_unmarked"
    UNPROCESSABLE_UNMARKED = "unprocessable_unmarked"
    PUBLISHED = "published"
    UNPROCESSABLE = "unprocessable"
    DEFERRED = "deferred"


class QuarantineEvent(StrEnum):
    INSPECT = "inspect"
    RECOVERED_RETRYABLE = "recovered_retryable"
    RECOVERED_INVALID = "recovered_invalid"
    RECOVERED_OUTPUT_UNMARKED = "recovered_output_unmarked"
    RECOVERED_UNPROCESSABLE = "recovered_unprocessable"
    RECOVERED_PUBLISHED = "recovered_published"
    OUTPUT_COMMITTED = "output_committed"
    CLASSIFIED_UNPROCESSABLE = "classified_unprocessable"
    RETRYABLE_FAILURE = "retryable_failure"
    MARK_COMMITTED = "mark_committed"
    MARK_FAILED = "mark_failed"
    RETENTION_EXPIRED = "retention_expired"
    DEFER = "defer"
    CANCEL = "cancel"
    RESTART = "restart"


class QuarantineAction(StrEnum):
    SALVAGE = "salvage"
    REAUTHENTICATE = "reauthenticate"
    MARK_PUBLISHED = "mark_published"
    MARK_UNPROCESSABLE = "mark_unprocessable"
    KEEP = "keep"
    DELETE = "delete"
    SKIP = "skip"
    WAIT = "wait"


@dataclass(frozen=True, slots=True)
class QuarantineDecision:
    state: QuarantineState
    action: QuarantineAction


class QuarantineTransitionError(RuntimeError):
    """An event is impossible for the current quarantine lifecycle state."""


_TRANSITIONS = {
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_RETRYABLE): QuarantineDecision(
        QuarantineState.RETRYABLE, QuarantineAction.SALVAGE
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_INVALID): QuarantineDecision(
        QuarantineState.INVALID_EVIDENCE, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_OUTPUT_UNMARKED): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_UNPROCESSABLE): QuarantineDecision(
        QuarantineState.UNPROCESSABLE, QuarantineAction.SKIP
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_PUBLISHED): QuarantineDecision(
        QuarantineState.PUBLISHED, QuarantineAction.SKIP
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.RETRYABLE, QuarantineAction.SALVAGE
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.INVALID_EVIDENCE, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.PUBLISHED, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.PUBLISHED, QuarantineAction.SKIP
    ),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.INSPECT): QuarantineDecision(
        QuarantineState.UNPROCESSABLE, QuarantineAction.SKIP
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.MARK_PUBLISHED
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.OUTPUT_COMMITTED): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.MARK_PUBLISHED
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.MARK_COMMITTED): QuarantineDecision(
        QuarantineState.PUBLISHED, QuarantineAction.KEEP
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.MARK_FAILED): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.KEEP
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.CLASSIFIED_UNPROCESSABLE): QuarantineDecision(
        QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineAction.MARK_UNPROCESSABLE
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.CLASSIFIED_UNPROCESSABLE): QuarantineDecision(
        QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineAction.MARK_UNPROCESSABLE
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.MARK_COMMITTED): QuarantineDecision(
        QuarantineState.UNPROCESSABLE, QuarantineAction.KEEP
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.MARK_FAILED): QuarantineDecision(
        QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineAction.KEEP
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.RETRYABLE_FAILURE): QuarantineDecision(
        QuarantineState.RETRYABLE, QuarantineAction.KEEP
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.RETRYABLE_FAILURE): QuarantineDecision(
        QuarantineState.INVALID_EVIDENCE, QuarantineAction.KEEP
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.RETRYABLE_FAILURE): QuarantineDecision(
        QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineAction.KEEP
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.RETRYABLE_FAILURE): QuarantineDecision(
        QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineAction.KEEP
    ),
    (QuarantineState.PUBLISHED, QuarantineEvent.RETENTION_EXPIRED): QuarantineDecision(
        QuarantineState.PUBLISHED, QuarantineAction.DELETE
    ),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.RETENTION_EXPIRED): QuarantineDecision(
        QuarantineState.UNPROCESSABLE, QuarantineAction.DELETE
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.DEFER): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.DEFER): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.DEFER): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.DEFER): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.CANCEL): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.CANCEL): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.CANCEL): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.CANCEL): QuarantineDecision(
        QuarantineState.DEFERRED, QuarantineAction.WAIT
    ),
    (QuarantineState.DEFERRED, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE
    ),
    (QuarantineState.PUBLISHED, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.PUBLISHED, QuarantineAction.SKIP
    ),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.RESTART): QuarantineDecision(
        QuarantineState.UNPROCESSABLE, QuarantineAction.SKIP
    ),
}


def transition(state: QuarantineState, event: QuarantineEvent) -> QuarantineDecision:
    """Apply one closed-union lifecycle transition without side effects."""
    if not isinstance(state, QuarantineState) or not isinstance(event, QuarantineEvent):
        raise QuarantineTransitionError("unsupported quarantine state or event")
    try:
        return _TRANSITIONS[state, event]
    except KeyError as error:
        raise QuarantineTransitionError(f"{event.value} is invalid while {state.value}") from error
