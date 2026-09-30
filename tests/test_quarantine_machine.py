from __future__ import annotations

from itertools import product

import pytest

from omi_collector.capture.domain.quarantine_machine import (
    QuarantineAction,
    QuarantineDecision,
    QuarantineEvent,
    QuarantineState,
    QuarantineTransitionError,
    transition,
)

EXPECTED = {
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

STATES = (
    QuarantineState.RECOVERING,
    QuarantineState.RETRYABLE,
    QuarantineState.INVALID_EVIDENCE,
    QuarantineState.OUTPUT_DURABLE_UNMARKED,
    QuarantineState.UNPROCESSABLE_UNMARKED,
    QuarantineState.PUBLISHED,
    QuarantineState.UNPROCESSABLE,
    QuarantineState.DEFERRED,
)
EVENTS = (
    QuarantineEvent.INSPECT,
    QuarantineEvent.RECOVERED_RETRYABLE,
    QuarantineEvent.RECOVERED_INVALID,
    QuarantineEvent.RECOVERED_OUTPUT_UNMARKED,
    QuarantineEvent.RECOVERED_UNPROCESSABLE,
    QuarantineEvent.RECOVERED_PUBLISHED,
    QuarantineEvent.OUTPUT_COMMITTED,
    QuarantineEvent.CLASSIFIED_UNPROCESSABLE,
    QuarantineEvent.RETRYABLE_FAILURE,
    QuarantineEvent.MARK_COMMITTED,
    QuarantineEvent.MARK_FAILED,
    QuarantineEvent.RETENTION_EXPIRED,
    QuarantineEvent.DEFER,
    QuarantineEvent.CANCEL,
    QuarantineEvent.RESTART,
)


def test_state_event_matrix_is_complete_and_rejects_impossible_cells() -> None:
    assert set(QuarantineState) == set(STATES)
    assert set(QuarantineEvent) == set(EVENTS)
    cells = set(product(STATES, EVENTS))
    invalid = cells - set(EXPECTED)
    assert set(EXPECTED).isdisjoint(invalid)
    assert set(EXPECTED) | invalid == cells
    for state, event in cells:
        if (state, event) in EXPECTED:
            assert transition(state, event) == EXPECTED[state, event]
        else:
            with pytest.raises(QuarantineTransitionError):
                transition(state, event)


def test_every_declared_lifecycle_state_is_reachable() -> None:
    reachable = {QuarantineState.RECOVERING}
    while True:
        advanced = reachable | {decision.state for (state, _event), decision in EXPECTED.items() if state in reachable}
        if advanced == reachable:
            break
        reachable = advanced
    assert reachable == set(STATES)


def test_publication_restart_and_retention_path_preserves_order() -> None:
    first = transition(QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED)
    assert first == EXPECTED[QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED]
    failed_mark = transition(first.state, QuarantineEvent.MARK_FAILED)
    assert failed_mark.state is QuarantineState.OUTPUT_DURABLE_UNMARKED
    restarted = transition(failed_mark.state, QuarantineEvent.RESTART)
    assert restarted.state is QuarantineState.RECOVERING
    assert restarted.action is QuarantineAction.REAUTHENTICATE
    recovered = transition(restarted.state, QuarantineEvent.RECOVERED_RETRYABLE)
    assert recovered.state is QuarantineState.RETRYABLE
    assert (
        transition(recovered.state, QuarantineEvent.OUTPUT_COMMITTED).state is QuarantineState.OUTPUT_DURABLE_UNMARKED
    )
    assert (
        transition(QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.MARK_COMMITTED).state
        is QuarantineState.PUBLISHED
    )
    assert transition(QuarantineState.PUBLISHED, QuarantineEvent.RETENTION_EXPIRED).action is QuarantineAction.DELETE


def test_deferral_cancellation_and_restart_preserve_source_for_reauthentication() -> None:
    for event in (QuarantineEvent.DEFER, QuarantineEvent.CANCEL):
        deferred = transition(QuarantineState.INVALID_EVIDENCE, event)
        assert deferred.state is QuarantineState.DEFERRED
        restarted = transition(deferred.state, QuarantineEvent.RESTART)
        assert restarted.state is QuarantineState.RECOVERING
        assert restarted.action is QuarantineAction.REAUTHENTICATE
        assert transition(restarted.state, QuarantineEvent.RECOVERED_INVALID).state is QuarantineState.INVALID_EVIDENCE


def test_unprocessable_is_terminal_only_after_marker_commit() -> None:
    pending = transition(QuarantineState.RETRYABLE, QuarantineEvent.CLASSIFIED_UNPROCESSABLE)
    assert pending.state is QuarantineState.UNPROCESSABLE_UNMARKED
    assert transition(pending.state, QuarantineEvent.MARK_FAILED).state is pending.state
    assert transition(pending.state, QuarantineEvent.MARK_COMMITTED).state is QuarantineState.UNPROCESSABLE
