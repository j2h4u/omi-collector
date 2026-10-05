from __future__ import annotations

from itertools import product

import pytest

from omi_collector.capture.domain.quarantine_machine import (
    QuarantineAction,
    QuarantineEvent,
    QuarantineState,
    QuarantineTransitionError,
    transition,
)

EXPECTED = {
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_RETRYABLE): (
        QuarantineState.RETRYABLE,
        QuarantineAction.SALVAGE,
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_INVALID): (
        QuarantineState.INVALID_EVIDENCE,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_OUTPUT_UNMARKED): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_UNPROCESSABLE): (
        QuarantineState.UNPROCESSABLE,
        QuarantineAction.SKIP,
    ),
    (QuarantineState.RECOVERING, QuarantineEvent.RECOVERED_PUBLISHED): (
        QuarantineState.PUBLISHED,
        QuarantineAction.SKIP,
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.INSPECT): (QuarantineState.RETRYABLE, QuarantineAction.SALVAGE),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.INSPECT): (
        QuarantineState.INVALID_EVIDENCE,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.INSPECT): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.INSPECT): (
        QuarantineState.UNPROCESSABLE_UNMARKED,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.PUBLISHED, QuarantineEvent.INSPECT): (QuarantineState.PUBLISHED, QuarantineAction.SKIP),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.INSPECT): (QuarantineState.UNPROCESSABLE, QuarantineAction.SKIP),
    (QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.MARK_PUBLISHED,
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.OUTPUT_COMMITTED): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.MARK_PUBLISHED,
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.MARK_COMMITTED): (
        QuarantineState.PUBLISHED,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.MARK_FAILED): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.CLASSIFIED_UNPROCESSABLE): (
        QuarantineState.UNPROCESSABLE_UNMARKED,
        QuarantineAction.MARK_UNPROCESSABLE,
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.CLASSIFIED_UNPROCESSABLE): (
        QuarantineState.UNPROCESSABLE_UNMARKED,
        QuarantineAction.MARK_UNPROCESSABLE,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.MARK_COMMITTED): (
        QuarantineState.UNPROCESSABLE,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.MARK_FAILED): (
        QuarantineState.UNPROCESSABLE_UNMARKED,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.RETRYABLE_FAILURE): (QuarantineState.RETRYABLE, QuarantineAction.KEEP),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.RETRYABLE_FAILURE): (
        QuarantineState.INVALID_EVIDENCE,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.RETRYABLE_FAILURE): (
        QuarantineState.OUTPUT_DURABLE_UNMARKED,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.RETRYABLE_FAILURE): (
        QuarantineState.UNPROCESSABLE_UNMARKED,
        QuarantineAction.KEEP,
    ),
    (QuarantineState.PUBLISHED, QuarantineEvent.RETENTION_EXPIRED): (
        QuarantineState.PUBLISHED,
        QuarantineAction.DELETE,
    ),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.RETENTION_EXPIRED): (
        QuarantineState.UNPROCESSABLE,
        QuarantineAction.DELETE,
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.DEFER): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.DEFER): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.DEFER): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.DEFER): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.RETRYABLE, QuarantineEvent.CANCEL): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.CANCEL): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.CANCEL): (
        QuarantineState.DEFERRED,
        QuarantineAction.WAIT,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.CANCEL): (QuarantineState.DEFERRED, QuarantineAction.WAIT),
    (QuarantineState.DEFERRED, QuarantineEvent.RESTART): (QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE),
    (QuarantineState.OUTPUT_DURABLE_UNMARKED, QuarantineEvent.RESTART): (
        QuarantineState.RECOVERING,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.UNPROCESSABLE_UNMARKED, QuarantineEvent.RESTART): (
        QuarantineState.RECOVERING,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.INVALID_EVIDENCE, QuarantineEvent.RESTART): (
        QuarantineState.RECOVERING,
        QuarantineAction.REAUTHENTICATE,
    ),
    (QuarantineState.RETRYABLE, QuarantineEvent.RESTART): (QuarantineState.RECOVERING, QuarantineAction.REAUTHENTICATE),
    (QuarantineState.PUBLISHED, QuarantineEvent.RESTART): (QuarantineState.PUBLISHED, QuarantineAction.SKIP),
    (QuarantineState.UNPROCESSABLE, QuarantineEvent.RESTART): (QuarantineState.UNPROCESSABLE, QuarantineAction.SKIP),
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
            decision = transition(state, event)
            assert (decision.state, decision.action) == EXPECTED[state, event]
        else:
            with pytest.raises(QuarantineTransitionError):
                transition(state, event)


def test_every_declared_lifecycle_state_is_reachable() -> None:
    reachable = {QuarantineState.RECOVERING}
    while True:
        advanced = reachable | {
            next_state for (state, _event), (next_state, _action) in EXPECTED.items() if state in reachable
        }
        if advanced == reachable:
            break
        reachable = advanced
    assert reachable == set(STATES)


@pytest.mark.parametrize(
    ("state", "event"),
    [
        ("retryable", QuarantineEvent.INSPECT),
        (QuarantineState.RETRYABLE, "inspect"),
    ],
)
def test_transition_rejects_invalid_state_or_event_type(state: object, event: object) -> None:
    with pytest.raises(QuarantineTransitionError):
        transition(state, event)  # type: ignore[arg-type]


def test_publication_restart_and_retention_path_preserves_order() -> None:
    first = transition(QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED)
    assert (first.state, first.action) == EXPECTED[QuarantineState.RETRYABLE, QuarantineEvent.OUTPUT_COMMITTED]
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


def test_public_transition_decision_rejects_mutation_and_remains_unchanged() -> None:
    decision = transition(QuarantineState.RETRYABLE, QuarantineEvent.INSPECT)
    expected = (QuarantineState.RETRYABLE, QuarantineAction.SALVAGE)
    state_attribute = "state"

    try:
        setattr(decision, state_attribute, QuarantineState.INVALID_EVIDENCE)
    except AttributeError:
        mutation_rejected = True
    else:
        mutation_rejected = False

    subsequent = transition(QuarantineState.RETRYABLE, QuarantineEvent.INSPECT)
    subsequent_value = (subsequent.state, subsequent.action)
    if not mutation_rejected:
        setattr(decision, state_attribute, expected[0])

    assert mutation_rejected
    assert (decision.state, decision.action) == expected
    assert subsequent_value == expected
