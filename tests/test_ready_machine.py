"""Recovery closure decisions preserve authenticated evidence."""

import pytest

from omi_collector.capture.domain.ready_machine import (
    Finished,
    PublicationAction,
    PublicationResult,
    PublicationState,
    RetryWait,
    Running,
    Wake,
    publication_transition,
    recovered_closure,
)


def test_failed_stage_revision_keeps_saturating_backoff() -> None:
    state = PublicationState(work=Running(1, "before", 0), next_token=1)
    state, command = publication_transition(state, Finished(1, "after", None, PublicationResult.TRANSIENT, 10, (1, 5)))
    assert isinstance(state.work, RetryWait)
    assert command.deadline == 11
    state, command = publication_transition(state, Wake("after", 11))
    assert command.action is PublicationAction.RUN
    state, command = publication_transition(
        state, Finished(2, "after-again", None, PublicationResult.TRANSIENT, 11, (1, 5))
    )
    assert command.deadline == 16


@pytest.mark.parametrize(
    ("closure_case", "expected"),
    (
        ((None, None, "absence", None), None),
        ((None, 101, "absence", None), (101, "absence")),
        (((100, "absence"), None, "absence", None), None),
        (((100, "absence"), 99, "absence", None), None),
        (((100, "absence"), 100, "recovery_exhausted", None), None),
        (((100, "absence"), 101, "absence", None), (101, "absence")),
        (((100, "drained"), 101, "absence", None), (101, "absence")),
        (((100, "legacy_prefix_publication"), 99, "public gap check", None), None),
        (
            ((100, "legacy_prefix_publication"), 101, "public gap check", None),
            (101, "public gap check"),
        ),
        (((100, "absence"), 99, "drained", 100), (100, "drained")),
        (((100, "absence"), 100, "drained", 100), (100, "drained")),
        (((100, "absence"), 101, "drained", 101), (101, "drained")),
        (((100, "drained"), 100, "drained", 100), None),
        (((100, "drained"), 99, "absence", None), None),
        (((100, "absence"), None, "drained", 100), None),
    ),
)
def test_recovered_closure_frontier_table(
    closure_case: tuple[tuple[int, str] | None, int | None, str, int | None],
    expected: tuple[int, str] | None,
) -> None:
    existing, recovered, reason, cursor = closure_case
    assert (
        recovered_closure(
            existing=existing,
            recovered_frontier=recovered,
            reason=reason,
            drain_cursor=cursor,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("existing", "recovered", "reason", "cursor"),
    (
        ((100, "absence"), 99, "drained", 99),
        ((100, "absence"), 101, "drained", 100),
        ((100, "absence"), None, "drained", 99),
        ((100, "absence"), 99, "absence", 100),
        (None, None, "drained", None),
        (None, 100, "absence", 0),
        ((100, None), 99, "absence", None),
        ((100, ""), 99, "absence", None),
        (None, 99, "", None),
        (None, 99, "drained", True),
    ),
)
def test_recovered_closure_rejects_invalid_evidence(
    existing: tuple[int, str] | None,
    recovered: int | None,
    reason: str,
    cursor: int | None,
) -> None:
    with pytest.raises(ValueError):
        recovered_closure(
            existing=existing,
            recovered_frontier=recovered,
            reason=reason,
            drain_cursor=cursor,
        )
