"""Ready publication is a pure decision from durable facts."""

from dataclasses import FrozenInstanceError
from itertools import product

import pytest

from omi_collector.capture.domain.ready_machine import ReadyCommand, ReadyState, decide_ready, recovered_closure


def test_ready_machine_complete_boolean_table() -> None:
    for drained, has_audio, threshold_met in product((False, True), repeat=3):
        if threshold_met and not has_audio:
            with pytest.raises(ValueError, match="empty audio"):
                decide_ready(
                    drained=drained,
                    has_audio=has_audio,
                    threshold_met=threshold_met,
                )
            continue
        decision = decide_ready(
            drained=drained,
            has_audio=has_audio,
            threshold_met=threshold_met,
        )
        if not drained:
            expected = ReadyState.WAITING_FOR_DRAIN
        elif not has_audio or not threshold_met:
            expected = ReadyState.WAITING_FOR_THRESHOLD
        else:
            expected = ReadyState.READY_TO_PUBLISH
        assert decision.state == expected
        assert decision.command == (
            ReadyCommand.PUBLISH if expected == ReadyState.READY_TO_PUBLISH else ReadyCommand.WAIT
        )


def test_ready_decision_cannot_change_after_derivation() -> None:
    decision = decide_ready(drained=True, has_audio=True, threshold_met=True)
    with pytest.raises(FrozenInstanceError):
        decision.__setattr__("command", ReadyCommand.WAIT)
    assert decision.command == ReadyCommand.PUBLISH


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
