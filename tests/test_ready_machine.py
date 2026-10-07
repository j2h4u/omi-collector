"""Ready publication is a pure decision from durable facts."""

from dataclasses import FrozenInstanceError
from itertools import product

import pytest

from omi_collector.capture.domain.ready_machine import ReadyCommand, ReadyState, decide_ready


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
