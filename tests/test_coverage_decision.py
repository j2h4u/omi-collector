"""Coverage decisions preserve full-source and certified-prefix custody."""

import pytest

from omi_collector.capture.domain.coverage_decision import CoverageDecisionError, decide_coverage
from omi_collector.capture.domain.internal_audio_machine import AudioPhase


def test_full_coverage_begins_retirement() -> None:
    assert decide_coverage("sealed_waiting", 10, 10) == (AudioPhase.RETIRING, 0)


def test_certified_blocked_prefix_keeps_uncovered_records() -> None:
    assert decide_coverage("blocked", 4, 10) == (AudioPhase.COVERED, None)


def test_partial_source_requires_blocked_lifecycle() -> None:
    with pytest.raises(CoverageDecisionError, match="only a certified blocked prefix"):
        decide_coverage("sealed_waiting", 4, 10)


def test_invalid_transition_is_refused() -> None:
    with pytest.raises(CoverageDecisionError, match="capture source FSM refused ReadyCustody"):
        decide_coverage("receiving", 10, 10)
