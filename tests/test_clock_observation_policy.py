from __future__ import annotations

import pytest

from omi_collector.capture.domain.clock_state import ClockAnchor, ClockIntent, ClockRange, ClockState
from omi_collector.capture.domain.ring_protocol import RingInfo


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (12.0, ClockRange(10, 15, 10.0, 2.5, "approximate", "old")),
        (12.01, ClockRange(10, 15, None, None, "unknown", "old")),
    ],
)
def test_observation_tolerance_classifies_audio_since_old_anchor(offset: float, expected: ClockRange) -> None:
    old = ClockRange(5, 10, 9.0, 0.25, "confirmed", "earlier")
    state = ClockState((old,), anchor=ClockAnchor(10, 10.0, 0.5, "confirmed", "old"))

    observed = state.observe(
        RingInfo(8, 15, 100, 0, 80),
        offset_seconds=offset,
        uncertainty_seconds=0.75,
        source_id="new",
        tolerance_seconds=2.0,
    )

    assert observed.ranges == (old, expected)
    assert observed.anchor == ClockAnchor(15, offset, 0.75, "approximate", "new")


def test_observation_without_anchor_marks_existing_audio_unknown() -> None:
    observed = ClockState().observe(
        RingInfo(4, 10, 100, 0, 80),
        offset_seconds=10.0,
        uncertainty_seconds=0.5,
        source_id="first",
        tolerance_seconds=2.0,
    )

    assert observed.ranges == (ClockRange(4, 10, None, None, "unknown", "first"),)
    assert observed.anchor == ClockAnchor(10, 10.0, 0.5, "approximate", "first")


def test_pending_clock_write_makes_new_audio_unknown_even_within_tolerance() -> None:
    intent = ClockIntent("write", "prepared", 100, 101, 101.0, 10)
    state = ClockState(intent=intent, anchor=ClockAnchor(10, 10.0, 0.5, "confirmed", "old"))

    observed = state.observe(
        RingInfo(8, 15, 100, 0, 80),
        offset_seconds=11.0,
        uncertainty_seconds=0.75,
        source_id="new",
        tolerance_seconds=2.0,
    )

    assert observed.ranges == (ClockRange(10, 15, None, None, "unknown", "old"),)
    assert observed.anchor == ClockAnchor(15, 11.0, 0.75, "approximate", "new")
    assert observed.intent == intent
