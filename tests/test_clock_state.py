from __future__ import annotations

import pytest

from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.domain.clock_state import (
    ClockAnchor,
    ClockIntent,
    ClockRange,
    ClockState,
    ClockStateError,
)


def _range(start: int, end: int, offset: float | None, source: str = "clock-a") -> ClockRange:
    return ClockRange(
        start,
        end,
        offset,
        0.5 if offset is not None else None,
        "confirmed" if offset is not None else "unknown",
        source,
    )


def test_new_clock_evidence_fills_gaps_without_changing_prior_audio_time() -> None:
    original = _range(10, 20, 100.0)
    state = ClockState((original,)).add_range(_range(15, 25, 200.0, "clock-b"))

    assert state.ranges == (original, _range(20, 25, 200.0, "clock-b"))
    assert ClockSegmentMap.from_state(state).utc_for(19, 50) == 150.0
    assert ClockSegmentMap.from_state(state).utc_for(20, 50) == 250.0


def test_adjacent_unchanged_clock_ranges_merge_and_retain_source_identity() -> None:
    state = ClockState().add_range(_range(0, 5, 10.0))
    state = state.add_range(_range(5, 8, 10.0))

    assert state.ranges == (_range(0, 8, 10.0),)


def test_unknown_write_window_stays_unmapped_between_known_epochs() -> None:
    state = ClockState((_range(0, 5, 10.0), _range(5, 8, None, "uncertain"), _range(8, 12, 20.0, "clock-b")))
    segments = ClockSegmentMap.from_state(state)

    assert segments.utc_for(4, 100) == 110.0
    assert segments.utc_for(6, 100) is None
    assert segments.utc_for(9, 100) == 120.0


def test_clock_write_transition_keeps_ambiguous_boundary_explicit() -> None:
    original = _range(0, 5, 10.0)
    intent = ClockIntent("set-time-1", "prepared", 100, 200, 201.0, 5)
    state = ClockState((original,)).begin_intent(intent)
    state = state.mark_uncertain("set-time-1", write_sequence_after=7)
    unknown = _range(5, 7, None, "uncertain-write")
    corrected = _range(7, 10, 100.0, "set-time-1")
    state = state.finish_intent("set-time-1", (unknown, corrected))

    assert state.intent is None
    assert state.ranges == (original, unknown, corrected)
    assert ClockSegmentMap.from_state(state).utc_for(6, 50) is None
    assert ClockSegmentMap.from_state(state).utc_for(8, 50) == 150.0


def test_latest_useful_anchor_survives_no_audio_state_updates_and_round_trips() -> None:
    anchor = ClockAnchor(8, 20.0, 0.75, "approximate", "clock-b")
    state = ClockState((_range(0, 5, 10.0),), anchor=anchor)
    state = state.add_range(_range(5, 8, 10.0))

    assert state.anchor == anchor
    assert ClockState.from_payload(state.to_payload()) == state


def test_clock_state_payload_rejects_unknown_schema_and_fields() -> None:
    payload = ClockState().to_payload()
    payload["schema_version"] = 2
    with pytest.raises(ClockStateError, match="schema is unsupported"):
        ClockState.from_payload(payload)

    payload = ClockState().to_payload()
    payload["unexpected"] = True
    with pytest.raises(ClockStateError, match="payload is invalid"):
        ClockState.from_payload(payload)


def test_clock_write_transition_rejects_conflicting_or_impossible_steps() -> None:
    state = ClockState()
    intent = ClockIntent("set-time-1", "prepared", 100, 200, 201.0, 5)
    state = state.begin_intent(intent)
    with pytest.raises(ClockStateError, match="already pending"):
        state.begin_intent(intent)
    with pytest.raises(ClockStateError, match="transition is invalid"):
        state.finish_intent("set-time-1", ())
    with pytest.raises(ClockStateError, match="transition is invalid"):
        state.mark_uncertain("other-id")
    canceled = state.cancel_prepared_intent("set-time-1")
    assert canceled.intent is None
    with pytest.raises(ClockStateError, match="transition is invalid"):
        canceled.mark_uncertain("set-time-1")


def test_uncertain_clock_write_cannot_finish_without_unknown_boundary_coverage() -> None:
    intent = ClockIntent("set-time-1", "prepared", 100, 200, 201.0, 5)
    unresolved = ClockState().begin_intent(intent).mark_uncertain("set-time-1")
    with pytest.raises(ClockStateError, match="boundary is unresolved"):
        unresolved.finish_intent("set-time-1", ())

    bounded = ClockState().begin_intent(intent).mark_uncertain("set-time-1", write_sequence_after=7)
    with pytest.raises(ClockStateError, match="coverage is incomplete"):
        bounded.finish_intent("set-time-1", ())
    with pytest.raises(ClockStateError, match="must remain unknown"):
        bounded.finish_intent("set-time-1", (_range(5, 7, 10.0),))


def test_uncertain_clock_intent_accepts_one_later_known_boundary() -> None:
    intent = ClockIntent("set-time-1", "prepared", 100, 200, 201.0, 5)
    state = ClockState().begin_intent(intent).mark_uncertain("set-time-1")
    bounded = state.mark_uncertain("set-time-1", write_sequence_after=7)

    assert bounded.intent is not None
    assert bounded.intent.write_sequence_after == 7
    assert bounded.mark_uncertain("set-time-1", write_sequence_after=7) == bounded
    with pytest.raises(ClockStateError, match="transition is invalid"):
        bounded.mark_uncertain("set-time-1", write_sequence_after=8)


def test_uncertain_intent_hides_existing_mappings_across_its_write_window() -> None:
    intent = ClockIntent("set-time-1", "prepared", 100, 200, 201.0, 5)
    state = ClockState((_range(0, 10, 10.0),)).begin_intent(intent)
    state = state.mark_uncertain("set-time-1", write_sequence_after=7)
    segments = ClockSegmentMap.from_state(state)

    assert segments.utc_for(4, 50) == 60.0
    assert segments.utc_for(6, 50) is None
    assert segments.utc_for(8, 50) == 60.0


@pytest.mark.parametrize(
    "item",
    [
        (1, 1, 1.0, 0.1, "confirmed", "id"),
        (0, 2, None, 0.1, "unknown", "id"),
        (0, 2, None, None, "confirmed", "id"),
        (0, 2, 1.0, -1.0, "confirmed", "id"),
        (0, 2, "offset", 0.1, "confirmed", "id"),
        (0, 2, float("nan"), 0.1, "confirmed", "id"),
        (0, 2, 1.0, 0.1, [], "id"),
    ],
)
def test_invalid_range_is_rejected(item: tuple[object, ...]) -> None:
    with pytest.raises(ClockStateError):
        ClockRange(*item)  # type: ignore[arg-type]
