from __future__ import annotations

import pytest

import omi_collector.capture.adapters.public_time_facts as public_time_facts
from omi_collector.capture.adapters.public_time_facts import (
    FrozenTimePredecessor,
    PublicTimeProjection,
)
from omi_collector.capture.domain.clock_state import ClockIntent, ClockRange, ClockState


def _mapped(
    start: int,
    end: int,
    offset: float,
    source: str,
    *,
    confidence: str = "confirmed",
) -> ClockRange:
    return ClockRange(start, end, offset, 0.25, confidence, source)


def test_freezes_exact_mapped_label_quality_and_uncertainty() -> None:
    state = ClockState((_mapped(10, 12, -4.6, "anchor", confidence="approximate"),))

    frozen = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=100)

    assert frozen.quality == "approximate"
    assert frozen.seconds == 95
    assert frozen.utc_uncertainty_seconds == 0.25
    assert frozen.comparable_to_previous is False


def test_adjacent_mapped_ranges_share_causality_despite_observation_change() -> None:
    state = ClockState((_mapped(10, 11, 1.0, "first"), _mapped(11, 12, 1.0, "second")))
    first = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=100)

    second = PublicTimeProjection.from_state(state).freeze(
        sequence=11,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert first.causal_membership == second.causal_membership
    assert second.comparable_to_previous is True


def test_later_clock_snapshot_can_confirm_prior_mapped_record_is_comparable() -> None:
    prior_state = ClockState((_mapped(10, 11, 1.0, "first"),))
    current_state = ClockState((_mapped(10, 11, 1.0, "first"), _mapped(11, 12, 1.0, "second")))
    first = PublicTimeProjection.from_state(prior_state).freeze(sequence=10, raw_timestamp=100)

    second = PublicTimeProjection.from_state(current_state).freeze(
        sequence=11,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert first.causal_membership != second.causal_membership
    assert second.comparable_to_previous is True


def test_explicit_unknown_ranges_preserve_labels_and_causal_boundaries() -> None:
    state = ClockState(
        (
            ClockRange(10, 12, None, None, "unknown", "first-unknown"),
            ClockRange(12, 13, None, None, "unknown", "second-unknown"),
        )
    )
    first = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=400)
    second = PublicTimeProjection.from_state(state).freeze(
        sequence=11,
        raw_timestamp=401,
        predecessor=FrozenTimePredecessor(10, first),
    )
    third = PublicTimeProjection.from_state(state).freeze(
        sequence=12,
        raw_timestamp=402,
        predecessor=FrozenTimePredecessor(11, second),
    )

    assert (first.quality, first.seconds, first.utc_uncertainty_seconds) == ("unknown", 400, None)
    assert second.comparable_to_previous is True
    assert third.comparable_to_previous is False
    assert second.causal_membership != third.causal_membership


def test_uncovered_records_do_not_invent_clock_membership() -> None:
    first = PublicTimeProjection.from_state(ClockState()).freeze(sequence=10, raw_timestamp=400)
    second = PublicTimeProjection.from_state(ClockState()).freeze(
        sequence=11,
        raw_timestamp=401,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert first.seconds == 400
    assert second.seconds == 401
    assert second.comparable_to_previous is False


def test_uncertain_intent_cuts_mapped_causality_before_and_after_boundary() -> None:
    intent = ClockIntent("set-time", "uncertain", 1, 2, 10.0, 11, 12)
    state = ClockState((_mapped(10, 13, 5.0, "anchor"),), intent=intent)
    before = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=100)
    uncertain = PublicTimeProjection.from_state(state).freeze(
        sequence=11,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(10, before),
    )
    after = PublicTimeProjection.from_state(state).freeze(
        sequence=12,
        raw_timestamp=102,
        predecessor=FrozenTimePredecessor(11, uncertain),
    )

    assert (before.quality, uncertain.quality, after.quality) == ("known", "unknown", "known")
    assert uncertain.seconds == 101
    assert uncertain.comparable_to_previous is False
    assert after.comparable_to_previous is False
    assert before.causal_membership != after.causal_membership


def test_consecutive_records_inside_bounded_uncertain_window_are_incomparable() -> None:
    intent = ClockIntent("set-time", "uncertain", 1, 2, 10.0, 11, 14)
    state = ClockState((_mapped(10, 15, 5.0, "anchor"),), intent=intent)
    first = PublicTimeProjection.from_state(state).freeze(sequence=11, raw_timestamp=101)

    second = PublicTimeProjection.from_state(state).freeze(
        sequence=12,
        raw_timestamp=102,
        predecessor=FrozenTimePredecessor(11, first),
    )

    assert (first.quality, second.quality) == ("unknown", "unknown")
    assert first.causal_membership != second.causal_membership
    assert second.comparable_to_previous is False


def test_consecutive_records_inside_open_uncertain_window_are_incomparable() -> None:
    intent = ClockIntent("set-time", "uncertain", 1, 2, 10.0, 11)
    state = ClockState((_mapped(10, 15, 5.0, "anchor"),), intent=intent)
    first = PublicTimeProjection.from_state(state).freeze(sequence=11, raw_timestamp=101)

    second = PublicTimeProjection.from_state(state).freeze(
        sequence=12,
        raw_timestamp=102,
        predecessor=FrozenTimePredecessor(11, first),
    )

    assert (first.quality, second.quality) == ("unknown", "unknown")
    assert first.causal_membership != second.causal_membership
    assert second.comparable_to_previous is False


def test_empty_uncertain_window_still_preserves_its_causal_boundary() -> None:
    intent = ClockIntent("empty", "uncertain", 0, 0, 0.0, 101, 101)
    state = ClockState((_mapped(100, 103, 0.0, "same"),), intent=intent)
    before = PublicTimeProjection.from_state(state).freeze(sequence=100, raw_timestamp=100)

    after = PublicTimeProjection.from_state(state).freeze(
        sequence=101,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(100, before),
    )

    assert (before.quality, after.quality) == ("known", "known")
    assert before.causal_membership != after.causal_membership
    assert after.comparable_to_previous is False


def test_unrepresentable_mapping_clears_to_useful_unknown_label() -> None:
    state = ClockState((_mapped(10, 12, float(1 << 32), "anchor"),))
    first = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=7)
    second = PublicTimeProjection.from_state(state).freeze(
        sequence=11,
        raw_timestamp=8,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert (first.quality, first.seconds, first.utc_uncertainty_seconds) == ("unknown", 7, None)
    assert second.comparable_to_previous is True


def test_unrepresentable_mapped_raw_labels_do_not_bridge_offset_changes() -> None:
    state = ClockState((_mapped(10, 11, -1000.0, "before"), _mapped(11, 12, -2000.0, "after")))
    first = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=1)

    second = PublicTimeProjection.from_state(state).freeze(
        sequence=11,
        raw_timestamp=1005,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert (first.quality, first.seconds) == ("unknown", 1)
    assert (second.quality, second.seconds) == ("unknown", 1005)
    assert first.causal_membership != second.causal_membership
    assert second.comparable_to_previous is False


def test_sequence_gap_is_time_incomparable_within_one_mapped_component() -> None:
    state = ClockState((_mapped(10, 20, 0.0, "anchor"),))
    first = PublicTimeProjection.from_state(state).freeze(sequence=10, raw_timestamp=100)

    later = PublicTimeProjection.from_state(state).freeze(
        sequence=12,
        raw_timestamp=102,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert later.causal_membership == first.causal_membership
    assert later.comparable_to_previous is False


def test_prepared_projection_handles_range_edges_and_uncertain_boundary() -> None:
    state = ClockState(
        (
            _mapped(10, 12, -0.5, "first"),
            ClockRange(12, 13, None, None, "unknown", "gap"),
            _mapped(13, 16, 1.0, "last", confidence="approximate"),
        ),
        intent=ClockIntent("clock", "uncertain", 0, 0, 0.0, 14, 15),
    )
    projection = PublicTimeProjection.from_state(state)
    expected = [
        (9, "unknown", 100, "unknown-uncovered:9"),
        (10, "known", 100, "mapped:10:12"),
        (12, "unknown", 100, "unknown-range:12:13:gap"),
        (13, "approximate", 101, "mapped:13:14"),
        (14, "unknown", 100, "unknown-intent:14:15:clock:14"),
        (15, "approximate", 101, "mapped:15:16"),
        (16, "unknown", 100, "unknown-uncovered:16"),
    ]
    for sequence, quality, seconds, membership in expected:
        actual = projection.freeze(sequence=sequence, raw_timestamp=100)
        assert (actual.quality, actual.seconds, actual.causal_membership) == (quality, seconds, membership)


def test_prepared_projection_builds_components_once_for_many_records(monkeypatch: pytest.MonkeyPatch) -> None:
    state = ClockState(tuple(_mapped(i, i + 1, 0.0, str(i)) for i in range(1000)))
    calls = 0
    original = public_time_facts._mapped_intervals

    def count_intervals(snapshot: ClockState) -> tuple[tuple[int, int], ...]:
        nonlocal calls
        calls += 1
        return original(snapshot)

    monkeypatch.setattr(public_time_facts, "_mapped_intervals", count_intervals)
    projection = PublicTimeProjection.from_state(state)
    for sequence in range(1000):
        assert projection.freeze(sequence=sequence, raw_timestamp=100).causal_membership == "mapped:0:1000"
    assert calls == 1
