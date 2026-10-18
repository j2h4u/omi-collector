from __future__ import annotations

from omi_collector.capture.adapters.public_time_facts import FrozenTimePredecessor, freeze_public_time
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

    frozen = freeze_public_time(state, sequence=10, raw_timestamp=100)

    assert frozen.quality == "approximate"
    assert frozen.seconds == 95
    assert frozen.utc_uncertainty_seconds == 0.25
    assert frozen.comparable_to_previous is False


def test_adjacent_mapped_ranges_share_causality_despite_observation_change() -> None:
    state = ClockState((_mapped(10, 11, 1.0, "first"), _mapped(11, 12, 1.0, "second")))
    first = freeze_public_time(state, sequence=10, raw_timestamp=100)

    second = freeze_public_time(
        state,
        sequence=11,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert first.causal_membership == second.causal_membership
    assert second.comparable_to_previous is True


def test_later_clock_snapshot_can_confirm_prior_mapped_record_is_comparable() -> None:
    prior_state = ClockState((_mapped(10, 11, 1.0, "first"),))
    current_state = ClockState((_mapped(10, 11, 1.0, "first"), _mapped(11, 12, 1.0, "second")))
    first = freeze_public_time(prior_state, sequence=10, raw_timestamp=100)

    second = freeze_public_time(
        current_state,
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
    first = freeze_public_time(state, sequence=10, raw_timestamp=400)
    second = freeze_public_time(
        state,
        sequence=11,
        raw_timestamp=401,
        predecessor=FrozenTimePredecessor(10, first),
    )
    third = freeze_public_time(
        state,
        sequence=12,
        raw_timestamp=402,
        predecessor=FrozenTimePredecessor(11, second),
    )

    assert (first.quality, first.seconds, first.utc_uncertainty_seconds) == ("unknown", 400, None)
    assert second.comparable_to_previous is True
    assert third.comparable_to_previous is False
    assert second.causal_membership != third.causal_membership


def test_uncovered_records_do_not_invent_clock_membership() -> None:
    first = freeze_public_time(ClockState(), sequence=10, raw_timestamp=400)
    second = freeze_public_time(
        ClockState(),
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
    before = freeze_public_time(state, sequence=10, raw_timestamp=100)
    uncertain = freeze_public_time(
        state,
        sequence=11,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(10, before),
    )
    after = freeze_public_time(
        state,
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
    first = freeze_public_time(state, sequence=11, raw_timestamp=101)

    second = freeze_public_time(
        state,
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
    first = freeze_public_time(state, sequence=11, raw_timestamp=101)

    second = freeze_public_time(
        state,
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
    before = freeze_public_time(state, sequence=100, raw_timestamp=100)

    after = freeze_public_time(
        state,
        sequence=101,
        raw_timestamp=101,
        predecessor=FrozenTimePredecessor(100, before),
    )

    assert (before.quality, after.quality) == ("known", "known")
    assert before.causal_membership != after.causal_membership
    assert after.comparable_to_previous is False


def test_unrepresentable_mapping_clears_to_useful_unknown_label() -> None:
    state = ClockState((_mapped(10, 12, float(1 << 32), "anchor"),))
    first = freeze_public_time(state, sequence=10, raw_timestamp=7)
    second = freeze_public_time(
        state,
        sequence=11,
        raw_timestamp=8,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert (first.quality, first.seconds, first.utc_uncertainty_seconds) == ("unknown", 7, None)
    assert second.comparable_to_previous is True


def test_sequence_gap_is_time_incomparable_within_one_mapped_component() -> None:
    state = ClockState((_mapped(10, 20, 0.0, "anchor"),))
    first = freeze_public_time(state, sequence=10, raw_timestamp=100)

    later = freeze_public_time(
        state,
        sequence=12,
        raw_timestamp=102,
        predecessor=FrozenTimePredecessor(10, first),
    )

    assert later.causal_membership == first.causal_membership
    assert later.comparable_to_previous is False
