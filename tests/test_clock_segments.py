from __future__ import annotations

from dataclasses import replace

import pytest

from omi_collector.capture.adapters.clock_observations import ClockObservation
from omi_collector.capture.adapters.clock_segments import (
    ClockEpochMembership,
    ClockSegment,
    ClockSegmentError,
    ClockSegmentMap,
    segments_with_estimates,
)


def _observation() -> ClockObservation:
    return ClockObservation(
        1,
        "observation",
        0,
        "native_trusted",
        "session",
        "host-boot",
        1000.0,
        1000.2,
        10.0,
        10.2,
        1005,
        10,
        20,
    )


def test_segment_uses_rtc_read_midpoint_and_leaves_unconfirmed_records_unknown() -> None:
    segment = ClockSegment.from_confirmed_membership(_observation(), ClockEpochMembership("observation", 12, 15))
    mapping = ClockSegmentMap((segment,))

    assert segment.utc_offset_seconds == pytest.approx(-4.9)
    assert segment.uncertainty_seconds == pytest.approx(1.1)
    assert mapping.utc_for(12, 105) == pytest.approx(100.1)
    assert mapping.utc_for(11, 105) is None
    assert mapping.utc_for(15, 105) is None


def test_segment_map_rejects_overlapping_clock_epochs() -> None:
    observation = _observation()
    with pytest.raises(ClockSegmentError, match="overlap"):
        ClockSegmentMap(
            (
                ClockSegment.from_confirmed_membership(observation, ClockEpochMembership("observation", 10, 14)),
                ClockSegment.from_confirmed_membership(observation, ClockEpochMembership("observation", 13, 15)),
            )
        )


def test_drifted_initial_observation_corrects_only_its_explicitly_confirmed_membership() -> None:
    observation = replace(_observation(), device_epoch=8200, observation_role="initial")
    segment = ClockSegment.from_confirmed_membership(observation, ClockEpochMembership("observation", 12, 15))

    assert ClockSegmentMap((segment,)).utc_for(12, 8500) == pytest.approx(1300.1)
    assert ClockSegmentMap(()).utc_for(12, 8500) is None


def test_segment_rejects_non_native_or_mismatched_membership() -> None:
    membership = ClockEpochMembership("other-observation", 12, 15)
    with pytest.raises(ClockSegmentError, match="native RTC anchor"):
        ClockSegment.from_confirmed_membership(_observation(), membership)
    with pytest.raises(ClockSegmentError, match="native RTC anchor"):
        ClockSegment.from_confirmed_membership(
            replace(_observation(), evidence_kind="anchored_monotonic"),
            ClockEpochMembership("observation", 12, 15),
        )


def test_unmatched_native_info_observation_estimates_its_sequence_range() -> None:
    observation = replace(
        _observation(),
        info_sequence_min=10_165_164,
        info_sequence_max=10_849_980,
        device_epoch=1_790_768_454,
        host_realtime_start=1_790_767_757.109,
        host_realtime_end=1_790_767_757.140,
    )

    segment_map = segments_with_estimates((observation,), ClockSegmentMap(()))

    assert len(segment_map.segments) == 1
    segment = segment_map.segments[0]
    assert (segment.start_sequence, segment.next_sequence) == (10_165_164, 10_849_980)
    assert segment.utc_offset_seconds == pytest.approx(-696.8755)
    assert segment.confidence == "approximate"


def test_clock_correction_estimates_initial_backlog_and_later_boundary_separately() -> None:
    initial = replace(
        _observation(),
        info_sequence_min=100,
        info_sequence_max=120,
        device_epoch=43,
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        operation_id="operation",
        observation_role="initial",
    )
    later = replace(
        initial,
        observation_id="later",
        causal_order=1,
        device_epoch=72,
        info_sequence_min=120,
        info_sequence_max=130,
        host_realtime_start=2000.0,
        host_realtime_end=2000.0,
        observation_role="later",
        parent_observation_id="observation",
        effective_boundary_sequence=120,
    )

    segments = segments_with_estimates((initial, later), ClockSegmentMap(()), frozenset({"operation"})).segments

    assert [(item.start_sequence, item.next_sequence, item.observation_id) for item in segments] == [
        (100, 120, "observation"),
        (120, 130, "later"),
    ]
    assert segments[0].utc_offset_seconds == pytest.approx(957.0)
    assert segments[1].utc_offset_seconds == pytest.approx(1928.0)


def test_later_visit_estimate_preserves_verified_settime_backlog_boundary() -> None:
    initial = replace(
        _observation(),
        info_sequence_min=10,
        info_sequence_max=20,
        device_epoch=1600,
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        operation_id="operation",
        observation_role="initial",
    )
    readback = replace(
        initial,
        observation_id="readback",
        causal_order=1,
        info_sequence_min=20,
        info_sequence_max=20,
        device_epoch=2000,
        host_realtime_start=2000.0,
        host_realtime_end=2000.0,
        observation_role="later",
        parent_observation_id="observation",
        effective_boundary_sequence=20,
    )
    next_visit = replace(
        initial,
        observation_id="next-visit",
        causal_order=2,
        session_id="next-session",
        info_sequence_min=10,
        info_sequence_max=30,
        device_epoch=3000,
        host_realtime_start=3000.0,
        host_realtime_end=3000.0,
        operation_id=None,
        observation_role="standalone",
        parent_observation_id=None,
    )

    segments = segments_with_estimates(
        (initial, readback, next_visit), ClockSegmentMap(()), frozenset({"operation"})
    ).segments

    assert [(item.start_sequence, item.next_sequence, item.observation_id) for item in segments] == [
        (10, 20, "observation"),
        (20, 30, "next-visit"),
    ]
    assert [item.utc_offset_seconds for item in segments] == pytest.approx([-600.0, 0.0])


def test_second_verified_correction_and_membership_keep_their_priority() -> None:
    first = replace(
        _observation(),
        info_sequence_min=10,
        info_sequence_max=20,
        device_epoch=1600,
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        operation_id="first",
        observation_role="initial",
    )
    first_readback = replace(
        first,
        observation_id="first-readback",
        causal_order=1,
        info_sequence_min=20,
        info_sequence_max=20,
        device_epoch=2000,
        host_realtime_start=2000.0,
        host_realtime_end=2000.0,
        observation_role="later",
        parent_observation_id="observation",
        effective_boundary_sequence=20,
    )
    second = replace(
        first,
        observation_id="second",
        causal_order=2,
        info_sequence_max=30,
        device_epoch=3000,
        host_realtime_start=3000.0,
        host_realtime_end=3000.0,
        operation_id="second-op",
    )
    second_readback = replace(
        second,
        observation_id="second-readback",
        causal_order=3,
        info_sequence_min=30,
        info_sequence_max=40,
        device_epoch=3100,
        host_realtime_start=3000.0,
        host_realtime_end=3000.0,
        observation_role="later",
        parent_observation_id="second",
        effective_boundary_sequence=30,
    )
    confirmed = ClockSegment("confirmed", 15, 18, 9.0, 1.0)

    segments = segments_with_estimates(
        (first, first_readback, second, second_readback),
        ClockSegmentMap((confirmed,)),
        frozenset({"first", "second-op"}),
    ).segments

    assert [(item.start_sequence, item.next_sequence, item.confidence) for item in segments] == [
        (10, 15, "approximate"),
        (15, 18, "confirmed"),
        (18, 20, "approximate"),
        (20, 30, "approximate"),
        (30, 40, "approximate"),
    ]
    assert [item.utc_offset_seconds for item in segments] == pytest.approx([-600, 9, -600, 0, -100])


def test_membership_for_tail_does_not_discard_observation_backlog_estimate() -> None:
    observation = replace(
        _observation(),
        info_sequence_min=10,
        info_sequence_max=20,
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        device_epoch=1600,
    )
    confirmed = ClockSegmentMap((ClockSegment("observation", 20, 21, 0.0, 1.0),))

    segments = segments_with_estimates((observation,), confirmed).segments

    assert [(item.start_sequence, item.next_sequence, item.confidence) for item in segments] == [
        (10, 20, "approximate"),
        (20, 21, "confirmed"),
    ]


def test_approximate_estimate_keeps_rtc_quantization_allowance() -> None:
    observation = replace(_observation(), info_sequence_min=4, info_sequence_max=5)

    segment = segments_with_estimates((observation,), ClockSegmentMap(())).segments[0]

    assert segment.utc_offset_seconds == pytest.approx(-4.9)
    assert segment.uncertainty_seconds == pytest.approx(1.1)
    assert segment.confidence == "approximate"


@pytest.mark.parametrize("start,next_sequence", [(-1, 1), (1, 1), (2, 1)])
def test_clock_ranges_reject_negative_or_empty_or_reversed_boundaries(start: int, next_sequence: int) -> None:
    with pytest.raises(ClockSegmentError):
        ClockEpochMembership("observation", start, next_sequence)
    with pytest.raises(ClockSegmentError):
        ClockSegment("observation", start, next_sequence, 0.0, 0.0)


@pytest.mark.parametrize(
    "offset,uncertainty",
    [(float("nan"), 0.0), (float("inf"), 0.0), (0.0, -1.0), (0.0, float("nan")), (0.0, float("inf"))],
)
def test_clock_segments_reject_nonfinite_offsets_and_invalid_uncertainty(offset: float, uncertainty: float) -> None:
    with pytest.raises(ClockSegmentError):
        ClockSegment("observation", 0, 1, offset, uncertainty)


def test_clock_segment_owns_start_but_excludes_next_sequence_and_accepts_zero_uncertainty() -> None:
    mapping = ClockSegmentMap((ClockSegment("observation", 0, 1, 0.0, 0.0),))

    assert mapping.utc_for(0, 123) == 123
    assert mapping.utc_for(1, 123) is None


def test_later_same_range_observation_replaces_estimate_without_fragments() -> None:
    first = replace(_observation(), info_sequence_min=10, info_sequence_max=20, device_epoch=10)
    later = replace(first, observation_id="later", causal_order=1, device_epoch=30)

    segment_map = segments_with_estimates((first, later), ClockSegmentMap(()))

    assert [(segment.start_sequence, segment.next_sequence) for segment in segment_map.segments] == [(10, 20)]
    assert segment_map.utc_for(10, 100) == pytest.approx(1070.1)
    assert segment_map.utc_for(20, 100) is None


def test_estimates_leave_gaps_outside_observed_ranges_unknown() -> None:
    first = replace(_observation(), info_sequence_min=10, info_sequence_max=12)
    second = replace(first, observation_id="later", causal_order=1, info_sequence_min=14, info_sequence_max=16)

    segment_map = segments_with_estimates((first, second), ClockSegmentMap(()))

    assert [(segment.start_sequence, segment.next_sequence) for segment in segment_map.segments] == [(10, 12), (14, 16)]
    assert segment_map.utc_for(12, 100) is None
    assert segment_map.utc_for(13, 100) is None
    assert segment_map.utc_for(16, 100) is None
