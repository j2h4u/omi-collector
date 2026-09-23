from __future__ import annotations

from dataclasses import replace

import pytest

from omi_collector.capture.adapters.clock_observations import ClockObservation
from omi_collector.capture.adapters.clock_segments import (
    ClockEpochMembership,
    ClockSegment,
    ClockSegmentError,
    ClockSegmentMap,
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
