from __future__ import annotations

from pathlib import Path

import pytest

from omi_collector.capture.adapters.clock_memberships import ClockMembership, ClockMembershipError, ClockMembershipStore
from omi_collector.capture.adapters.clock_observations import ClockObservation


def _observation() -> ClockObservation:
    return ClockObservation(
        1, "anchor", 0, "native_trusted", "session-a", "boot", 1000.0, 1000.2, 1.0, 1.2, 1005, 100, 100
    )


def test_same_session_info_interval_becomes_a_durable_clock_segment(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    membership = ClockMembership("anchor", "session-a", 100, 110)

    assert store.record(membership) == membership
    segments = store.segments((_observation(),))

    assert segments.utc_for(100, 1005) == 1000.1
    assert segments.utc_for(109, 1005) == 1000.1
    assert segments.utc_for(110, 1005) is None


def test_membership_does_not_cross_a_ble_session(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    store.record(ClockMembership("anchor", "session-b", 100, 110))

    with pytest.raises(ClockMembershipError, match="no durable observation"):
        store.segments((_observation(),))
