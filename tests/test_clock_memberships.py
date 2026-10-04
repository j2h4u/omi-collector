from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from omi_collector.capture.adapters.clock_memberships import ClockMembership, ClockMembershipError, ClockMembershipStore
from omi_collector.capture.adapters.clock_observations import ClockObservation


def _observation(observation_id: str = "anchor") -> ClockObservation:
    return ClockObservation(
        1, observation_id, 0, "native_trusted", "session-a", "boot", 1000.0, 1000.2, 1.0, 1.2, 1005, 0, 1
    )


def test_empty_membership_store_has_no_records_or_segments(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")

    assert store.records() == ()
    assert store.segments(()).segments == ()


def test_zero_to_one_membership_survives_reopen_and_maps_its_only_record(tmp_path: Path) -> None:
    device_state_path = tmp_path / "device.json"
    membership = ClockMembership("anchor", "session-a", 0, 1)
    store = ClockMembershipStore(device_state_path)

    assert store.record(membership) == membership
    store.record_membership("anchor", "session-a", 0, 1)

    reopened = ClockMembershipStore(device_state_path)
    assert reopened.records() == (membership,)
    segments = reopened.segments((_observation(),))

    assert segments.utc_for(0, 1005) == 1000.1
    assert segments.utc_for(1, 1005) is None


@pytest.mark.parametrize(
    ("observation_id", "session_id", "start", "next_sequence"),
    [
        ("", "session-a", 0, 1),
        ("anchor", "", 0, 1),
        ("anchor", "session-a", -1, 0),
        ("anchor", "session-a", 0, 0),
        ("anchor", "session-a", 2, 1),
    ],
)
def test_membership_rejects_empty_identity_and_invalid_bounds(
    tmp_path: Path, observation_id: str, session_id: str, start: int, next_sequence: int
) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")

    with pytest.raises(ClockMembershipError, match="clock membership is invalid"):
        store.record(ClockMembership(observation_id, session_id, start, next_sequence))


def test_duplicate_membership_is_idempotent_and_conflict_preserves_ledger(tmp_path: Path) -> None:
    device_state_path = tmp_path / "device.json"
    store = ClockMembershipStore(device_state_path)
    membership = ClockMembership("anchor", "session-a", 0, 1)
    store.record(membership)
    (membership_file,) = (tmp_path / "clock-memberships").glob("*.json")
    durable_bytes = membership_file.read_bytes()

    assert store.record(membership) == membership
    assert membership_file.read_bytes() == durable_bytes
    durable_before_conflict = store.records()
    store.record_membership("anchor", "session-a", 0, 1)
    assert membership_file.read_bytes() == durable_bytes
    assert store.records() == durable_before_conflict

    with pytest.raises(ClockMembershipError, match="conflicts with durable evidence"):
        store.record(ClockMembership("anchor", "session-b", 0, 1))

    assert membership_file.read_bytes() == durable_bytes
    assert store.records() == durable_before_conflict
    assert ClockMembershipStore(device_state_path).records() == durable_before_conflict


def test_adjacent_memberships_form_adjacent_clock_segments(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    store.record(ClockMembership("anchor", "session-a", 0, 1))
    store.record(ClockMembership("next", "session-a", 1, 2))

    segments = store.segments((_observation(), replace(_observation(), observation_id="next")))

    assert segments.utc_for(0, 1005) == 1000.1
    assert segments.utc_for(1, 1005) == 1000.1
    assert segments.utc_for(2, 1005) is None


def test_records_reject_overlapping_memberships(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    store.record(ClockMembership("anchor", "session-a", 0, 2))
    store.record(ClockMembership("next", "session-a", 1, 3))

    with pytest.raises(ClockMembershipError, match="clock memberships overlap"):
        store.records()


def test_membership_without_a_durable_observation_cannot_form_segments(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    store.record(ClockMembership("unknown", "session-a", 0, 1))

    with pytest.raises(ClockMembershipError, match="no durable observation"):
        store.segments((_observation(),))


def test_membership_does_not_cross_a_ble_session(tmp_path: Path) -> None:
    store = ClockMembershipStore(tmp_path / "device.json")
    store.record(ClockMembership("anchor", "session-b", 0, 1))

    with pytest.raises(ClockMembershipError, match="no durable observation"):
        store.segments((_observation(),))
