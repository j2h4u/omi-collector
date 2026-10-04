from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from omi_collector.capture.adapters import clock_observations
from omi_collector.capture.adapters.clock_observations import ClockObservationError, ClockObservationStore

# Dynamic malformed-schema fixtures intentionally cross the JSON boundary.
# pyright: reportAny=false, reportArgumentType=false


def _values() -> dict[str, object]:
    return {
        "session_id": "session-a",
        "host_boot_id": "boot-a",
        "host_realtime_start": 1789749500.0,
        "host_realtime_end": 1789749500.2,
        "host_monotonic_start": 42.0,
        "host_monotonic_end": 42.2,
        "device_epoch": 1789749500,
        "info_sequence_min": 10,
        "info_sequence_max": 12,
    }


def test_file_lock_takes_and_releases_exclusive_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[int] = []
    monkeypatch.setattr(clock_observations.fcntl, "flock", lambda _fd, operation: events.append(operation))

    with pytest.raises(RuntimeError, match="check release"), clock_observations._file_lock(tmp_path / ".lock"):
        raise RuntimeError("check release")

    assert events == [clock_observations.fcntl.LOCK_EX, clock_observations.fcntl.LOCK_UN]


def test_observation_store_is_causal_and_immutable(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    first = store.append(evidence_kind="native_trusted", **_values())
    second = store.append(
        evidence_kind="native_trusted",
        **{**_values(), "device_epoch": 1789749501, "info_sequence_min": 12, "info_sequence_max": 14},
    )
    established = store.append(
        evidence_kind="native_trusted",
        operation_id="op-a",
        effective_boundary_sequence=11,
        **_values(),
    )

    assert [item.causal_order for item in store.records()] == [0, 1, 2]
    assert established.effective_boundary_sequence == 11
    assert established.causal_order == 2
    assert established.parent_observation_id == first.observation_id
    assert established.observation_role == "initial"
    assert store.records()[0].effective_boundary_sequence is None
    assert second.causal_order == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("host_realtime_start", math.nan),
        ("host_monotonic_end", math.inf),
        ("info_sequence_max", 9),
        ("effective_boundary_sequence", 99),
        ("evidence_kind", "within_threshold"),
    ],
)
def test_store_rejects_untrusted_or_invalid_evidence(tmp_path: Path, field: str, value: object) -> None:
    values = _values()
    values[field] = value
    with pytest.raises(ClockObservationError):
        kind = values.pop("evidence_kind", "native_trusted")
        ClockObservationStore(tmp_path / "device.json").append(evidence_kind=kind, **values)


def test_store_rejects_noncanonical_or_gapped_ledger(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    item = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{item.observation_id}.json"
    document = json.loads(path.read_text())
    document["causal_order"] = 2
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ClockObservationError):
        store.records()


@pytest.mark.parametrize("timestamp", [0, 4294967295])
def test_raw_timestamp_and_hash_survive_reopen(tmp_path: Path, timestamp: int) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    raw_hash = "a" * 64

    store.append(
        evidence_kind="native_trusted",
        raw_timestamp=timestamp,
        raw_timestamp_hash=raw_hash,
        **_values(),
    )

    reopened = ClockObservationStore(tmp_path / "device.json")
    (record,) = reopened.records()
    assert record.raw_timestamp == timestamp
    assert record.raw_timestamp_hash == raw_hash


@pytest.mark.parametrize("timestamp", [-1, 4294967296, True, 1.5, "1"])
def test_invalid_raw_timestamp_does_not_change_durable_ledger(tmp_path: Path, timestamp: object) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError):
        store.append(
            evidence_kind="native_trusted",
            raw_timestamp=timestamp,
            raw_timestamp_hash="b" * 64,
            **_values(),
        )

    assert path.read_bytes() == before
    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


@pytest.mark.parametrize("raw_hash", ["a" * 63, "a" * 65, "g" * 64, "A" * 64, 123])
def test_invalid_raw_timestamp_hash_does_not_change_durable_ledger(tmp_path: Path, raw_hash: object) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError):
        store.append(
            evidence_kind="native_trusted",
            raw_timestamp=1,
            raw_timestamp_hash=raw_hash,
            **_values(),
        )

    assert path.read_bytes() == before
    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


@pytest.mark.parametrize(
    ("raw_timestamp", "raw_hash"),
    [(1, None), (None, "a" * 64)],
)
def test_incomplete_raw_timestamp_evidence_does_not_change_durable_ledger(
    tmp_path: Path, raw_timestamp: int | None, raw_hash: str | None
) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError):
        store.append(
            evidence_kind="native_trusted",
            raw_timestamp=raw_timestamp,
            raw_timestamp_hash=raw_hash,
            **_values(),
        )

    assert path.read_bytes() == before
    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


def test_explicit_observation_id_is_idempotent_but_rejects_changed_evidence(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    evidence = {
        **_values(),
        "evidence_kind": "native_trusted",
        "observation_id": "stable-observation",
        "raw_timestamp": 1,
        "raw_timestamp_hash": "c" * 64,
    }
    original = store.append(**evidence)
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    assert store.append(**evidence) == original
    assert path.read_bytes() == before
    assert store.records() == (original,)

    with pytest.raises(ClockObservationError):
        store.append(**{**evidence, "raw_timestamp": 2})

    assert path.read_bytes() == before
    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


def test_matching_standalone_parent_uses_latest_record_and_survives_reopen(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    first = store.append(evidence_kind="native_trusted", **_values())
    latest = store.append(evidence_kind="native_trusted", **_values())

    initial = store.append(evidence_kind="native_trusted", operation_id="op-a", **_values())
    reopened = ClockObservationStore(tmp_path / "device.json")

    assert first.observation_id != latest.observation_id
    assert (initial.parent_observation_id, initial.observation_role, initial.causal_order) == (
        latest.observation_id,
        "initial",
        2,
    )
    assert reopened.records() == (first, latest, initial)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host_boot_id", "boot-b"),
        ("info_sequence_min", 11),
        ("info_sequence_max", 13),
        ("device_epoch", 1789749501),
    ],
)
def test_operation_does_not_attach_to_nonmatching_standalone(tmp_path: Path, field: str, value: object) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    standalone = store.append(evidence_kind="native_trusted", **_values())
    changed = {**_values(), field: value}

    operation = store.append(evidence_kind="native_trusted", operation_id="op-a", **changed)

    assert operation.parent_observation_id is None
    assert operation.observation_role == "standalone"
    assert ClockObservationStore(tmp_path / "device.json").records()[0] == standalone


def test_later_operation_uses_latest_initial_parent_and_does_not_cross_operations(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    seed = store.append(evidence_kind="native_trusted", **_values())
    first_initial = store.append(evidence_kind="native_trusted", operation_id="op-a", **_values())
    latest_initial = store.append(
        evidence_kind="native_trusted",
        operation_id="op-a",
        observation_role="initial",
        parent_observation_id="explicit-parent",
        **{**_values(), "host_boot_id": "boot-b", "info_sequence_min": 20, "info_sequence_max": 22},
    )
    changed = {**_values(), "host_boot_id": "boot-c", "info_sequence_min": 30, "info_sequence_max": 32}

    later = store.append(evidence_kind="native_trusted", operation_id="op-a", **changed)
    other_operation = store.append(evidence_kind="native_trusted", operation_id="op-b", **changed)
    reopened = ClockObservationStore(tmp_path / "device.json").records()

    assert (first_initial.parent_observation_id, first_initial.observation_role) == (seed.observation_id, "initial")
    assert (latest_initial.parent_observation_id, latest_initial.observation_role) == ("explicit-parent", "initial")
    assert (later.parent_observation_id, later.observation_role, later.causal_order) == (
        latest_initial.observation_id,
        "later",
        3,
    )
    assert (other_operation.parent_observation_id, other_operation.observation_role, other_operation.causal_order) == (
        None,
        "standalone",
        4,
    )
    assert reopened == (seed, first_initial, latest_initial, later, other_operation)


def test_explicit_later_adopts_latest_initial_and_unscoped_fields_are_preserved(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    seed = store.append(evidence_kind="native_trusted", **_values())
    initial = store.append(evidence_kind="native_trusted", operation_id="op-a", **_values())
    later = store.append(
        evidence_kind="native_trusted",
        operation_id="op-a",
        observation_role="later",
        **{**_values(), "host_boot_id": "boot-b"},
    )
    explicit = store.append(
        evidence_kind="native_trusted",
        observation_role="later",
        parent_observation_id="caller-parent",
        **{**_values(), "host_boot_id": "boot-c"},
    )

    assert (later.parent_observation_id, later.observation_role) == (initial.observation_id, "later")
    assert (explicit.parent_observation_id, explicit.observation_role) == ("caller-parent", "later")
    assert ClockObservationStore(tmp_path / "device.json").records() == (seed, initial, later, explicit)


def test_zero_duration_host_intervals_are_valid_and_durable(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    values = {
        **_values(),
        "host_realtime_start": 1000.0,
        "host_realtime_end": 1000.0,
        "host_monotonic_start": 10.0,
        "host_monotonic_end": 10.0,
    }

    original = store.append(evidence_kind="native_trusted", **values)

    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host_realtime_start", True),
        ("host_realtime_start", 1789749501.0),
        ("host_realtime_end", math.nan),
        ("host_monotonic_start", math.inf),
        ("host_monotonic_end", False),
        ("host_monotonic_end", 9.0),
    ],
)
def test_invalid_host_intervals_leave_existing_ledger_unchanged(tmp_path: Path, field: str, value: object) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    changed = {**_values(), field: value}

    with pytest.raises(ClockObservationError):
        store.append(evidence_kind="native_trusted", **changed)

    assert ClockObservationStore(tmp_path / "device.json").records() == (original,)


def test_uint64_sequence_edges_and_effective_boundaries_survive_reopen(tmp_path: Path) -> None:
    maximum = (1 << 64) - 1
    store = ClockObservationStore(tmp_path / "device.json")
    minimum_edge = store.append(
        evidence_kind="native_trusted",
        effective_boundary_sequence=0,
        **{**_values(), "info_sequence_min": 0, "info_sequence_max": 0, "device_epoch": 0},
    )
    maximum_edge = store.append(
        evidence_kind="native_trusted",
        effective_boundary_sequence=maximum,
        **{**_values(), "info_sequence_min": 0, "info_sequence_max": maximum, "device_epoch": maximum},
    )

    assert ClockObservationStore(tmp_path / "device.json").records() == (minimum_edge, maximum_edge)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("info_sequence_min", -1),
        ("info_sequence_max", (1 << 64)),
        ("effective_boundary_sequence", -1),
        ("effective_boundary_sequence", 13),
    ],
)
def test_out_of_range_sequence_and_boundary_values_are_rejected(tmp_path: Path, field: str, value: object) -> None:
    values = {**_values(), field: value}

    with pytest.raises(ClockObservationError):
        ClockObservationStore(tmp_path / "device.json").append(evidence_kind="native_trusted", **values)
