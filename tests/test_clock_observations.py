from __future__ import annotations

import json
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from threading import Barrier
from typing import IO

import pytest

from omi_collector.capture.adapters import clock_observations
from omi_collector.capture.adapters.clock_observations import ClockObservationError, ClockObservationStore
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap, segments_with_estimates

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


def test_observation_keeps_its_validated_epoch_for_estimation(tmp_path: Path) -> None:
    observation = ClockObservationStore(tmp_path / "device.json").append(evidence_kind="native_trusted", **_values())

    field = "device_epoch"
    with pytest.raises(FrozenInstanceError):
        setattr(observation, field, observation.device_epoch + 1)

    mapping = segments_with_estimates((observation,), ClockSegmentMap(()))

    assert mapping.utc_for(observation.info_sequence_min, 1789749500) == pytest.approx(1789749500.1)
    assert mapping.utc_for(observation.info_sequence_max, 1789749500) is None


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


def test_store_creates_a_new_nested_device_state_directory(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "new" / "nested" / "device.json")

    original = store.append(evidence_kind="native_trusted", **_values())

    assert store.records() == (original,)
    assert (tmp_path / "new" / "nested" / "clock-observations" / f"{original.observation_id}.json").is_file()


def test_first_append_rejects_parent_directory_sync_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sync_attempts: list[Path] = []

    def fail_sync(path: Path) -> None:
        sync_attempts.append(path)
        raise OSError("directory sync failed")

    monkeypatch.setattr(clock_observations, "_sync_directory", fail_sync)
    store = ClockObservationStore(tmp_path / "new" / "device.json")

    with pytest.raises(ClockObservationError, match="directory is not durable"):
        store.append(evidence_kind="native_trusted", **_values())

    assert sync_attempts == [tmp_path / "new"]
    assert not tuple((tmp_path / "new" / "clock-observations").glob("*.json"))


def test_concurrent_public_appends_repair_causal_order_after_initial_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    barrier = Barrier(2)
    original_causal_fields = clock_observations._causal_fields

    def synchronize_initial_reads(*args: object, **kwargs: object) -> tuple[str | None, str]:
        barrier.wait(timeout=5)
        return original_causal_fields(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(clock_observations, "_causal_fields", synchronize_initial_reads)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(store.append, evidence_kind="native_trusted", **_values()) for _ in range(2)]
        appended = [future.result(timeout=10) for future in futures]

    reopened = ClockObservationStore(tmp_path / "device.json").records()
    assert sorted(item.causal_order for item in appended) == [0, 1]
    assert [item.causal_order for item in reopened] == [0, 1]
    assert {item.observation_id for item in reopened} == {item.observation_id for item in appended}


def test_temporary_file_creation_failure_uses_typed_store_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_open = Path.open

    def fail_temporary_open(  # noqa: PLR0913, PLR0917 - mirrors pathlib.Path.open for fault injection.
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> IO:
        if mode == "xb" and path.name.endswith(".tmp"):
            raise FileNotFoundError("temporary directory disappeared")
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", fail_temporary_open)
    store = ClockObservationStore(tmp_path / "device.json")

    with pytest.raises(ClockObservationError, match="is not durable"):
        store.append(evidence_kind="native_trusted", **_values())

    assert not tuple((tmp_path / "clock-observations").glob("*.json"))


def test_records_rejects_noncanonical_bytes_with_valid_causal_order(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    item = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{item.observation_id}.json"
    document = json.loads(path.read_text())
    assert document["causal_order"] == 0
    path.write_text(json.dumps(document, sort_keys=True) + "\n")

    with pytest.raises(ClockObservationError, match="ledger is invalid"):
        store.records()


def test_identity_text_at_maximum_length_survives_reopen(tmp_path: Path) -> None:
    session_id = "s" * 256
    store = ClockObservationStore(tmp_path / "device.json")

    original = store.append(evidence_kind="native_trusted", **{**_values(), "session_id": session_id})

    (reopened,) = ClockObservationStore(tmp_path / "device.json").records()
    assert reopened == original
    assert reopened.session_id == session_id


@pytest.mark.parametrize(
    ("field", "value"),
    [("session_id", ""), ("host_boot_id", ""), ("session_id", False), ("observation_id", 1)],
)
def test_invalid_identity_text_is_rejected_without_changing_ledger(tmp_path: Path, field: str, value: object) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError):
        store.append(evidence_kind="native_trusted", **{**_values(), field: value})

    assert path.read_bytes() == before
    assert store.records() == (original,)


def test_observation_rejects_negative_constructor_causal_order() -> None:
    with pytest.raises(ClockObservationError, match="causal order is invalid"):
        clock_observations.ClockObservation(
            version=1,
            observation_id="invalid-order",
            causal_order=-1,
            evidence_kind="native_trusted",
            **_values(),
        )


def test_observation_rejects_boolean_constructor_causal_order() -> None:
    with pytest.raises(ClockObservationError, match="causal order is invalid"):
        clock_observations.ClockObservation(
            version=1,
            observation_id="boolean-order",
            causal_order=True,
            evidence_kind="native_trusted",
            **_values(),
        )


def test_empty_operation_reference_is_rejected_without_changing_ledger(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError, match="operation reference is invalid"):
        store.append(evidence_kind="native_trusted", operation_id="", **_values())

    assert path.read_bytes() == before
    assert store.records() == (original,)


def test_empty_explicit_parent_reference_is_rejected_by_public_append(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")

    with pytest.raises(ClockObservationError, match="parent reference is invalid"):
        store.append(
            evidence_kind="native_trusted",
            observation_role="later",
            parent_observation_id="",
            **_values(),
        )

    assert store.records() == ()


def test_boolean_effective_boundary_is_rejected_without_changing_ledger(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    original = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{original.observation_id}.json"
    before = path.read_bytes()

    with pytest.raises(ClockObservationError, match="effective boundary is invalid"):
        store.append(evidence_kind="native_trusted", effective_boundary_sequence=True, **_values())

    assert path.read_bytes() == before
    assert store.records() == (original,)


def test_later_operation_does_not_adopt_foreign_initial_parent(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    foreign_initial = store.append(
        evidence_kind="native_trusted", operation_id="op-a", observation_role="initial", **_values()
    )

    later = store.append(
        evidence_kind="native_trusted",
        operation_id="op-b",
        observation_role="later",
        **{**_values(), "host_boot_id": "boot-b"},
    )

    assert foreign_initial.observation_role == "initial"
    assert later.operation_id == "op-b"
    assert later.observation_role == "later"
    assert later.parent_observation_id is None
    assert ClockObservationStore(tmp_path / "device.json").records() == (foreign_initial, later)


def test_escaped_unicode_canonical_record_survives_reopen(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    item = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{item.observation_id}.json"
    document = json.loads(path.read_text())
    document["session_id"] = "sesión-雪"
    path.write_bytes((json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode())

    (reopened,) = store.records()

    assert reopened.session_id == "sesión-雪"
    assert reopened.causal_order == 0
