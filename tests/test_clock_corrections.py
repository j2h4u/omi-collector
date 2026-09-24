from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.clock_corrections import (
    ClockCorrectionError,
    ClockCorrectionStore,
)


def test_intent_is_durable_before_confirmation(tmp_path: Path) -> None:
    state = tmp_path / "device.json"
    store = ClockCorrectionStore(state, tmp_path / "attempts")
    intent = store.prepare(1360, 1000, 360.2, 20)

    prepared = cast(
        dict[str, object],
        json.loads((tmp_path / "clock-corrections" / f"{intent.operation_id}.json").read_text()),
    )
    assert prepared["state"] == "prepared"
    assert (tmp_path / "clock-corrections" / f"{intent.operation_id}.json").stat().st_mode & 0o777 == 0o600

    intent = store.mark_unresolved(intent)
    store.finish(intent, state="applied", boundary_sequence_max=22, verified_epoch=1001)
    assert store.confirmed()[0].state == "applied"


def test_active_attempt_prevents_clock_write_but_retired_attempt_does_not(tmp_path: Path) -> None:
    attempts = tmp_path / "attempts"
    active = attempts / "active"
    active.mkdir(parents=True)
    store = ClockCorrectionStore(tmp_path / "device.json", attempts)

    with pytest.raises(ClockCorrectionError, match="active audio attempt"):
        store.prepare(1100, 1000, 100.0, 20)

    (active / "terminal-retired.json").write_text("{}", encoding="utf-8")
    assert store.prepare(1100, 1000, 100.0, 20).state == "prepared"


def test_unresolved_clock_write_is_not_confirmed(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare(1100, 1000, 100.0, 20)
    intent = store.mark_unresolved(intent)
    store.finish(intent, state="unresolved", boundary_sequence_max=None, verified_epoch=None)

    assert store.confirmed() == ()


def test_unresolved_nonzero_boundary_cannot_be_resolved_directly(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1100, 1000, 100.0, 20))

    with pytest.raises(ClockCorrectionError, match="ambiguous boundary"):
        store.finish(intent, state="resolved", boundary_sequence_max=22, verified_epoch=1001)


def test_prepared_intent_recovers_as_not_written(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare(1100, 1000, 100.0, 20)

    recovered = store.recover_prepared()

    assert recovered[0].operation_id == intent.operation_id
    assert recovered[0].state == "not_written"
    assert store.records()[0].state == "not_written"


def test_startup_replay_ignores_legacy_anchored_monotonic_observations(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    initial = store.observation_store.append(
        evidence_kind="anchored_monotonic",
        session_id="legacy",
        host_boot_id="host-boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1300,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=correction.operation_id,
        observation_role="initial",
    )
    store.observation_store.append(
        evidence_kind="anchored_monotonic",
        session_id="legacy",
        host_boot_id="host-boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=1000,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=correction.operation_id,
        effective_boundary_sequence=20,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )

    assert store.reconcile_recovered_observations(near_zero_threshold=5.0) == ()
    assert store.records()[0].state == "unresolved"


@pytest.mark.parametrize("state", ["prepared", "unresolved", "applied"])
def test_prepare_rejects_any_pending_correction(tmp_path: Path, state: str) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare(1100, 1000, 100.0, 20)
    if state == "unresolved":
        intent = store.mark_unresolved(intent)
    elif state == "applied":
        intent = store.mark_unresolved(intent)
        store.finish(intent, state="applied", boundary_sequence_max=20, verified_epoch=1000)

    with pytest.raises(ClockCorrectionError, match="pending correction"):
        store.prepare(1200, 1000, 200.0, 24)


def test_later_near_zero_observation_marks_unresolved_write_applied(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))

    reconciled = store.reconcile_observation(1001, 0.5, 24, near_zero_threshold=5.0)

    assert reconciled[0].operation_id == intent.operation_id
    assert reconciled[0].state == "applied"
    assert reconciled[0].boundary_sequence_max == 24
    assert reconciled[0].verified_epoch == 1001


def test_reconcile_waits_until_observation_reaches_pending_boundary(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))

    assert store.reconcile_observation(1001, 0.5, 19, near_zero_threshold=5.0) == ()
    assert next(
        correction for correction in store.records() if correction.operation_id == intent.operation_id
    ).state == ("unresolved")


def test_reconcile_does_not_attribute_near_zero_to_earlier_unresolved_operation(
    tmp_path: Path,
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    later = cast(dict[str, object], json.loads(path.read_text()))
    later.update(
        operation_id="b" * 32,
        state="resolved",
        boundary_sequence_min=30,
        boundary_sequence_max=35,
        verified_epoch=1000,
    )
    (tmp_path / "clock-corrections" / f"{'b' * 32}.json").write_text(json.dumps(later))

    assert store.reconcile_observation(1001, 0.5, 40, near_zero_threshold=5.0) == ()
    assert next(
        correction for correction in store.records() if correction.operation_id == intent.operation_id
    ).state == ("unresolved")


@pytest.mark.parametrize("later_state", ["unresolved", "resolved"])
def test_reconcile_requires_one_unambiguous_pending_operation(tmp_path: Path, later_state: str) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    later = cast(dict[str, object], json.loads(path.read_text()))
    later.update(
        operation_id="b" * 32,
        state=later_state,
        boundary_sequence_min=20,
        boundary_sequence_max=24 if later_state == "resolved" else None,
        verified_epoch=1000 if later_state == "resolved" else None,
    )
    (tmp_path / "clock-corrections" / f"{'b' * 32}.json").write_text(json.dumps(later))

    assert store.reconcile_observation(1001, 0.5, 24, near_zero_threshold=5.0) == ()
    assert all(correction.state in {"unresolved", later_state} for correction in store.records())


def test_later_matching_drift_observation_marks_unresolved_write_not_applied(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))

    reconciled = store.reconcile_observation(1301, 300.0, 24, near_zero_threshold=5.0)

    assert reconciled[0].operation_id == intent.operation_id
    assert reconciled[0].state == "not_applied"
    assert reconciled[0].boundary_sequence_max == 24
    assert reconciled[0].verified_epoch is None


def test_causal_observation_reconciles_by_typed_host_interval(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    store.observation_store.append(
        evidence_kind="native_trusted",
        observation_id="9" * 32,
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1300,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=intent.operation_id,
        observation_role="initial",
    )
    later = store.observation_store.append(
        evidence_kind="native_trusted",
        observation_id="a" * 32,
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1000,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=intent.operation_id,
        effective_boundary_sequence=20,
        observation_role="later",
    )
    result = store.reconcile_causal_observation(later, near_zero_threshold=5.0)
    assert result[0].state == "applied"
    assert store.reconcile_causal_observation(later, near_zero_threshold=5.0) == ()


def test_causal_observation_allows_ordered_successive_same_boundary_operation(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    first = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20, operation_id="first"))
    store.finish(first, state="applied", boundary_sequence_max=20, verified_epoch=1000)
    applied = store.records()[0]
    store.finish(
        applied,
        state="resolved",
        boundary_sequence_max=applied.boundary_sequence_max,
        verified_epoch=applied.verified_epoch,
    )
    second = store.mark_unresolved(store.prepare(1301, 1000, 301.0, 20, operation_id="second"))
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
        observation_id="c" * 32,
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1301,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=second.operation_id,
        observation_role="initial",
    )
    later = store.observation_store.append(
        evidence_kind="native_trusted",
        observation_id="d" * 32,
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1001.0,
        host_realtime_end=1001.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=1001,
        info_sequence_min=20,
        info_sequence_max=20,
        operation_id=second.operation_id,
        effective_boundary_sequence=20,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )

    result = store.reconcile_causal_observation(later, near_zero_threshold=5.0)

    assert result[0].operation_id == second.operation_id
    assert result[0].state == "applied"


def test_clock_correction_schema_is_strict(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare(1300, 1000, 300.0, 20)
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    value = cast(dict[str, object], json.loads(path.read_text()))
    value["version"] = 1
    path.write_text(json.dumps(value))

    with pytest.raises(ClockCorrectionError, match="invalid"):
        store.records()


@pytest.mark.parametrize(
    ("state", "boundary_sequence_max", "verified_epoch"),
    [
        ("bogus", None, None),
        ("unresolved", 24, None),
        ("resolved", None, 1000),
        ("applied", 24, None),
    ],
)
def test_clock_correction_state_fields_are_strict(
    tmp_path: Path, state: str, boundary_sequence_max: int | None, verified_epoch: int | None
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare(1300, 1000, 300.0, 20)
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    value = cast(dict[str, object], json.loads(path.read_text()))
    value.update(state=state, boundary_sequence_max=boundary_sequence_max, verified_epoch=verified_epoch)
    path.write_text(json.dumps(value))

    with pytest.raises(ClockCorrectionError, match="invalid"):
        store.records()
