from __future__ import annotations

import json
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

import omi_collector.capture.adapters.clock_corrections as clock_corrections_module
from omi_collector.capture.adapters.clock_corrections import (
    ClockCorrectionError,
    ClockCorrectionStore,
)
from omi_collector.capture.adapters.clock_observations import ClockObservation


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


def test_observation_store_append_returns_the_persisted_observation(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")

    appended = store.append(
        evidence_kind="native_trusted",
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1000,
        info_sequence_min=20,
        info_sequence_max=20,
    )

    assert isinstance(appended, ClockObservation)
    assert store.observation_store.records() == (appended,)
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").observation_store.records() == (
        appended,
    )


def test_mark_unresolved_is_idempotent_and_rejects_conflicting_or_terminal_intents(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    prepared = store.prepare(1100, 1000, 100.0, 20)
    path = tmp_path / "clock-corrections" / f"{prepared.operation_id}.json"
    prepared_bytes = path.read_bytes()
    conflicting = replace(prepared, drift_seconds=101.0)

    with pytest.raises(ClockCorrectionError, match="not prepared"):
        store.mark_unresolved(conflicting)

    assert path.read_bytes() == prepared_bytes
    unresolved = store.mark_unresolved(prepared)
    unresolved_bytes = path.read_bytes()

    assert store.mark_unresolved(unresolved) == unresolved
    assert path.read_bytes() == unresolved_bytes
    assert store.mark_unresolved(prepared) == unresolved
    assert path.read_bytes() == unresolved_bytes

    applied = store.finish(unresolved, state="applied", boundary_sequence_max=24, verified_epoch=1000)
    applied_bytes = path.read_bytes()
    with pytest.raises(ClockCorrectionError, match="not prepared"):
        store.mark_unresolved(unresolved)

    assert path.read_bytes() == applied_bytes
    assert store.records() == (applied,)


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


@pytest.mark.parametrize("state", ["prepared", "unresolved"])
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


def test_applied_correction_is_terminal_and_does_not_block_a_new_operation(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1100, 1000, 100.0, 20))
    store.finish(intent, state="applied", boundary_sequence_max=20, verified_epoch=1000)

    with pytest.raises(ClockCorrectionError, match="durable state"):
        store.finish(intent, state="resolved", boundary_sequence_max=20, verified_epoch=1000)

    restarted_store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    assert restarted_store.prepare(1200, 1000, 200.0, 24).state == "prepared"


def test_fresh_session_observation_retires_only_unclassifiable_uncertainty(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="old-session",
        host_boot_id="boot",
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
    same_session = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="old-session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=1320,
        info_sequence_min=20,
        info_sequence_max=24,
        operation_id=correction.operation_id,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )
    assert store.reconcile_causal_observation(same_session, near_zero_threshold=5.0) == ()
    assert store.records()[0].state == "unresolved"

    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    later = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="new-session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=3.0,
        host_monotonic_end=3.0,
        device_epoch=1320,
        info_sequence_min=20,
        info_sequence_max=24,
        operation_id=correction.operation_id,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )
    assert store.reconcile_causal_observation(later, near_zero_threshold=5.0) == ()
    assert store.records()[0].state == "unresolved"
    store.note_transport_closed()
    assert store.reconcile_causal_observation(later, near_zero_threshold=5.0) == ()

    later = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="next-session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=4.0,
        host_monotonic_end=4.0,
        device_epoch=1320,
        info_sequence_min=20,
        info_sequence_max=24,
        operation_id=correction.operation_id,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )

    reconciled = store.reconcile_causal_observation(later, near_zero_threshold=5.0)

    assert reconciled[0].state == "unknown"
    assert reconciled[0].boundary_sequence_max == 24
    assert reconciled[0].verified_epoch is None
    assert store.prepare(1320, 1000, 320.0, 24).state == "prepared"


@pytest.mark.parametrize(("epoch", "expected_state"), [(1000, "applied"), (1300, "not_applied")])
def test_cross_session_terminal_classification_requires_a_post_close_observation(
    tmp_path: Path, epoch: int, expected_state: str
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="old-session",
        host_boot_id="boot",
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

    def later_observation(monotonic: float) -> ClockObservation:
        return store.observation_store.append(
            evidence_kind="native_trusted",
            session_id="new-session",
            host_boot_id="boot",
            host_realtime_start=1000.0,
            host_realtime_end=1000.0,
            host_monotonic_start=monotonic,
            host_monotonic_end=monotonic,
            device_epoch=epoch,
            info_sequence_min=20,
            info_sequence_max=24,
            operation_id=correction.operation_id,
            observation_role="later",
            parent_observation_id=initial.observation_id,
        )

    before_close = later_observation(2.0)
    assert store.reconcile_causal_observation(before_close, near_zero_threshold=5.0) == ()
    assert store.records()[0].state == "unresolved"

    store.note_transport_closed()
    after_close = later_observation(3.0)
    reconciled = store.reconcile_causal_observation(after_close, near_zero_threshold=5.0)

    assert reconciled[0].state == expected_state
    assert store.prepare(1200, 1000, 200.0, 24).state == "prepared"


def test_later_near_zero_observation_marks_unresolved_write_applied(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))

    reconciled = store.reconcile_observation(1001, 0.5, 24, near_zero_threshold=5.0)

    assert reconciled[0].operation_id == intent.operation_id
    assert reconciled[0].state == "applied"
    assert reconciled[0].boundary_sequence_max == 24
    assert reconciled[0].verified_epoch == 1001


@pytest.mark.parametrize("has_observation_record", [False, True])
def test_reconcile_without_a_durable_observation_reference_is_a_noop(
    tmp_path: Path, has_observation_record: bool
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    correction_path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    correction_bytes = correction_path.read_bytes()
    if has_observation_record:
        store.observation_store.append(
            evidence_kind="native_trusted",
            session_id="session",
            host_boot_id="boot",
            host_realtime_start=1000.0,
            host_realtime_end=1000.0,
            host_monotonic_start=1.0,
            host_monotonic_end=1.0,
            device_epoch=1001,
            info_sequence_min=20,
            info_sequence_max=24,
            operation_id=intent.operation_id,
        )
        assert store.observation_store.records()

    assert (
        store.reconcile_observation(
            1001,
            0.5,
            24,
            near_zero_threshold=5.0,
            operation_id=intent.operation_id,
        )
        == ()
    )
    assert correction_path.read_bytes() == correction_bytes
    if has_observation_record:
        assert store.reconcile_observation(1001, 0.5, 24, near_zero_threshold=5.0) == ()
        assert correction_path.read_bytes() == correction_bytes
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (intent,)


@pytest.mark.parametrize("mismatch", ["epoch", "frontier"])
def test_reconcile_rejects_observation_reference_mismatch_without_mutating_correction(
    tmp_path: Path, mismatch: str
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    evidence = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1001,
        info_sequence_min=20,
        info_sequence_max=24,
        operation_id=intent.operation_id,
    )
    correction_path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    correction_bytes = correction_path.read_bytes()
    observed_epoch = 1002 if mismatch == "epoch" else evidence.device_epoch
    boundary_sequence_max = 25 if mismatch == "frontier" else evidence.info_sequence_max

    with pytest.raises(ClockCorrectionError, match="reference conflicts"):
        store.reconcile_observation(
            observed_epoch,
            0.5,
            boundary_sequence_max,
            near_zero_threshold=5.0,
            observation_id=evidence.observation_id,
            operation_id=intent.operation_id,
        )

    assert correction_path.read_bytes() == correction_bytes
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (intent,)


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
    assert store.records()[0].state == "applied"
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


@pytest.mark.parametrize(
    ("frontier", "effective_boundary", "threshold", "drift"),
    [
        (0, 0, 5.0, 0.0),
        (20, 20, 0.25, 0.0),
        (24, 24, 5.0, 0.0),
    ],
)
def test_reconcile_accepts_valid_boundary_and_threshold_edges(
    tmp_path: Path, frontier: int, effective_boundary: int, threshold: float, drift: float
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, frontier))
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1300,
        info_sequence_min=frontier,
        info_sequence_max=frontier,
        operation_id=intent.operation_id,
        observation_role="initial",
    )
    later = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=1000,
        info_sequence_min=frontier,
        info_sequence_max=frontier,
        operation_id=intent.operation_id,
        effective_boundary_sequence=effective_boundary,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )

    reconciled = store.reconcile_observation(
        1000,
        drift,
        frontier,
        near_zero_threshold=threshold,
        effective_boundary_sequence=effective_boundary,
        observation_id=later.observation_id,
        operation_id=intent.operation_id,
    )

    expected = reconciled[0]
    assert expected.state == "applied"
    assert expected.boundary_sequence_min == frontier
    assert expected.boundary_sequence_max == effective_boundary
    assert expected.verified_epoch == 1000
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (expected,)


@pytest.mark.parametrize(
    ("frontier", "effective_boundary", "threshold"),
    [(24, None, 0.0), (24, None, -0.25), (-1, None, 5.0), (24, -1, 5.0), (24, 25, 5.0)],
)
def test_reconcile_rejects_invalid_inputs_without_changing_durable_correction(
    tmp_path: Path, frontier: int, effective_boundary: int | None, threshold: float
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    original_bytes = path.read_bytes()

    with pytest.raises(ValueError):
        store.reconcile_observation(
            1000,
            0.0,
            frontier,
            near_zero_threshold=threshold,
            effective_boundary_sequence=effective_boundary,
        )

    assert path.read_bytes() == original_bytes
    assert store.records() == (intent,)
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (intent,)


@pytest.mark.parametrize(("start", "drift"), [(0, 0.0), (-1, 0.0), (20, float("inf"))])
def test_prepare_boundary_and_finite_drift_validation(tmp_path: Path, start: int, drift: float) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")

    if start == 0 and drift == 0.0:
        correction = store.prepare(1100, 1000, drift, start)
        assert correction.boundary_sequence_min == 0
        assert store.records() == (correction,)
    else:
        with pytest.raises(ClockCorrectionError):
            store.prepare(1100, 1000, drift, start)
        assert store.records() == ()


def test_finish_is_idempotent_only_for_identical_durable_completion(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.mark_unresolved(store.prepare(1100, 1000, 100.0, 20))
    applied = store.finish(correction, state="applied", boundary_sequence_max=24, verified_epoch=1000)
    path = tmp_path / "clock-corrections" / f"{correction.operation_id}.json"
    applied_bytes = path.read_bytes()

    assert store.finish(applied, state="applied", boundary_sequence_max=24, verified_epoch=1000) == applied
    assert path.read_bytes() == applied_bytes
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (applied,)

    with pytest.raises(ClockCorrectionError):
        store.finish(applied, state="resolved", boundary_sequence_max=24, verified_epoch=1000)
    assert path.read_bytes() == applied_bytes
    assert store.records() == (applied,)


@pytest.mark.parametrize(("boundary", "valid"), [(20, True), (19, False)])
def test_unresolved_zero_width_resolved_boundary(tmp_path: Path, boundary: int, valid: bool) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.mark_unresolved(store.prepare(1100, 1000, 100.0, 20))

    if valid:
        resolved = store.finish(correction, state="resolved", boundary_sequence_max=boundary, verified_epoch=1000)
        assert resolved.state == "resolved"
        assert resolved.boundary_sequence_min == resolved.boundary_sequence_max == boundary
        assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (resolved,)
    else:
        with pytest.raises(ClockCorrectionError, match="boundary"):
            store.finish(correction, state="resolved", boundary_sequence_max=boundary, verified_epoch=1000)
        assert store.records() == (correction,)


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
    "changes",
    [
        pytest.param({"boundary_sequence_min": -1}, id="negative-boundary-min"),
        pytest.param({"observed_epoch": True}, id="boolean-epoch"),
        pytest.param({"operation_id": ""}, id="empty-operation-id"),
        pytest.param({"drift_seconds": True}, id="boolean-drift"),
        pytest.param({"state": "mystery"}, id="unknown-state"),
        pytest.param({"state": "unknown"}, id="unknown-missing-boundary"),
        pytest.param(
            {"state": "unknown", "boundary_sequence_max": 20, "verified_epoch": 1000},
            id="unknown-with-verified-epoch",
        ),
    ],
)
def test_malformed_durable_correction_fails_closed_without_rewriting_bytes(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    correction = store.prepare(1300, 1000, 300.0, 20)
    path = tmp_path / "clock-corrections" / f"{correction.operation_id}.json"
    value = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
    value.update(changes)
    path.write_text(json.dumps(value), encoding="utf-8")
    corrupted_bytes = path.read_bytes()

    with pytest.raises(ClockCorrectionError, match="invalid"):
        store.records()

    assert path.read_bytes() == corrupted_bytes


def test_zero_minimum_boundary_can_finish_as_not_written(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    prepared = store.prepare(1100, 1000, 100.0, 0)
    completed = store.finish(prepared, state="not_written", boundary_sequence_max=None, verified_epoch=None)

    assert completed.state == "not_written"
    assert completed.boundary_sequence_min == 0
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (completed,)


@pytest.mark.parametrize(
    ("state", "boundary_sequence_max", "verified_epoch"),
    [
        ("applied", 24, None),
        ("applied", None, 1000),
        ("unresolved", 24, None),
        ("unknown", None, None),
        ("not_applied", 24, 1000),
    ],
)
def test_finish_rejects_each_invalid_completion_field_without_mutating_intent(
    tmp_path: Path, state: str, boundary_sequence_max: int | None, verified_epoch: int | None
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20))
    path = tmp_path / "clock-corrections" / f"{intent.operation_id}.json"
    original_bytes = path.read_bytes()

    with pytest.raises(ClockCorrectionError):
        store.finish(
            intent,
            state=state,
            boundary_sequence_max=boundary_sequence_max,
            verified_epoch=verified_epoch,
        )

    assert path.read_bytes() == original_bytes
    assert ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts").records() == (intent,)


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


def test_concurrent_identical_prepare_returns_the_same_durable_correction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    correction_root = tmp_path / "clock-corrections"
    correction_root.mkdir()
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    lock_open_barrier = threading.Barrier(2)
    original_open = Path.open

    def wait_until_both_prepares_reach_the_lock(path: Path, *args: object, **kwargs: object) -> object:
        if path == correction_root / ".lock":
            lock_open_barrier.wait(timeout=2)
        return cast(Callable[..., object], original_open)(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", wait_until_both_prepares_reach_the_lock)
    with ThreadPoolExecutor(max_workers=2) as callers:
        futures = [
            callers.submit(store.prepare, 1300, 1000, 300.0, 20, operation_id="same-operation")
            for _ in range(2)
        ]
        try:
            results = [future.result(timeout=3) for future in futures]
        finally:
            lock_open_barrier.abort()
    assert len(results) == 2
    assert results[0] == results[1]
    assert len(store.records()) == 1
    assert store.records()[0] == results[0]


def _fail_clock_correction_directory_fsync(
    monkeypatch: pytest.MonkeyPatch, correction_root: Path
) -> OSError:
    original_open = os.open
    original_fsync = os.fsync
    correction_directory_descriptors: set[int] = set()
    sentinel = OSError("clock correction directory fsync sentinel")

    def track_directory_open(path: str | os.PathLike[str], flags: int, *args: int) -> int:
        descriptor = original_open(path, flags, *args)
        if Path(path) == correction_root:
            correction_directory_descriptors.add(descriptor)
        return descriptor

    def fail_only_for_correction_directory(descriptor: int) -> None:
        if descriptor in correction_directory_descriptors:
            raise sentinel
        original_fsync(descriptor)

    monkeypatch.setattr(clock_corrections_module.os, "open", track_directory_open)
    monkeypatch.setattr(clock_corrections_module.os, "fsync", fail_only_for_correction_directory)
    return sentinel


def test_prepare_preserves_directory_fsync_error_after_atomic_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    correction_root = tmp_path / "clock-corrections"
    correction_root.mkdir()
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    sentinel = _fail_clock_correction_directory_fsync(monkeypatch, correction_root)

    with pytest.raises(ClockCorrectionError, match="intent is not durable") as raised:
        store.prepare(1300, 1000, 300.0, 20, operation_id="prepare-fsync")

    assert raised.value.__cause__ is sentinel
    path = correction_root / "prepare-fsync.json"
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "prepared"
    assert list(correction_root.glob(".*.tmp")) == []
    assert store.records()[0].operation_id == "prepare-fsync"


def test_finish_preserves_directory_fsync_error_after_atomic_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20, operation_id="finish-fsync"))
    correction_root = tmp_path / "clock-corrections"
    sentinel = _fail_clock_correction_directory_fsync(monkeypatch, correction_root)

    with pytest.raises(ClockCorrectionError, match="result is not durable") as raised:
        store.finish(intent, state="applied", boundary_sequence_max=24, verified_epoch=1000)

    assert raised.value.__cause__ is sentinel
    path = correction_root / "finish-fsync.json"
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "applied"
    assert list(correction_root.glob(".*.tmp")) == []
    assert store.records()[0].state == "applied"


def _append_native_observation(store: ClockCorrectionStore, values: dict[str, object]) -> ClockObservation:
    return store.observation_store.append(
        evidence_kind="native_trusted",
        session_id=cast(str, values["session_id"]),
        host_boot_id="host-boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=cast(int, values["device_epoch"]),
        info_sequence_min=20,
        info_sequence_max=cast(int, values["info_sequence_max"]),
        operation_id=cast(str | None, values.get("operation_id")),
        observation_role=cast(str, values.get("observation_role", "standalone")),
        parent_observation_id=cast(str | None, values.get("parent_observation_id")),
    )


@pytest.mark.parametrize(
    ("other_operation_sequence", "should_settle_current"),
    [(20, False), (22, False), (24, False), (25, True)],
    ids=("lower-boundary", "interior", "upper-boundary", "outside-range"),
)
def test_causal_reconcile_preserves_another_valid_unresolved_operation(
    tmp_path: Path, other_operation_sequence: int, should_settle_current: bool
) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    current = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 20, operation_id="current-operation"))

    other_root = tmp_path / "other"
    other_store = ClockCorrectionStore(other_root / "device.json", other_root / "attempts")
    other = other_store.mark_unresolved(
        other_store.prepare(
            1300,
            1000,
            300.0,
            other_operation_sequence,
            operation_id="other-operation",
        )
    )
    assert other.state == "unresolved"
    other_record = other_root / "clock-corrections" / "other-operation.json"
    (tmp_path / "clock-corrections" / "other-operation.json").write_bytes(other_record.read_bytes())
    initial = _append_native_observation(store, {
        "session_id": "same-session",
        "device_epoch": 1300,
        "info_sequence_max": 20,
        "operation_id": current.operation_id,
        "observation_role": "initial",
    })
    later = _append_native_observation(store, {
        "session_id": "same-session",
        "device_epoch": 1000,
        "info_sequence_max": 24,
        "operation_id": current.operation_id,
        "observation_role": "later",
        "parent_observation_id": initial.observation_id,
    })
    current_bytes = (tmp_path / "clock-corrections" / "current-operation.json").read_bytes()
    other_bytes = (tmp_path / "clock-corrections" / "other-operation.json").read_bytes()

    reconciled = store.reconcile_causal_observation(later, near_zero_threshold=5.0)

    states = {item.operation_id: item.state for item in store.records()}
    if should_settle_current:
        assert tuple(item.operation_id for item in reconciled) == ("current-operation",)
        assert states == {"current-operation": "applied", "other-operation": "unresolved"}
    else:
        assert reconciled == ()
        assert states == {"current-operation": "unresolved", "other-operation": "unresolved"}
        assert (tmp_path / "clock-corrections" / "current-operation.json").read_bytes() == current_bytes
        assert (tmp_path / "clock-corrections" / "other-operation.json").read_bytes() == other_bytes


def test_standalone_native_parent_can_support_cross_session_reconciliation(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    parent = _append_native_observation(store, {
        "session_id": "parent-session",
        "device_epoch": 1000,
        "info_sequence_max": 20,
    })
    current = store.mark_unresolved(store.prepare(1100, 1000, 100.0, 20, operation_id="current-operation"))
    later = _append_native_observation(store, {
        "session_id": "later-session",
        "device_epoch": 1000,
        "info_sequence_max": 24,
        "operation_id": current.operation_id,
        "observation_role": "later",
        "parent_observation_id": parent.observation_id,
    })

    reconciled = store.reconcile_causal_observation(later, near_zero_threshold=5.0)

    assert tuple(item.operation_id for item in reconciled) == ("current-operation",)
    assert reconciled[0].state == "applied"
    assert reconciled[0].boundary_sequence_max == 24
