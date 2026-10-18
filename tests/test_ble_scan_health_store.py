from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn, cast

import pytest

import omi_collector.capture.adapters.ble_scan_health_store as store_module
from omi_collector.capture.adapters.ble_scan_diagnostics import ScanDiagnostics as HciScanDiagnostics
from omi_collector.capture.adapters.ble_scan_health_store import BleScanIncidentStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind
from omi_collector.capture.application.ble_scan_health import ScanIncidentContext
from omi_collector.operator_status import _ble_scan_incident_summary


class _FakeBus:
    def __init__(self, reply: object | None = None, *, block_connect: asyncio.Event | None = None) -> None:
        self.reply = reply
        self.block_connect = block_connect
        self.connected = asyncio.Event()
        self.disconnected = False

    async def connect(self) -> None:
        self.connected.set()
        if self.block_connect is not None:
            await self.block_connect.wait()

    async def call(self, _message: object) -> object:
        assert self.reply is not None
        return self.reply

    def disconnect(self) -> None:
        self.disconnected = True


class _FakeHciWorker:
    adapter = "hci0"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def snapshot(self) -> dict[str, object]:
        return {"status": "available", "counts": {"legacy_advertising": 3}}


class _Diagnostics:
    def __init__(self, state_db: StateDB) -> None:
        self.state_db = state_db
        self.snapshots = 0

    async def snapshot(self, generation: int) -> dict[str, object]:
        self.snapshots += 1
        return {"generation": generation, "hci": {"status": "degraded"}}


def _context(generation: int) -> ScanIncidentContext:
    return ScanIncidentContext(
        generation=generation,
        reason="scan_silence",
        active_seconds=180.0,
        bleak_callbacks=0,
        last_callback_age_seconds=None,
        threshold_seconds=180.0,
    )


def test_incident_is_snapshotted_before_write_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    diagnostics = _Diagnostics(db)
    store = BleScanIncidentStore(db, diagnostics)  # type: ignore[arg-type]

    async def run() -> None:
        await store.open(_context(1))
        await store.open(_context(2))

    asyncio.run(run())
    incident = db.get(StateKind.BLE_SCAN_INCIDENT, "open")
    assert diagnostics.snapshots == 2
    assert incident is not None
    assert incident["context"] == {
        "generation": 1,
        "reason": "scan_silence",
        "active_seconds": 180.0,
        "bleak_callbacks": 0,
        "last_callback_age_seconds": None,
        "threshold_seconds": 180.0,
        "error_kind": None,
    }
    assert incident["diagnostics"] == {"generation": 1, "hci": {"status": "degraded"}}
    summary = _ble_scan_incident_summary(StateDB(path, read_only=True))
    assert summary["active"] is True
    assert summary["fault_category"] == "host_ble_receive_unavailable"
    assert "diagnostics" not in summary

    reopened = StateDB(path)
    restarted_store = BleScanIncidentStore(reopened, _Diagnostics(reopened))  # type: ignore[arg-type]
    assert asyncio.run(restarted_store.active())
    assert asyncio.run(restarted_store.resolve(3, "advertisements_resumed")) is None
    assert db.get(StateKind.BLE_SCAN_INCIDENT, "open") is None
    history = db.get(StateKind.BLE_SCAN_INCIDENT, "history")
    assert history is not None
    resolved = history["entries"][0]  # type: ignore[index]
    assert resolved["detected_at"] == incident["detected_at"]  # type: ignore[index]
    assert resolved["context"] == incident["context"]
    assert resolved["diagnostics"] == incident["diagnostics"]
    assert resolved["resolution"] == "advertisements_resumed"  # type: ignore[index]


def test_resolved_history_is_bounded(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    store = BleScanIncidentStore(db, _Diagnostics(db))  # type: ignore[arg-type]

    async def run() -> None:
        for generation in range(10):
            await store.open(_context(generation))
            await store.resolve(generation, "recovered")

    asyncio.run(run())
    history = db.get(StateKind.BLE_SCAN_INCIDENT, "history")
    assert history is not None
    assert len(history["entries"]) == 8  # type: ignore[arg-type]


def test_retry_keeps_first_context_snapshot_and_detection_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    diagnostics = _Diagnostics(db)
    store = BleScanIncidentStore(db, diagnostics)  # type: ignore[arg-type]
    real_open = store._open
    attempts = 0

    def fail_once(context: ScanIncidentContext, snapshot: dict[str, object], detected_at: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise StateDBError("injected write failure")
        return real_open(context, snapshot, detected_at)

    monkeypatch.setattr(store, "_open", fail_once)
    monkeypatch.setattr(store_module, "_now", lambda: "2026-10-18T12:00:00+00:00")

    with pytest.raises(StateDBError, match="injected write failure"):
        asyncio.run(store.open(_context(1)))
    asyncio.run(store.open(_context(2)))

    incident = db.get(StateKind.BLE_SCAN_INCIDENT, "open")
    assert incident is not None
    assert incident["context"]["generation"] == 1  # type: ignore[index]
    assert incident["diagnostics"] == {"generation": 1, "hci": {"status": "degraded"}}
    assert incident["detected_at"] == "2026-10-18T12:00:00+00:00"
    assert diagnostics.snapshots == 1


def test_facade_reads_selected_bluez_adapter_and_scopes_device_count(monkeypatch: pytest.MonkeyPatch) -> None:
    objects = {
        "/org/bluez/hci0": {
            "org.bluez.Adapter1": {"Powered": SimpleNamespace(value=True), "Discovering": SimpleNamespace(value=True)}
        },
        "/org/bluez/hci0/dev_A": {"org.bluez.Device1": {}},
        "/org/bluez/hci0/dev_B": {"org.bluez.Device1": {}},
        "/org/bluez/hci1": {"org.bluez.Adapter1": {"Powered": False, "Discovering": False}},
        "/org/bluez/hci1/dev_C": {"org.bluez.Device1": {}},
        "/org/bluez/hci10/dev_D": {"org.bluez.Device1": {}},
    }
    bus = _FakeBus(SimpleNamespace(message_type=SimpleNamespace(name="METHOD_RETURN"), body=[objects]))
    monkeypatch.setattr(store_module, "MessageBus", lambda **_kwargs: bus)
    facade = store_module.ScanDiagnosticsFacade("hci0", worker=cast(HciScanDiagnostics, _FakeHciWorker()))

    async def run() -> dict[str, object]:
        await facade.prepare(4)
        return await facade.snapshot(4)

    result = asyncio.run(run())

    assert result["bluez"] == {
        "available": True,
        "powered": True,
        "discovering": True,
        "devices_count": 2,
        "failure": None,
    }
    assert bus.disconnected


def test_bluez_dbus_error_is_an_explicit_unavailable_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = _FakeBus(SimpleNamespace(message_type=SimpleNamespace(name="ERROR"), body=["org.bluez.Error.Failed"]))
    monkeypatch.setattr(store_module, "MessageBus", lambda **_kwargs: bus)

    result = asyncio.run(store_module._bluez_snapshot("hci0"))

    assert result == store_module._unavailable("dbus_error")
    assert bus.disconnected


def test_permission_and_timeout_diagnostics_do_not_block_incident_persistence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def no_raw_permission(*_args: object, **_kwargs: object) -> NoReturn:
        raise PermissionError("HCI observation not permitted")

    async def bluez_timeout(_adapter: str) -> dict[str, object]:
        raise TimeoutError

    monkeypatch.setattr(store_module, "_bluez_snapshot", bluez_timeout)
    worker = HciScanDiagnostics("hci0", socket_factory=no_raw_permission)
    diagnostics = store_module.ScanDiagnosticsFacade("hci0", worker=worker)
    db = StateDB(tmp_path / "state.sqlite3")
    store = BleScanIncidentStore(db, diagnostics)

    async def run() -> None:
        await diagnostics.prepare(9)
        snapshot = await diagnostics.snapshot(9)
        assert snapshot["hci"]["status"] == "degraded"  # type: ignore[index]
        assert snapshot["hci"]["failure"] == "cap_net_raw_unavailable"  # type: ignore[index]
        assert snapshot["hci"]["filter"]["available"] is False  # type: ignore[index]
        assert snapshot["bluez"] == store_module._unavailable("timeout")
        await store.open(_context(9))
        await diagnostics.close(9)

    asyncio.run(run())

    incident = db.get(StateKind.BLE_SCAN_INCIDENT, "open")
    assert incident is not None
    assert incident["diagnostics"]["hci"]["failure"] == "cap_net_raw_unavailable"  # type: ignore[index]
    assert incident["diagnostics"]["bluez"]["failure"] == "timeout"  # type: ignore[index]


def test_worker_and_bluez_failures_become_explicit_unavailable_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingWorker(_FakeHciWorker):
        def snapshot(self) -> dict[str, object]:
            raise PermissionError("worker snapshot failed")

    async def bluez_failure(_adapter: str) -> dict[str, object]:
        raise OSError("D-Bus unavailable")

    monkeypatch.setattr(store_module, "_bluez_snapshot", bluez_failure)
    facade = store_module.ScanDiagnosticsFacade("hci0", worker=cast(HciScanDiagnostics, FailingWorker()))

    async def run() -> dict[str, object]:
        await facade.prepare(10)
        return await facade.snapshot(10)

    result = asyncio.run(run())

    assert result == {
        "hci": {"available": False, "status": "degraded", "failure": "PermissionError"},
        "bluez": store_module._unavailable("OSError"),
    }


def test_stale_generation_is_unknown_without_sampling_worker() -> None:
    class FailingWorker(_FakeHciWorker):
        def snapshot(self) -> dict[str, object]:
            raise AssertionError("stale generation must not sample HCI")

    facade = store_module.ScanDiagnosticsFacade("hci0", worker=cast(HciScanDiagnostics, FailingWorker()))

    result = asyncio.run(facade.snapshot(3))

    assert result == {
        "hci": {"available": False, "failure": "generation_unavailable"},
        "bluez": store_module._unavailable(),
    }


def test_cancelled_connect_disconnects_bluez_bus(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = _FakeBus(block_connect=asyncio.Event())
    monkeypatch.setattr(store_module, "MessageBus", lambda **_kwargs: bus)
    facade = store_module.ScanDiagnosticsFacade("hci0", worker=cast(HciScanDiagnostics, _FakeHciWorker()))

    async def run() -> None:
        await facade.prepare(12)
        task = asyncio.create_task(facade.snapshot(12))
        await bus.connected.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert bus.disconnected
