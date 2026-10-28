"""Bounded BLE scan evidence in the Collector-owned state database."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from dbus_fast import BusType, Message
from dbus_fast.aio import MessageBus

from ..application.ble_scan_health import ScanIncidentContext
from ..application.owned_work import joined_to_thread
from .ble_scan_diagnostics import ScanDiagnostics as HciScanDiagnostics
from .collector_state_db import StateDB, StateKind, StateTransaction

_OPEN = "open"
_HISTORY = "history"
_HISTORY_LIMIT = 8


class BleScanIncidentStore:
    """Persist one immutable open incident and a small resolved history."""

    def __init__(
        self,
        state_db: StateDB,
        diagnostics: ScanDiagnosticsFacade,
        *,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self._state_db = state_db
        self._diagnostics = diagnostics
        self._on_change = on_change
        self._pending_open: tuple[ScanIncidentContext, dict[str, object], str] | None = None

    async def active(self) -> bool:
        return await joined_to_thread(self._active)

    def _active(self) -> bool:
        return self._state_db.get(StateKind.BLE_SCAN_INCIDENT, _OPEN) is not None

    async def open(self, context: ScanIncidentContext) -> None:
        # Take the passive, bounded snapshot before entering SQLite's write transaction.
        if self._pending_open is None:
            detected_at = _now()
            try:
                snapshot = await self._diagnostics.snapshot(context.generation)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - missing diagnostics must not suppress incident persistence
                snapshot = {"available": False, "failure": type(error).__name__}
            self._pending_open = (context, snapshot, detected_at)
        pending_context, snapshot, detected_at = self._pending_open
        changed = await joined_to_thread(self._open, pending_context, snapshot, detected_at)
        self._pending_open = None
        if changed and self._on_change is not None:
            self._on_change()

    def _open(self, context: ScanIncidentContext, snapshot: dict[str, object], detected_at: str) -> bool:
        with self._state_db.transaction() as transaction:
            if transaction.get(StateKind.BLE_SCAN_INCIDENT, _OPEN) is not None:
                return False
            transaction.put(
                StateKind.BLE_SCAN_INCIDENT,
                _OPEN,
                {
                    "incident_id": str(uuid4()),
                    "detected_at": detected_at,
                    "context": _context_payload(context),
                    "diagnostics": snapshot,
                    "status": "open",
                },
            )
        return True

    async def resolve(self, generation: int, reason: str) -> None:
        changed = await joined_to_thread(self._resolve, generation, reason)
        if changed and self._on_change is not None:
            self._on_change()

    def _resolve(self, generation: int, reason: str) -> bool:
        with self._state_db.transaction() as transaction:
            incident = transaction.get(StateKind.BLE_SCAN_INCIDENT, _OPEN)
            if incident is None:
                return False
            history = transaction.get(StateKind.BLE_SCAN_INCIDENT, _HISTORY)
            entries = history.get("entries", []) if history is not None else []
            if not isinstance(entries, list):
                entries = []
            entries = [entry for entry in entries if isinstance(entry, dict)]
            context = incident.get("context")
            entries.append(
                {
                    "incident_id": incident.get("incident_id"),
                    "detected_at": incident.get("detected_at"),
                    "resolved_at": _now(),
                    "context": incident.get("context"),
                    "diagnostics": incident.get("diagnostics"),
                    "reason": context.get("reason") if isinstance(context, dict) else None,
                    "resolution": reason,
                    "generation": generation,
                }
            )
            transaction.put(
                StateKind.BLE_SCAN_INCIDENT,
                _HISTORY,
                {"entries": entries[-_HISTORY_LIMIT:]},
            )
            transaction.delete(StateKind.BLE_SCAN_INCIDENT, _OPEN)
        return True


def read_ble_scan_incident(state_db: StateDB | StateTransaction) -> dict[str, object] | None:
    """Return the bounded current-or-latest incident metadata for projections."""
    incident = state_db.get(StateKind.BLE_SCAN_INCIDENT, _OPEN)
    if incident is not None:
        context = incident.get("context")
        return {
            "incident_id": incident.get("incident_id"),
            "status": "open",
            "reason": context.get("reason") if isinstance(context, dict) else None,
            "detected_at": incident.get("detected_at"),
            "resolved_at": None,
        }
    history = state_db.get(StateKind.BLE_SCAN_INCIDENT, _HISTORY)
    entries = history.get("entries") if history is not None else None
    if not isinstance(entries, list) or not entries or not isinstance(entries[-1], dict):
        return None
    latest = entries[-1]
    context = latest.get("context")
    return {
        "incident_id": latest.get("incident_id"),
        "status": "resolved",
        "reason": context.get("reason") if isinstance(context, dict) else latest.get("reason"),
        "detected_at": latest.get("detected_at"),
        "resolved_at": latest.get("resolved_at"),
    }


class ScanDiagnosticsFacade:
    """Pair passive HCI counters with a bounded, read-only BlueZ snapshot."""

    def __init__(self, adapter: str, *, worker: HciScanDiagnostics | None = None) -> None:
        self._worker = worker or HciScanDiagnostics(adapter)
        self._generation: int | None = None
        self._worker_generation: int | None = None

    async def prepare(self, generation: int) -> None:
        if self._generation is not None and self._generation != generation:
            self._generation = None
            self._worker_generation = None
            await self._worker.stop()
        self._generation = generation
        await self._worker.start()
        self._worker_generation = self._worker.generation

    async def close(self, generation: int) -> None:
        if self._generation != generation:
            return
        self._generation = None
        self._worker_generation = None
        await self._worker.stop()

    def healthy(self, generation: int) -> None:
        if self._generation == generation:
            worker_generation = self._worker_generation
            if worker_generation is not None:
                self._worker.healthy(worker_generation)

    async def snapshot(self, generation: int) -> dict[str, object]:
        if self._generation != generation:
            return {"hci": {"available": False, "failure": "generation_unavailable"}, "bluez": _unavailable()}
        worker_generation = self._worker_generation
        if worker_generation is None:
            return {"hci": {"available": False, "failure": "generation_unavailable"}, "bluez": _unavailable()}
        try:
            available = await self._worker.probe(worker_generation)
            if self._generation != generation or self._worker.generation != worker_generation:
                return {"hci": {"available": False, "failure": "generation_unavailable"}, "bluez": _unavailable()}
            hci = self._worker.snapshot()
            if not available:
                hci.update({"available": False, "radio_evidence_available": False})
        except Exception as error:  # noqa: BLE001 - diagnostic availability cannot affect collection
            hci = {"available": False, "status": "degraded", "failure": type(error).__name__}
        finally:
            self._worker.end_probe(worker_generation)
        try:
            bluez = await asyncio.wait_for(_bluez_snapshot(self._worker.adapter), timeout=10.0)
        except TimeoutError:
            bluez = _unavailable("timeout")
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - diagnostic availability cannot affect collection
            bluez = _unavailable(type(error).__name__)
        if self._generation != generation:
            return {"hci": {"available": False, "failure": "generation_unavailable"}, "bluez": _unavailable()}
        return {"hci": hci, "bluez": bluez}


async def _bluez_snapshot(adapter: str) -> dict[str, object]:
    bus = MessageBus(bus_type=BusType.SYSTEM)
    try:
        await bus.connect()
        reply = await bus.call(
            Message(
                destination="org.bluez",
                path="/",
                interface="org.freedesktop.DBus.ObjectManager",
                member="GetManagedObjects",
            )
        )
        if reply.message_type.name == "ERROR" or not reply.body:
            return _unavailable("dbus_error")
        objects = cast(dict[str, dict[str, dict[str, object]]], reply.body[0])
        properties = objects.get(f"/org/bluez/{adapter}", {}).get("org.bluez.Adapter1", {})
        adapter_path = f"/org/bluez/{adapter}"
        devices = sum(
            path.startswith(f"{adapter_path}/") and "org.bluez.Device1" in interfaces
            for path, interfaces in objects.items()
        )
        return {
            "available": bool(properties),
            "powered": _variant_value(properties.get("Powered")),
            "discovering": _variant_value(properties.get("Discovering")),
            "devices_count": devices,
            "failure": None if properties else "adapter_unavailable",
        }
    finally:
        bus.disconnect()


def _variant_value(value: object) -> object:
    return getattr(value, "value", value)


def _unavailable(reason: str = "unavailable") -> dict[str, object]:
    return {"available": False, "powered": None, "discovering": None, "devices_count": None, "failure": reason}


def _context_payload(context: ScanIncidentContext) -> dict[str, object]:
    return {
        "generation": context.generation,
        "reason": context.reason,
        "active_seconds": context.active_seconds,
        "bleak_callbacks": context.bleak_callbacks,
        "last_callback_age_seconds": context.last_callback_age_seconds,
        "threshold_seconds": context.threshold_seconds,
        "error_kind": context.error_kind,
    }


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
