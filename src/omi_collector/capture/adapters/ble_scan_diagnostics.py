"""Passive, bounded diagnostics for BLE scan behavior below BlueZ."""

from __future__ import annotations

import asyncio
import errno
import socket
import struct
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Protocol, cast

from .ble_link_observability import _hci_adapter_index, _native_hci_bind

_AF_BLUETOOTH = getattr(socket, "AF_BLUETOOTH", 31)
_BTPROTO_HCI = getattr(socket, "BTPROTO_HCI", 1)
_HCI_FILTER = 2
_HCI_CHANNEL_RAW = 0
_HCI_EVENT_HEADER_BYTES = 3
_HCI_FILTER_BYTES = 16
_HCI_EVENT_PACKET = 0x04
_LE_META = 0x3E
_COMMAND_COMPLETE = 0x0E
_COMMAND_STATUS = 0x0F
_HARDWARE_ERROR = 0x10
_LEGACY_ADVERTISING_REPORT = 0x02
_EXTENDED_ADVERTISING_REPORT = 0x0D
_LEGACY_REPORT_HEADER_BYTES = 9
_EXTENDED_REPORT_HEADER_BYTES = 24
_COMMAND_STATUS_BYTES = 4
_SCAN_OPCODES = {0x200B, 0x200C, 0x2041, 0x2042}
_MAX_DRAIN_PACKETS = 128
_PROBE_SECONDS = 3.0


class _HciSocket(Protocol):
    def fileno(self) -> int: ...

    def setblocking(self, flag: bool, /) -> None: ...

    def setsockopt(self, level: int, option: int, value: bytes, /) -> None: ...

    def getsockopt(self, level: int, option: int, length: int, /) -> bytes | int: ...

    def recv(self, size: int, /) -> bytes: ...

    def close(self) -> None: ...


def _hci_scan_filter(*, include_le_meta: bool) -> bytes:
    event_mask_low = (1 << _COMMAND_COMPLETE) | (1 << _COMMAND_STATUS) | (1 << _HARDWARE_ERROR)
    event_mask_high = (1 << (_LE_META - 32)) if include_le_meta else 0
    return struct.pack("<IIIH2x", 1 << _HCI_EVENT_PACKET, event_mask_low, event_mask_high, 0)


def _valid_advertising_reports(payload: bytes, *, extended: bool) -> bool:
    if not payload or payload[0] == 0:
        return False
    fixed_bytes = _EXTENDED_REPORT_HEADER_BYTES if extended else _LEGACY_REPORT_HEADER_BYTES
    offset = 1
    for _ in range(payload[0]):
        if len(payload) < offset + fixed_bytes:
            return False
        data_length = payload[offset + fixed_bytes - 1]
        offset += fixed_bytes + data_length + (0 if extended else 1)
        if offset > len(payload):
            return False
    return offset == len(payload)


class ScanDiagnostics:
    """Observe scan-related HCI events without sending commands or retaining packets."""

    def __init__(
        self,
        adapter: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        socket_factory: Callable[..., _HciSocket] | None = None,
        native_bind: Callable[[int, int, int, int], None] | None = None,
    ) -> None:
        self.adapter = adapter
        self._clock = clock
        self._socket_factory = socket_factory or cast(Callable[..., _HciSocket], socket.socket)
        self._native_bind = native_bind or _native_hci_bind
        self._socket: _HciSocket | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generation = 0
        self._reset_evidence()
        self._status = "not_started"
        self._failure: str | None = None
        self._filter_readback: dict[str, object] = {"available": False, "le_meta_enabled": None}
        self._capture_state = "unavailable"
        self._quiet_attempted = False
        self._probe_count = 0
        self._probe_wakeup: asyncio.Event | None = None

    async def start(self) -> None:
        if self._socket is not None:
            return
        self._generation += 1
        self._reset_evidence()
        self._filter_readback = {"available": False, "le_meta_enabled": None}
        self._started_monotonic = self._clock()
        try:
            sock = self._socket_factory(_AF_BLUETOOTH, socket.SOCK_RAW, _BTPROTO_HCI)
            try:
                self._native_bind(sock.fileno(), _AF_BLUETOOTH, _hci_adapter_index(self.adapter), _HCI_CHANNEL_RAW)
                sock.setblocking(False)
                sock.setsockopt(0, _HCI_FILTER, _hci_scan_filter(include_le_meta=True))
                self._filter_readback = self._read_filter(sock)
            except BaseException:
                with suppress(BaseException):
                    sock.close()
                raise
            self._socket = sock
            self._failure = None
            self._status = "available"
            loop = asyncio.get_running_loop()
            self._loop = loop
            self._probe_wakeup = asyncio.Event()
            self._quiet_attempted = False
            self._probe_count = 0
            self._capture_state = "full" if self._filter_readback.get("le_meta_enabled") else "unavailable"
            generation = self._generation
            loop.add_reader(sock.fileno(), self._read_events, generation, sock)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - diagnostic access must never interrupt collection
            self._status = "degraded"
            self._failure = self._failure_name(error)
            self._capture_state = "unavailable"
            sock, self._socket = self._socket, None
            loop, self._loop = self._loop, None
            self._close_capture_socket(sock, loop)

    @property
    def generation(self) -> int:
        return self._generation

    async def stop(self) -> None:
        self._generation += 1
        if self._probe_wakeup is not None:
            self._probe_wakeup.set()
        self._probe_wakeup = None
        self._probe_count = 0
        self._capture_state = "stopped"
        sock, self._socket = self._socket, None
        loop, self._loop = self._loop, None
        self._close_capture_socket(sock, loop)
        if self._status == "available":
            self._status = "stopped"

    @staticmethod
    def _close_capture_socket(sock: _HciSocket | None, loop: asyncio.AbstractEventLoop | None) -> None:
        if sock is None:
            return
        if loop is not None:
            with suppress(Exception):
                loop.remove_reader(sock.fileno())
        with suppress(OSError):
            sock.close()

    def snapshot(self) -> dict[str, object]:
        return {
            "adapter": self.adapter,
            "status": self._status,
            "generation": self._generation,
            "started_monotonic": self._started_monotonic,
            "failure": self._failure,
            "filter": dict(self._filter_readback),
            "capture": {"mode": self._capture_mode, "counts_scope": "advertising_capture_windows"},
            "counts": dict(self._counts),
            "last_monotonic": dict(self._last),
            "scan_commands": {f"0x{opcode:04x}": dict(value) for opcode, value in self._command_status.items()},
        }

    @property
    def _capture_mode(self) -> str:
        return "full" if self._capture_state in ("probing", "probe_recovered") else self._capture_state

    def healthy(self, generation: int) -> None:
        """Narrow raw capture once after a valid current-generation callback."""
        if generation != self._generation or self._socket is None:
            return
        if self._probe_count:
            self._capture_state = "probe_recovered"
            self._quiet_attempted = True
            return
        if self._quiet_attempted:
            return
        self._quiet_attempted = True
        self._narrow_filter()

    def _narrow_filter(self) -> None:
        if self._socket is None or self._capture_state == "unavailable":
            return
        try:
            self._socket.setsockopt(0, _HCI_FILTER, _hci_scan_filter(include_le_meta=False))
            self._filter_readback = self._read_filter(self._socket)
            self._capture_state = (
                "unavailable"
                if not self._filter_readback.get("available")
                else "full"
                if self._filter_readback.get("le_meta_enabled")
                else "quiet"
            )
        except Exception as error:  # noqa: BLE001 - failed narrowing keeps full passive capture
            self._filter_readback["narrow_failure"] = self._failure_name(error)
            self._capture_state = "full"

    async def probe(self, generation: int) -> bool:
        """Widen capture for a bounded observation window; return whether it was available."""
        if generation != self._generation or self._socket is None:
            return False
        sock = self._socket
        if self._probe_count == 0:
            self._capture_state = "probing"
            self._quiet_attempted = False
        self._probe_count += 1
        try:
            sock.setsockopt(0, _HCI_FILTER, _hci_scan_filter(include_le_meta=True))
            self._filter_readback = self._read_filter(sock)
            if not self._filter_readback.get("available") or not self._filter_readback.get("le_meta_enabled"):
                raise OSError("LE Meta filter readback unavailable")
            if self._capture_state != "probe_recovered":
                self._capture_state = "probing"
        except Exception as error:  # noqa: BLE001 - unavailable evidence must never look like radio silence
            self._capture_state = "unavailable"
            self._filter_readback.update(
                {"available": False, "radio_evidence_unavailable": True, "widen_failure": self._failure_name(error)}
            )
            return False
        wakeup = self._probe_wakeup
        if wakeup is not None:
            with suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=_PROBE_SECONDS)
        return (
            generation == self._generation
            and self._socket is sock
            and self._capture_state != "unavailable"
            and self._filter_readback.get("available") is True
            and self._filter_readback.get("le_meta_enabled") is True
        )

    def end_probe(self, generation: int) -> None:
        if generation != self._generation or self._socket is None or self._probe_count == 0:
            return
        self._probe_count -= 1
        if self._probe_count == 0 and self._capture_state != "unavailable":
            if self._capture_state == "probe_recovered":
                self._narrow_filter()
            else:
                self._capture_state = "full"

    def _read_events(self, generation: int, sock: _HciSocket) -> None:
        try:
            for _ in range(_MAX_DRAIN_PACKETS):
                if generation != self._generation:
                    return
                try:
                    packet = sock.recv(260)
                except BlockingIOError:
                    return
                self._observe(packet)
        except Exception as error:  # noqa: BLE001 - diagnostic access must never interrupt collection
            self._status = "degraded"
            self._failure = self._failure_name(error)
            self._capture_state = "unavailable"
            if self._probe_wakeup is not None:
                self._probe_wakeup.set()
            if self._socket is sock:
                self._socket = None
            loop, self._loop = self._loop, None
            self._close_capture_socket(sock, loop)

    def _observe(self, packet: bytes) -> None:
        if (
            len(packet) < _HCI_EVENT_HEADER_BYTES
            or packet[0] != _HCI_EVENT_PACKET
            or len(packet) < _HCI_EVENT_HEADER_BYTES + packet[2]
        ):
            self._increment("malformed")
            return
        event, payload = packet[1], packet[_HCI_EVENT_HEADER_BYTES : _HCI_EVENT_HEADER_BYTES + packet[2]]
        if event == _LE_META:
            self._observe_advertising(payload)
        elif event == _HARDWARE_ERROR:
            if payload:
                self._increment("hardware_error")
            else:
                self._increment("malformed")
        elif event in (_COMMAND_COMPLETE, _COMMAND_STATUS):
            self._observe_command(event, payload)

    def _observe_advertising(self, payload: bytes) -> None:
        if not payload:
            self._increment("malformed")
            return
        subevent = payload[0]
        counter = {
            _LEGACY_ADVERTISING_REPORT: ("legacy_advertising", False),
            _EXTENDED_ADVERTISING_REPORT: ("extended_advertising", True),
        }.get(subevent)
        if counter is None:
            return
        name, extended = counter
        if _valid_advertising_reports(payload[1:], extended=extended):
            self._increment(name)
        else:
            self._increment("malformed")

    def _observe_command(self, event: int, payload: bytes) -> None:
        if len(payload) < _COMMAND_STATUS_BYTES:
            return
        if event == _COMMAND_COMPLETE:
            opcode, status = int.from_bytes(payload[1:3], "little"), payload[3]
        else:
            opcode, status = int.from_bytes(payload[2:4], "little"), payload[0]
        self._record_command(opcode, status)

    def _record_command(self, opcode: int, status: int) -> None:
        if opcode in self._command_status:
            value = self._command_status[opcode]
            value["count"] = (value["count"] or 0) + 1
            value["last_status"] = status

    def _increment(self, name: str) -> None:
        self._counts[name] += 1
        self._last[name] = self._clock()

    def _reset_evidence(self) -> None:
        self._counts: dict[str, int] = {
            "legacy_advertising": 0,
            "extended_advertising": 0,
            "hardware_error": 0,
            "malformed": 0,
        }
        self._last: dict[str, float | None] = dict.fromkeys(self._counts)
        self._command_status: dict[int, dict[str, int | None]] = {
            opcode: {"count": 0, "last_status": None} for opcode in sorted(_SCAN_OPCODES)
        }
        self._started_monotonic: float | None = None

    @staticmethod
    def _read_filter(sock: _HciSocket) -> dict[str, object]:
        try:
            value = sock.getsockopt(0, _HCI_FILTER, _HCI_FILTER_BYTES)
            if not isinstance(value, bytes):
                return {"available": False, "le_meta_enabled": None, "failure": "invalid_readback"}
            if len(value) < _HCI_FILTER_BYTES:
                return {"available": False, "le_meta_enabled": None, "failure": "truncated_readback"}
            _, _, high, _ = struct.unpack("<IIIH2x", value[:_HCI_FILTER_BYTES])
            return {"available": True, "le_meta_enabled": bool(high & (1 << (_LE_META - 32)))}
        except (AttributeError, OSError, struct.error) as error:
            return {"available": False, "le_meta_enabled": None, "failure": ScanDiagnostics._failure_name(error)}

    @staticmethod
    def _failure_name(error: BaseException) -> str:
        if isinstance(error, PermissionError) or (isinstance(error, OSError) and error.errno == errno.EPERM):
            return "cap_net_raw_unavailable"
        return type(error).__name__
