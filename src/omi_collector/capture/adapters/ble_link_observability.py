"""Read-only, best-effort BLE link telemetry from the Linux HCI event stream.

This module intentionally observes only HCI event packets.  It never opens an
ACL channel and never sees GATT or audio payloads.  Every failure is diagnostic
only: collection must remain usable when raw HCI access is unavailable.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import logging
import os
import queue
import re
import socket
import struct
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG, BleConfig
from .debug_logging import debug_event, debug_exception

_DEBUG_LOGGER = logging.getLogger(DEFAULT_CONFIG.observability.debug_log.logger_name)
_WARNING_LOGGER = logging.getLogger("omi_collector.ble_link")
_HCI_HEADER_BYTES = 3
_LEGACY_CONNECTION_BYTES = 18
_ENHANCED_CONNECTION_BYTES = 30
_CONNECTION_UPDATE_BYTES = 9
_REMOTE_CONNECTION_PARAMETER_REQUEST_BYTES = 10
_DATA_LENGTH_CHANGE_BYTES = 10
_PHY_UPDATE_BYTES = 5
_DISCONNECT_BYTES = 4
_COMMAND_COMPLETE_HEADER_BYTES = 4
_READ_PHY_PARAMS_BYTES = 5
_AF_BLUETOOTH = getattr(socket, "AF_BLUETOOTH", 31)
_BTPROTO_HCI = getattr(socket, "BTPROTO_HCI", 1)
_HCI_EVENT_MASK_WORD_BITS = 32
_HCI_DEVICE_LIMIT = 0xFFFF
_UNSIGNED_SHORT_MAX = 0xFFFF
_HCI_ADAPTER_PATTERN = re.compile(r"hci[0-9]+")
_HCI_EVENT_PACKET = 0x04
_HCI_COMMAND_PACKET = 0x01
_HCI_CHANNEL_RAW = 0
_HCI_SOL = 0
_HCI_FILTER_OPTION = 2
_HCI_FILTER_OPCODE = 0
_HCI_EVENT_LE_META = 0x3E
_HCI_EVENT_DISCONNECTION_COMPLETE = 0x05
_HCI_EVENT_COMMAND_COMPLETE = 0x0E
_LE_CONNECTION_COMPLETE = 0x01
_LE_ENHANCED_CONNECTION_COMPLETE = 0x0A
_LE_CONNECTION_UPDATE_COMPLETE = 0x03
_LE_REMOTE_CONNECTION_PARAMETER_REQUEST = 0x06
_LE_DATA_LENGTH_CHANGE = 0x07
_LE_PHY_UPDATE_COMPLETE = 0x0C
_LE_READ_PHY_OPCODE = 0x2030
_PHY_1M = 0x01
_PHY_2M = 0x02
_PHY_CODED = 0x03
_ROLE_CENTRAL = 0x00
_ROLE_PERIPHERAL = 0x01
_PEER_ADDRESS_PUBLIC = 0x00
_PEER_ADDRESS_RANDOM = 0x01
_PEER_ADDRESS_PUBLIC_IDENTITY = 0x02
_PEER_ADDRESS_RANDOM_IDENTITY = 0x03
_INTERVAL_UNIT_MS = 1.25
_SUPERVISION_TIMEOUT_UNIT_MS = 10.0


class HciSocket(Protocol):
    def fileno(self) -> int: ...

    def setblocking(self, flag: bool) -> object: ...

    def recv(self, size: int) -> bytes: ...

    def send(self, payload: bytes) -> int: ...

    def setsockopt(self, level: int, option: int, value: bytes) -> object: ...

    def close(self) -> object: ...


SocketFactory = Callable[[int, int, int], HciSocket]
HciBinder = Callable[[int, int, int, int], None]
TerminalCallback = Callable[[dict[str, object]], object]


class _SockaddrHci(ctypes.Structure):
    """Linux ``struct sockaddr_hci`` (family, device, channel)."""

    _fields_ = [
        ("family", ctypes.c_ushort),
        ("dev", ctypes.c_ushort),
        ("channel", ctypes.c_ushort),
    ]


def _native_hci_bind(file_descriptor: int, family: int, dev: int, channel: int) -> None:
    """Bind an HCI socket through libc, bypassing Python's missing AF support."""
    if os.name != "posix" or not hasattr(os, "uname") or os.uname().sysname != "Linux":
        raise OSError(errno.ENOTSUP, "native HCI bind requires Linux")
    if not 0 <= family <= _UNSIGNED_SHORT_MAX:
        raise ValueError(f"family must fit unsigned short: {family}")
    if not 0 <= dev < _HCI_DEVICE_LIMIT:
        raise ValueError(f"dev must be in [0, {_HCI_DEVICE_LIMIT}): {dev}")
    if not 0 <= channel <= _UNSIGNED_SHORT_MAX:
        raise ValueError(f"channel must fit unsigned short: {channel}")
    address = _SockaddrHci(family, dev, channel)
    libc = ctypes.CDLL(None, use_errno=True)
    bind = libc.bind
    bind.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    bind.restype = ctypes.c_int
    if bind(file_descriptor, ctypes.byref(address), ctypes.sizeof(address)) == 0:
        return
    error_number = ctypes.get_errno() or errno.EIO
    raise OSError(error_number, os.strerror(error_number))


def _hci_adapter_index(adapter: str) -> int:
    if _HCI_ADAPTER_PATTERN.fullmatch(adapter) is None:
        raise ValueError("adapter must match hci<decimal>")
    index = int(adapter[3:])
    if index >= _HCI_DEVICE_LIMIT:
        raise ValueError(f"adapter device must be in [0, {_HCI_DEVICE_LIMIT})")
    return index


@dataclass(frozen=True, slots=True)
class PhyTransition:
    """One observed controller-reported PHY transition."""

    tx_phy: str | None
    rx_phy: str | None

    def as_dict(self) -> dict[str, object]:
        return {"tx_phy": self.tx_phy, "rx_phy": self.rx_phy}


@dataclass(frozen=True, slots=True)
class PhyOutcome:
    """Status and effective values from one PHY operation."""

    status_hex: str
    status_name: str
    effective_phy: PhyTransition | None

    def as_dict(self) -> dict[str, object]:
        return {
            "status_hex": self.status_hex,
            "status_name": self.status_name,
            "effective_phy": self.effective_phy.as_dict() if self.effective_phy is not None else None,
        }


@dataclass(frozen=True, slots=True)
class DataLengthTransition:
    """One effective LE Data Length Change negotiation result."""

    max_tx_octets: int
    max_tx_time: int
    max_rx_octets: int
    max_rx_time: int

    def as_dict(self) -> dict[str, object]:
        return {
            "max_tx_octets": self.max_tx_octets,
            "max_tx_time": self.max_tx_time,
            "max_rx_octets": self.max_rx_octets,
            "max_rx_time": self.max_rx_time,
        }


@dataclass(frozen=True, slots=True)
class ConnectionParameters:
    """Effective BLE connection parameters in human-readable units."""

    interval_ms: float
    latency: int
    supervision_timeout_ms: float

    def as_dict(self) -> dict[str, object]:
        return {
            "interval_ms": round(self.interval_ms, 3),
            "latency": self.latency,
            "supervision_timeout_ms": round(self.supervision_timeout_ms, 3),
        }


@dataclass(frozen=True, slots=True)
class ConnectionParameterRequest:
    """One controller-reported remote connection parameter request."""

    min_interval_ms: float
    max_interval_ms: float
    latency: int
    supervision_timeout_ms: float

    def as_dict(self) -> dict[str, object]:
        return {
            "min_interval_ms": round(self.min_interval_ms, 3),
            "max_interval_ms": round(self.max_interval_ms, 3),
            "latency": self.latency,
            "supervision_timeout_ms": round(self.supervision_timeout_ms, 3),
        }


@dataclass(frozen=True, slots=True)
class ConnectionParameterUpdate:
    """One controller-reported result of a connection parameter update."""

    status_hex: str
    status_name: str
    effective_parameters: ConnectionParameters | None

    def as_dict(self) -> dict[str, object]:
        return {
            "status_hex": self.status_hex,
            "status_name": self.status_name,
            "effective_parameters": self.effective_parameters.as_dict()
            if self.effective_parameters is not None
            else None,
        }


@dataclass(frozen=True, slots=True)
class BleLinkSessionRecord:
    """Terminal, bounded summary of one matched physical BLE connection."""

    address: str
    handle: int
    initial_connection_parameters: ConnectionParameters
    connection_parameter_requests: tuple[ConnectionParameterRequest, ...]
    connection_parameter_updates: tuple[ConnectionParameterUpdate, ...]
    final_connection_parameters: ConnectionParameters
    phy_policy: str
    tx_phy: str | None
    rx_phy: str | None
    phy_transitions: tuple[PhyTransition, ...]
    initial_phy_snapshot: PhyOutcome | None
    phy_update_outcomes: tuple[PhyOutcome, ...]
    data_length: DataLengthTransition | None
    data_length_transitions: tuple[DataLengthTransition, ...]
    role: str
    role_hex: str | None
    peer_address_type: str
    peer_address_type_hex: str | None
    duration_seconds: float
    disconnect_reason_hex: str | None
    disconnect_reason_name: str | None
    disconnect_class: str | None
    local_name: str | None
    observer_status: str
    dropped_packets: int

    def as_dict(self) -> dict[str, object]:
        return {
            "event": "ble_link_session",
            "address": self.address,
            "handle": self.handle,
            "initial_connection_parameters": self.initial_connection_parameters.as_dict(),
            "connection_parameter_requests": tuple(item.as_dict() for item in self.connection_parameter_requests),
            "connection_parameter_updates": tuple(item.as_dict() for item in self.connection_parameter_updates),
            "final_connection_parameters": self.final_connection_parameters.as_dict(),
            "phy_policy": self.phy_policy,
            "tx_phy": self.tx_phy,
            "rx_phy": self.rx_phy,
            "phy_transitions": tuple(item.as_dict() for item in self.phy_transitions),
            "initial_phy_snapshot": self.initial_phy_snapshot.as_dict()
            if self.initial_phy_snapshot is not None
            else None,
            "phy_update_outcomes": tuple(item.as_dict() for item in self.phy_update_outcomes),
            "data_length": self.data_length.as_dict() if self.data_length is not None else None,
            "data_length_transitions": tuple(item.as_dict() for item in self.data_length_transitions),
            "role": self.role,
            "role_hex": self.role_hex,
            "peer_address_type": self.peer_address_type,
            "peer_address_type_hex": self.peer_address_type_hex,
            "duration_seconds": round(self.duration_seconds, 3),
            "disconnect_reason_hex": self.disconnect_reason_hex,
            "disconnect_reason_name": self.disconnect_reason_name,
            "disconnect_class": self.disconnect_class,
            "local_name": self.local_name,
            "observer_status": self.observer_status,
            "dropped_packets": self.dropped_packets,
        }


@dataclass(frozen=True, slots=True)
class _ConnectEvent:
    handle: int
    address: str
    role: int
    peer_address_type: int
    interval: int
    latency: int
    supervision_timeout: int


@dataclass(frozen=True, slots=True)
class _PhyEvent:
    handle: int
    status: int
    tx_phy: int | None
    rx_phy: int | None


@dataclass(frozen=True, slots=True)
class _DataLengthChangeEvent:
    handle: int
    max_tx_octets: int
    max_tx_time: int
    max_rx_octets: int
    max_rx_time: int


@dataclass(frozen=True, slots=True)
class _ConnectionParameterUpdateEvent:
    handle: int
    status: int
    interval: int
    latency: int
    supervision_timeout: int


@dataclass(frozen=True, slots=True)
class _ConnectionParameterRequestEvent:
    handle: int
    min_interval: int
    max_interval: int
    latency: int
    supervision_timeout: int


@dataclass(frozen=True, slots=True)
class _CommandCompleteEvent:
    opcode: int
    status: int
    handle: int | None
    tx_phy: int | None
    rx_phy: int | None


@dataclass(frozen=True, slots=True)
class _DisconnectEvent:
    handle: int
    reason: int


@dataclass(slots=True)
class _ActiveSession:
    handle: int
    interval: int
    latency: int
    supervision_timeout: int
    started: float
    initial_connection_parameters: ConnectionParameters
    role: int
    peer_address_type: int
    requests: list[ConnectionParameterRequest] | None = None
    updates: list[ConnectionParameterUpdate] | None = None
    tx_phy: str | None = None
    rx_phy: str | None = None
    transitions: list[PhyTransition] | None = None
    initial_phy_snapshot: PhyOutcome | None = None
    phy_update_outcomes: list[PhyOutcome] | None = None
    data_length: DataLengthTransition | None = None
    data_length_transitions: list[DataLengthTransition] | None = None


def normalize_address(address: str) -> str:
    """Normalize colon-separated Bluetooth addresses for exact matching."""
    return address.strip().lower()


def _format_address(raw: bytes) -> str:
    return ":".join(f"{value:02x}" for value in raw[::-1])


def hci_filter_bytes() -> bytes:
    """Build the Linux ``struct hci_filter`` ABI for event-only telemetry."""
    event_mask_low = (1 << _HCI_EVENT_DISCONNECTION_COMPLETE) | (1 << _HCI_EVENT_COMMAND_COMPLETE)
    le_meta_bit = _HCI_EVENT_LE_META - _HCI_EVENT_MASK_WORD_BITS
    event_mask_high = 1 << le_meta_bit
    type_mask = 1 << _HCI_EVENT_PACKET
    return struct.pack("<IIIH2x", type_mask, event_mask_low, event_mask_high, _HCI_FILTER_OPCODE)


def _malformed(packet: bytes, detail: str, logger: logging.Logger) -> None:
    debug_event("ble_link_malformed_packet", logger=logger, packet_bytes=len(packet), detail=detail)


def parse_hci_packet(
    packet: bytes | bytearray,
    *,
    logger: logging.Logger | None = None,
) -> (
    _ConnectEvent
    | _PhyEvent
    | _ConnectionParameterUpdateEvent
    | _ConnectionParameterRequestEvent
    | _DataLengthChangeEvent
    | _CommandCompleteEvent
    | _DisconnectEvent
    | None
):
    """Parse only the HCI event subset used by the link observer.

    Invalid or truncated packets are ignored and recorded in the DEBUG ring.
    ACL, command, and every unrelated event packet are deliberately ignored.
    """
    target_logger = logger or _DEBUG_LOGGER
    data = bytes(packet)
    if len(data) < _HCI_HEADER_BYTES - 1 or data[0] != _HCI_EVENT_PACKET:
        return None
    payload_length = data[2] if len(data) >= _HCI_HEADER_BYTES else -1
    if payload_length < 0 or len(data) < _HCI_HEADER_BYTES + payload_length:
        _malformed(data, "truncated_hci_event", target_logger)
        return None
    event = data[1]
    payload = data[_HCI_HEADER_BYTES : _HCI_HEADER_BYTES + payload_length]
    if event == _HCI_EVENT_LE_META:
        return _parse_le_meta(payload, data, target_logger)
    if event == _HCI_EVENT_DISCONNECTION_COMPLETE:
        return _parse_disconnect(payload, data, target_logger)
    if event == _HCI_EVENT_COMMAND_COMPLETE:
        return _parse_command_complete(payload, data, target_logger)
    return None


def _parse_le_meta(  # noqa: PLR0911
    payload: bytes, packet: bytes, logger: logging.Logger
) -> (
    _ConnectEvent
    | _PhyEvent
    | _ConnectionParameterUpdateEvent
    | _ConnectionParameterRequestEvent
    | _DataLengthChangeEvent
    | None
):
    if not payload:
        _malformed(packet, "empty_le_meta_event", logger)
        return None
    subevent, body = payload[0], payload[1:]
    if subevent in (_LE_CONNECTION_COMPLETE, _LE_ENHANCED_CONNECTION_COMPLETE):
        return _parse_connection(body, packet, logger, enhanced=subevent == _LE_ENHANCED_CONNECTION_COMPLETE)
    parameter_parser = {
        _LE_CONNECTION_UPDATE_COMPLETE: _parse_connection_update,
        _LE_REMOTE_CONNECTION_PARAMETER_REQUEST: _parse_connection_parameter_request,
    }.get(subevent)
    if parameter_parser is not None:
        return parameter_parser(body, packet, logger)
    if subevent == _LE_DATA_LENGTH_CHANGE:
        if len(body) != _DATA_LENGTH_CHANGE_BYTES:
            _malformed(packet, "malformed_data_length_change", logger)
            return None
        return _DataLengthChangeEvent(
            int.from_bytes(body[0:2], "little") & 0x0FFF,
            int.from_bytes(body[2:4], "little"),
            int.from_bytes(body[4:6], "little"),
            int.from_bytes(body[6:8], "little"),
            int.from_bytes(body[8:10], "little"),
        )
    if subevent != _LE_PHY_UPDATE_COMPLETE:
        return None
    if len(body) < _PHY_UPDATE_BYTES:
        _malformed(packet, "truncated_phy_update_complete", logger)
        return None
    status, handle = body[0], int.from_bytes(body[1:3], "little") & 0x0FFF
    return _PhyEvent(handle, status, body[3], body[4]) if status == 0 else _PhyEvent(handle, status, None, None)


def _parse_connection(body: bytes, packet: bytes, logger: logging.Logger, *, enhanced: bool) -> _ConnectEvent | None:
    required = _ENHANCED_CONNECTION_BYTES if enhanced else _LEGACY_CONNECTION_BYTES
    if len(body) < required:
        _malformed(
            packet,
            "truncated_enhanced_connection_complete" if enhanced else "truncated_legacy_connection_complete",
            logger,
        )
        return None
    if body[0] != 0:
        return None
    base = 23 if enhanced else 11
    return _ConnectEvent(
        handle=int.from_bytes(body[1:3], "little") & 0x0FFF,
        address=_format_address(body[5:11]),
        role=body[3],
        peer_address_type=body[4],
        interval=int.from_bytes(body[base : base + 2], "little"),
        latency=int.from_bytes(body[base + 2 : base + 4], "little"),
        supervision_timeout=int.from_bytes(body[base + 4 : base + 6], "little"),
    )


def _parse_connection_update(
    body: bytes, packet: bytes, logger: logging.Logger
) -> _ConnectionParameterUpdateEvent | None:
    if len(body) < _CONNECTION_UPDATE_BYTES:
        _malformed(packet, "truncated_connection_update_complete", logger)
        return None
    return _ConnectionParameterUpdateEvent(
        int.from_bytes(body[1:3], "little") & 0x0FFF,
        body[0],
        int.from_bytes(body[3:5], "little"),
        int.from_bytes(body[5:7], "little"),
        int.from_bytes(body[7:9], "little"),
    )


def _parse_connection_parameter_request(
    body: bytes, packet: bytes, logger: logging.Logger
) -> _ConnectionParameterRequestEvent | None:
    if len(body) < _REMOTE_CONNECTION_PARAMETER_REQUEST_BYTES:
        _malformed(packet, "truncated_remote_connection_parameter_request", logger)
        return None
    return _ConnectionParameterRequestEvent(
        int.from_bytes(body[0:2], "little"),
        int.from_bytes(body[2:4], "little"),
        int.from_bytes(body[4:6], "little"),
        int.from_bytes(body[6:8], "little"),
        int.from_bytes(body[8:10], "little"),
    )


def _parse_disconnect(payload: bytes, packet: bytes, logger: logging.Logger) -> _DisconnectEvent | None:
    if len(payload) < _DISCONNECT_BYTES:
        _malformed(packet, "truncated_disconnection_complete", logger)
        return None
    return _DisconnectEvent(int.from_bytes(payload[1:3], "little") & 0x0FFF, payload[3]) if payload[0] == 0 else None


def _parse_command_complete(payload: bytes, packet: bytes, logger: logging.Logger) -> _CommandCompleteEvent | None:
    if len(payload) < _COMMAND_COMPLETE_HEADER_BYTES:
        _malformed(packet, "truncated_command_complete", logger)
        return None
    opcode = int.from_bytes(payload[1:3], "little")
    if opcode != _LE_READ_PHY_OPCODE:
        return None
    params = payload[3:]
    if len(params) < 1:
        _malformed(packet, "truncated_read_phy_complete", logger)
        return None
    if params[0] != 0:
        return _CommandCompleteEvent(opcode, params[0], None, None, None)
    if len(params) != _READ_PHY_PARAMS_BYTES:
        _malformed(packet, "malformed_read_phy_complete", logger)
        return None
    return _CommandCompleteEvent(
        opcode, params[0], int.from_bytes(params[1:3], "little") & 0x0FFF, params[3], params[4]
    )


def _phy_name(value: int) -> str | None:
    return {
        _PHY_1M: "1M",
        _PHY_2M: "2M",
        _PHY_CODED: "coded",
    }.get(value)


def _named_hci_value(value: int, names: dict[int, str]) -> tuple[str, str | None]:
    name = names.get(value)
    return (name, None) if name is not None else ("unknown", f"0x{value:02x}")


def _role_name(value: int) -> tuple[str, str | None]:
    return _named_hci_value(value, {_ROLE_CENTRAL: "central", _ROLE_PERIPHERAL: "peripheral"})


def _peer_address_type_name(value: int) -> tuple[str, str | None]:
    return _named_hci_value(
        value,
        {
            _PEER_ADDRESS_PUBLIC: "public",
            _PEER_ADDRESS_RANDOM: "random",
            _PEER_ADDRESS_PUBLIC_IDENTITY: "public_identity",
            _PEER_ADDRESS_RANDOM_IDENTITY: "random_identity",
        },
    )


def _connection_parameters(interval: int, latency: int, supervision_timeout: int) -> ConnectionParameters:
    return ConnectionParameters(
        interval * _INTERVAL_UNIT_MS,
        latency,
        supervision_timeout * _SUPERVISION_TIMEOUT_UNIT_MS,
    )


def _hci_status_name(status: int) -> str:
    return {
        0x00: "success",
        0x01: "unknown_command",
        0x02: "unknown_connection_identifier",
        0x05: "authentication_failure",
        0x06: "pin_or_key_missing",
        0x07: "memory_capacity_exceeded",
        0x08: "connection_timeout",
        0x0C: "command_disallowed",
        0x0D: "connection_rejected_limited_resources",
        0x1A: "unsupported_remote_feature",
    }.get(status, "unknown")


class BleLinkObserver:
    """Best-effort raw-HCI observer whose failures cannot fail collection."""

    def __init__(  # noqa: PLR0913
        self,
        address: str,
        *,
        adapter: str = DEFAULT_CONFIG.ble.adapter_name,
        phy_policy: str = "auto",
        local_name: str | None = None,
        config: BleConfig = DEFAULT_CONFIG.ble,
        socket_factory: SocketFactory | None = None,
        native_bind: HciBinder | None = None,
        clock: Callable[[], float] = time.monotonic,
        terminal_callback: TerminalCallback | None = None,
        debug_logger: logging.Logger | None = None,
        warning_logger: logging.Logger | None = None,
    ) -> None:
        if phy_policy not in {"auto", "force_1m"}:
            raise ValueError("phy_policy must be auto or force_1m")
        self.address = normalize_address(address)
        self.adapter = adapter
        self.phy_policy = phy_policy
        self.local_name = local_name[: config.observer_max_local_name_chars] if local_name else None
        self.config = config
        self._socket_factory = socket_factory or socket.socket
        self._native_bind = native_bind or _native_hci_bind
        self._clock = clock
        self._terminal_callback = terminal_callback
        self._debug_logger = debug_logger or _DEBUG_LOGGER
        self._warning_logger = warning_logger or _WARNING_LOGGER
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=config.observer_queue_max_packets)
        self._socket: HciSocket | None = None
        self._reader: threading.Thread | None = None
        self._processor: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._active: _ActiveSession | None = None
        self._warning_emitted = False
        self.observer_status = "not_started"
        self.dropped_packets = 0

    async def start(self) -> None:
        """Open a nonblocking HCI raw event socket, degrading on all errors."""
        try:
            self._start_sync()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - observer is diagnostic-only
            self.observer_status = "degraded"
            self._diagnostic_failure("ble_link_observer_start_failed", error)

    def _start_sync(self) -> None:
        if self._reader is not None:
            return
        adapter_index = _hci_adapter_index(self.adapter)
        sock = cast(HciSocket, self._socket_factory(_AF_BLUETOOTH, socket.SOCK_RAW, _BTPROTO_HCI))
        try:
            self._native_bind(sock.fileno(), _AF_BLUETOOTH, adapter_index, _HCI_CHANNEL_RAW)
            sock.setblocking(False)
            sock.setsockopt(_HCI_SOL, _HCI_FILTER_OPTION, hci_filter_bytes())
        except BaseException:
            with suppress(BaseException):
                sock.close()
            raise
        self._socket = sock
        self.observer_status = "available"
        self._stop.clear()
        self._reader = threading.Thread(target=self._reader_loop, name="omi-ble-link-reader", daemon=True)
        self._processor = threading.Thread(target=self._processor_loop, name="omi-ble-link-parser", daemon=True)
        self._reader.start()
        self._processor.start()

    async def close(self) -> None:
        """Stop workers, finalize an active record, and swallow all observer errors."""
        try:
            self._close_sync()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - observer is diagnostic-only
            self._diagnostic_failure("ble_link_observer_shutdown_failed", error)

    def _close_sync(self) -> None:
        self._stop.set()
        sock, self._socket = self._socket, None
        if sock is not None:
            try:
                sock.close()
            except BaseException as error:  # noqa: BLE001
                self._diagnostic_failure("ble_link_observer_socket_close_failed", error)
        reader = self._reader
        if reader is not None:
            reader.join(self.config.observer_join_timeout_seconds)
            if reader.is_alive():
                self._diagnostic("ble_link_observer_reader_timeout")
        processor = self._processor
        if processor is not None:
            while True:
                try:
                    self._queue.put_nowait(None)
                    break
                except queue.Full:
                    if not processor.is_alive():
                        self._diagnostic("ble_link_observer_processor_stopped")
                        break
                    self._stop.wait(self.config.observer_poll_seconds)
            processor.join(self.config.observer_join_timeout_seconds)
            if processor.is_alive():
                self._diagnostic("ble_link_observer_processor_timeout")
        self._reader = None
        self._processor = None
        self._finish(None)

    def _reader_loop(self) -> None:
        while not self._stop.is_set():
            sock = self._socket
            if sock is None:
                return
            try:
                packet = sock.recv(self.config.observer_receive_bytes)
            except BlockingIOError:
                self._stop.wait(self.config.observer_poll_seconds)
                continue
            except OSError as error:
                if not self._stop.is_set():
                    self._diagnostic_failure("ble_link_observer_receive_failed", error)
                return
            if not packet:
                continue
            try:
                self._queue.put_nowait(bytes(packet))
            except queue.Full:
                self.dropped_packets += 1
                self._diagnostic("ble_link_observer_queue_overflow", dropped_packets=self.dropped_packets)
                self._warn_once()

    def _processor_loop(self) -> None:
        while True:
            try:
                packet = self._queue.get(timeout=self.config.observer_poll_seconds)
            except queue.Empty:
                continue
            if packet is None:
                return
            try:
                self.handle_packet(packet)
            except BaseException as error:  # noqa: BLE001 - malformed vendor packets are nonfatal
                self._diagnostic_failure("ble_link_observer_parser_failed", error)

    def handle_packet(self, packet: bytes | bytearray) -> None:
        """Parse one packet synchronously; useful for deterministic tests."""
        event = parse_hci_packet(packet, logger=self._debug_logger)
        if event is None:
            return
        if isinstance(event, _ConnectEvent):
            self._handle_connect(event)
            return
        with self._lock:
            active = self._active
        event_handle = getattr(event, "handle", None)
        snapshot_without_handle = isinstance(event, _CommandCompleteEvent) and event_handle is None
        if active is None or (event_handle != active.handle and not snapshot_without_handle):
            return
        if isinstance(event, _ConnectionParameterRequestEvent):
            self._record_connection_parameter_request(event)
        elif isinstance(event, _ConnectionParameterUpdateEvent):
            self._record_connection_parameter_update(event)
        elif isinstance(event, _DataLengthChangeEvent):
            self._record_data_length(event)
        elif isinstance(event, _PhyEvent):
            self._record_phy_update(event)
        elif isinstance(event, _CommandCompleteEvent):
            self._record_phy_snapshot(event)
        elif isinstance(event, _DisconnectEvent):
            self._finish(event.reason)

    def _handle_connect(self, event: _ConnectEvent) -> None:
        if event.address != self.address:
            self._diagnostic("ble_link_address_mismatch", observed_address=event.address)
            return
        with self._lock:
            if self._active is not None:
                self._diagnostic("ble_link_duplicate_connect", handle=event.handle)
                return
            self._active = _ActiveSession(
                event.handle,
                event.interval,
                event.latency,
                event.supervision_timeout,
                self._clock(),
                _connection_parameters(event.interval, event.latency, event.supervision_timeout),
                event.role,
                event.peer_address_type,
                requests=[],
                updates=[],
                transitions=[],
                phy_update_outcomes=[],
                data_length_transitions=[],
            )
            if self.observer_status == "available":
                self._warning_emitted = False
        self._request_phy(event.handle)

    def _record_connection_parameter_request(self, event: _ConnectionParameterRequestEvent) -> None:
        request = ConnectionParameterRequest(
            event.min_interval * _INTERVAL_UNIT_MS,
            event.max_interval * _INTERVAL_UNIT_MS,
            event.latency,
            event.supervision_timeout * _SUPERVISION_TIMEOUT_UNIT_MS,
        )
        with self._lock:
            active = self._active
            requests = active.requests if active is not None else None
            if requests is None:
                return
            if len(requests) < self.config.observer_max_connection_parameter_requests:
                requests.append(request)
            else:
                self._diagnostic("ble_link_connection_parameter_request_overflow")

    def _record_connection_parameter_update(self, event: _ConnectionParameterUpdateEvent) -> None:
        effective_parameters = (
            _connection_parameters(event.interval, event.latency, event.supervision_timeout)
            if event.status == 0
            else None
        )
        update = ConnectionParameterUpdate(
            f"0x{event.status:02x}",
            _hci_status_name(event.status),
            effective_parameters,
        )
        with self._lock:
            active = self._active
            updates = active.updates if active is not None else None
            if active is None or updates is None:
                return
            if len(updates) < self.config.observer_max_connection_parameter_updates:
                updates.append(update)
            else:
                self._diagnostic("ble_link_connection_parameter_update_overflow")
            if event.status == 0:
                active.interval = event.interval
                active.latency = event.latency
                active.supervision_timeout = event.supervision_timeout

    def _request_phy(self, handle: int) -> None:
        sock = self._socket
        if sock is None:
            return
        opcode = _LE_READ_PHY_OPCODE
        command = bytes(
            (
                _HCI_COMMAND_PACKET,
                opcode & 0xFF,
                (opcode >> 8) & 0xFF,
                2,
                handle & 0xFF,
                (handle >> 8) & 0xFF,
            )
        )
        try:
            sock.send(command)
        except BaseException as error:  # noqa: BLE001
            self._diagnostic_failure("ble_link_read_phy_send_failed", error, handle=handle)

    def _record_phy_snapshot(self, event: _CommandCompleteEvent) -> None:
        with self._lock:
            active = self._active
            if active is None or (event.handle is not None and event.handle != active.handle):
                return
            effective = (
                PhyTransition(_phy_name(event.tx_phy), _phy_name(event.rx_phy))
                if event.status == 0 and event.tx_phy is not None and event.rx_phy is not None
                else None
            )
            active.initial_phy_snapshot = PhyOutcome(f"0x{event.status:02x}", _hci_status_name(event.status), effective)
            if effective is not None:
                active.tx_phy, active.rx_phy = effective.tx_phy, effective.rx_phy

    def _record_phy_update(self, event: _PhyEvent) -> None:
        effective = (
            PhyTransition(_phy_name(event.tx_phy), _phy_name(event.rx_phy))
            if event.status == 0 and event.tx_phy is not None and event.rx_phy is not None
            else None
        )
        outcome = PhyOutcome(f"0x{event.status:02x}", _hci_status_name(event.status), effective)
        self._diagnostic(
            "ble_link_phy_update",
            handle=event.handle,
            status_hex=outcome.status_hex,
            status_name=outcome.status_name,
            effective_phy=effective.as_dict() if effective is not None else None,
        )
        with self._lock:
            active = self._active
            if active is None:
                return
            outcomes = active.phy_update_outcomes
            if outcomes is not None:
                if len(outcomes) < self.config.observer_max_phy_update_outcomes:
                    outcomes.append(outcome)
                else:
                    self._diagnostic("ble_link_phy_update_outcome_overflow")
            if effective is None:
                return
            tx_phy, rx_phy = effective.tx_phy, effective.rx_phy
            if active.tx_phy == tx_phy and active.rx_phy == rx_phy:
                return
            active.tx_phy, active.rx_phy = tx_phy, rx_phy
            transitions = active.transitions
            if transitions is None:
                return
            if len(transitions) < self.config.observer_max_phy_transitions:
                transitions.append(PhyTransition(tx_phy, rx_phy))
            else:
                self._diagnostic("ble_link_transition_overflow")

    def _record_data_length(self, event: _DataLengthChangeEvent) -> None:
        transition = DataLengthTransition(
            event.max_tx_octets, event.max_tx_time, event.max_rx_octets, event.max_rx_time
        )
        self._diagnostic("ble_link_data_length_change", handle=event.handle, transition=transition.as_dict())
        with self._lock:
            active = self._active
            if active is None:
                return
            if active.data_length == transition:
                return
            active.data_length = transition
            transitions = active.data_length_transitions
            if transitions is None:
                return
            if len(transitions) < self.config.observer_max_data_length_transitions:
                transitions.append(transition)
            else:
                self._diagnostic("ble_link_data_length_transition_overflow")

    def _finish(self, reason: int | None) -> None:
        with self._lock:
            active, self._active = self._active, None
        if active is None:
            return
        elapsed = max(0.0, self._clock() - active.started)
        record = BleLinkSessionRecord(
            self.address,
            active.handle,
            active.initial_connection_parameters,
            tuple(active.requests or ()),
            tuple(active.updates or ()),
            _connection_parameters(active.interval, active.latency, active.supervision_timeout),
            self.phy_policy,
            active.tx_phy,
            active.rx_phy,
            tuple(active.transitions or ()),
            active.initial_phy_snapshot,
            tuple(active.phy_update_outcomes or ()),
            active.data_length,
            tuple(active.data_length_transitions or ()),
            *_role_name(active.role),
            *_peer_address_type_name(active.peer_address_type),
            elapsed,
            f"0x{reason:02x}" if reason is not None else None,
            _disconnect_reason_name(reason),
            _disconnect_class(reason),
            self.local_name,
            self.observer_status,
            self.dropped_packets,
        )
        payload = record.as_dict()
        try:
            if self._terminal_callback is not None:
                self._terminal_callback(payload)
        except BaseException as error:  # noqa: BLE001
            self._diagnostic_failure("ble_link_terminal_callback_failed", error)
        debug_event("ble_link_session", logger=self._debug_logger, record=payload)

    def _diagnostic(self, event: str, **fields: object) -> None:
        debug_event(event, logger=self._debug_logger, fields=fields)

    def _diagnostic_failure(self, event: str, error: BaseException, **fields: object) -> None:
        debug_exception(event, error, logger=self._debug_logger, fields=fields)
        self.observer_status = "degraded"
        self._warn_once()

    def _warn_once(self) -> None:
        if not self._warning_emitted:
            self._warning_emitted = True
            self._warning_logger.warning("BLE link observer unavailable")


def _disconnect_reason_name(reason: int | None) -> str | None:
    if reason is None:
        return None
    return {
        0x08: "supervision_timeout",
        0x13: "remote_user_terminated",
        0x14: "remote_low_resources",
        0x15: "remote_power_off",
        0x16: "local_host_terminated",
    }.get(reason, "unknown")


def _disconnect_class(reason: int | None) -> str | None:
    if reason is None:
        return None
    return {
        0x08: "timeout",
        0x13: "remote_requested",
        0x14: "remote_requested",
        0x15: "remote_requested",
        0x16: "local_host",
    }.get(reason, "unknown")


async def close_observer(observer: BleLinkObserver | None) -> None:
    """Close an optional observer without allowing it to affect collection."""
    if observer is None:
        return
    task = asyncio.create_task(observer.close())
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        except Exception as error:  # noqa: BLE001 - observer is diagnostic-only
            observer._diagnostic_failure("ble_link_observer_close_failed", error)
        raise
    except Exception as error:  # noqa: BLE001 - observer is diagnostic-only
        observer._diagnostic_failure("ble_link_observer_close_failed", error)
