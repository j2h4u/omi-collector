import asyncio
import ctypes
import errno
import json
import logging
import multiprocessing
import queue
import sys
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Protocol, cast

import pytest

import omi_collector.capture.adapters.ble_link_observability as ble_link_observability
from omi_collector.capture.adapters.ble_link_observability import (
    BleLinkObserver,
    _native_hci_bind,
    close_observer,
    hci_filter_bytes,
    parse_hci_packet,
)
from omi_collector.capture.adapters.debug_logging import close_debug_logging, configure_debug_logging
from omi_collector.config import DEFAULT_CONFIG, DebugLogConfig


def _packet(event: int, payload: bytes) -> bytes:
    return bytes((0x04, event, len(payload))) + payload


def _connect(
    *,
    enhanced: bool = False,
    address: bytes = b"\x06\x05\x04\x03\x02\x01",
    interval: int = 24,
    latency: int = 3,
    supervision_timeout: int = 200,
) -> bytes:
    body = bytearray(30 if enhanced else 18)
    body[1:3] = (0x0042).to_bytes(2, "little")
    body[5:11] = address
    base = 23 if enhanced else 11
    body[base : base + 2] = interval.to_bytes(2, "little")
    body[base + 2 : base + 4] = latency.to_bytes(2, "little")
    body[base + 4 : base + 6] = supervision_timeout.to_bytes(2, "little")
    return _packet(0x3E, bytes((0x0A if enhanced else 0x01,)) + body)


def _phy(tx: int, rx: int) -> bytes:
    return _packet(0x3E, b"\x0c\x00" + (0x42).to_bytes(2, "little") + bytes((tx, rx)))


def _phy_failure(status: int = 0x1A) -> bytes:
    return _packet(0x3E, bytes((0x0C, status)) + (0x42).to_bytes(2, "little") + b"\xff\xff")


def _data_length(
    *, max_tx_octets: int = 251, max_tx_time: int = 2120, max_rx_octets: int = 251, max_rx_time: int = 2120
) -> bytes:
    body = (0x42).to_bytes(2, "little")
    body += max_tx_octets.to_bytes(2, "little") + max_tx_time.to_bytes(2, "little")
    body += max_rx_octets.to_bytes(2, "little") + max_rx_time.to_bytes(2, "little")
    return _packet(0x3E, b"\x07" + body)


def _read_phy_complete(tx: int, rx: int) -> bytes:
    payload = b"\x01" + (0x2030).to_bytes(2, "little") + b"\x00" + (0x42).to_bytes(2, "little") + bytes((tx, rx))
    return _packet(0x0E, payload)


def _read_phy_failure(status: int = 0x1A) -> bytes:
    payload = b"\x01" + (0x2030).to_bytes(2, "little") + bytes((status,))
    return _packet(0x0E, payload)


def _read_rssi_complete(rssi_dbm: int = -47, *, status: int = 0, handle: int = 0x42) -> bytes:
    payload = b"\x01" + (0x1405).to_bytes(2, "little") + bytes((status,)) + handle.to_bytes(2, "little")
    return _packet(0x0E, payload + rssi_dbm.to_bytes(1, "little", signed=True))


def test_parser_preserves_controller_rssi_sentinel_context() -> None:
    event = parse_hci_packet(_read_rssi_complete(127))

    assert event is not None
    assert (event.handle, event.status, event.rssi_dbm) == (0x42, 0, None)  # type: ignore[union-attr]


def test_parser_decodes_signed_controller_rssi_and_preserves_failed_status() -> None:
    success = parse_hci_packet(_read_rssi_complete(-47))
    failure = parse_hci_packet(_read_rssi_complete(status=1))

    assert success is not None and (success.handle, success.status, success.rssi_dbm) == (0x42, 0, -47)  # type: ignore[union-attr]
    assert failure is not None and (failure.handle, failure.status, failure.rssi_dbm) == (None, 1, None)  # type: ignore[union-attr]


def test_observer_logs_only_matching_handle_rssi_observations(caplog: pytest.LogCaptureFixture) -> None:
    debug_logger = logging.getLogger("tests.ble_link.matched_rssi")
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=debug_logger)

    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        observer.handle_packet(_connect())
        observer.handle_packet(_read_rssi_complete(-47, handle=0x42))
        observer.handle_packet(_read_rssi_complete(-60, handle=0x43))
        observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))

    asyncio.run(observer.close())
    events = [
        record
        for record in caplog.records
        if record.name == debug_logger.name and getattr(record, "debug_event", None) == "ble_link_rssi_observed"
    ]
    assert len(events) == 1
    assert events[0].__dict__["debug_fields"] == {
        "handle": 0x42,
        "rssi_dbm": -47,
        "status_hex": "0x00",
        "status_name": "success",
    }


@pytest.mark.parametrize(
    ("packet", "record_method"),
    [
        (_read_rssi_complete(-47), "_record_rssi"),
        (_read_phy_complete(2, 2), "_record_phy_snapshot"),
    ],
    ids=("rssi", "phy_snapshot"),
)
def test_observer_ignores_telemetry_if_disconnect_wins_dispatch_race(
    packet: bytes,
    record_method: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    records: list[dict[str, object]] = []
    debug_logger = logging.getLogger("tests.ble_link.disconnect_dispatch_race")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        terminal_callback=records.append,
        debug_logger=debug_logger,
    )
    observer.handle_packet(_connect())
    dispatch_entered = threading.Event()
    resume_dispatch = threading.Event()
    failures: list[BaseException] = []
    original = cast(Callable[..., None], getattr(observer, record_method))

    def pause_before_record(event: object) -> None:
        dispatch_entered.set()
        if not resume_dispatch.wait(1.0):
            raise TimeoutError("telemetry dispatch was not resumed")
        original(event)

    monkeypatch.setattr(observer, record_method, pause_before_record)

    def dispatch() -> None:
        try:
            observer.handle_packet(packet)
        except BaseException as error:  # noqa: BLE001 - assert race path remains nonfatal
            failures.append(error)

    worker = threading.Thread(target=dispatch)
    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        worker.start()
        try:
            assert dispatch_entered.wait(1.0)
            observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
        finally:
            resume_dispatch.set()
            worker.join(1.0)

    assert not worker.is_alive()
    assert failures == []
    assert len(records) == 1
    assert records[0]["initial_phy_snapshot"] is None
    assert not any(
        record.name == debug_logger.name and getattr(record, "debug_event", None) == "ble_link_rssi_observed"
        for record in caplog.records
    )


@pytest.mark.parametrize(
    ("record_type", "field_count", "field_name"),
    [
        (ble_link_observability.PhyTransition, 2, "tx_phy"),
        (ble_link_observability.PhyOutcome, 3, "status_hex"),
        (ble_link_observability.DataLengthTransition, 4, "max_tx_octets"),
        (ble_link_observability.ConnectionParameters, 3, "interval_ms"),
        (ble_link_observability.ConnectionParameterRequest, 4, "min_interval_ms"),
        (ble_link_observability.ConnectionParameterUpdate, 3, "status_hex"),
        (ble_link_observability.BleLinkSessionRecord, 24, "address"),
        (ble_link_observability._PhyEvent, 4, "handle"),
        (ble_link_observability._DataLengthChangeEvent, 5, "handle"),
        (ble_link_observability._ConnectionParameterRequestEvent, 5, "handle"),
        (ble_link_observability._CommandCompleteEvent, 5, "opcode"),
        (ble_link_observability._DisconnectEvent, 2, "handle"),
    ],
)
def test_observer_telemetry_records_are_immutable(record_type: type[object], field_count: int, field_name: str) -> None:
    record_factory = cast(Callable[..., object], record_type)
    record = record_factory(*([None] * field_count))

    with pytest.raises(FrozenInstanceError):
        setattr(record, field_name, object())


def _connection_update(
    *, status: int = 0, interval: int = 12, latency: int = 0, supervision_timeout: int = 400
) -> bytes:
    body = bytes((status,)) + (0x42).to_bytes(2, "little")
    body += interval.to_bytes(2, "little") + latency.to_bytes(2, "little")
    body += supervision_timeout.to_bytes(2, "little")
    return _packet(0x3E, b"\x03" + body)


def _remote_connection_request(
    *, min_interval: int = 6, max_interval: int = 12, latency: int = 4, supervision_timeout: int = 400
) -> bytes:
    body = (0x42).to_bytes(2, "little")
    body += min_interval.to_bytes(2, "little") + max_interval.to_bytes(2, "little")
    body += latency.to_bytes(2, "little") + supervision_timeout.to_bytes(2, "little")
    return _packet(0x3E, b"\x06" + body)


def test_parser_supports_legacy_enhanced_phy_and_conversions() -> None:
    legacy = parse_hci_packet(_connect())
    enhanced = parse_hci_packet(_connect(enhanced=True))
    assert legacy is not None and enhanced is not None
    assert legacy.handle == enhanced.handle == 0x42  # type: ignore[reportAttributeAccessIssue]
    assert legacy.interval == enhanced.interval == 24  # type: ignore[reportAttributeAccessIssue]
    assert parse_hci_packet(_phy(3, 2)).tx_phy == 3  # type: ignore[union-attr]
    assert parse_hci_packet(_read_phy_complete(2, 3)).rx_phy == 3  # type: ignore[union-attr]


def test_parser_supports_connection_parameter_update_and_remote_request() -> None:
    update = parse_hci_packet(_connection_update())
    request = parse_hci_packet(_remote_connection_request())
    assert update is not None and request is not None
    assert update.handle == request.handle == 0x42  # type: ignore[union-attr]
    assert update.interval == 12  # type: ignore[union-attr]
    assert request.min_interval == 6  # type: ignore[union-attr]
    assert request.max_interval == 12  # type: ignore[union-attr]


def test_parser_stores_connection_role_and_peer_address_type() -> None:
    event = parse_hci_packet(_connect())
    assert event is not None
    assert event.role == 0  # type: ignore[union-attr]
    assert event.peer_address_type == 0  # type: ignore[union-attr]


@pytest.mark.parametrize("size", range(10))
def test_parser_rejects_every_short_data_length_change_body(size: int) -> None:
    assert parse_hci_packet(_packet(0x3E, b"\x07" + bytes(size))) is None


def test_parser_rejects_extra_data_length_change_body_bytes() -> None:
    assert parse_hci_packet(_packet(0x3E, b"\x07" + bytes(11))) is None


def test_parser_stores_data_length_change_layout_exactly() -> None:
    event = parse_hci_packet(_data_length(max_tx_octets=100, max_tx_time=101, max_rx_octets=102, max_rx_time=103))
    assert event is not None
    assert (event.max_tx_octets, event.max_tx_time, event.max_rx_octets, event.max_rx_time) == (100, 101, 102, 103)  # type: ignore[union-attr]


def test_observer_records_phy_snapshot_separately_from_failed_update() -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_read_phy_complete(1, 1))
    observer.handle_packet(_phy_failure())
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))

    record = records[0]
    assert record["initial_phy_snapshot"] == {
        "status_hex": "0x00",
        "status_name": "success",
        "effective_phy": {"tx_phy": "1M", "rx_phy": "1M"},
    }
    assert record["phy_update_outcomes"] == (
        {"status_hex": "0x1a", "status_name": "unsupported_remote_feature", "effective_phy": None},
    )
    assert record["tx_phy"] == "1M"
    assert record["disconnect_class"] == "remote_requested"


def test_observer_keeps_current_phy_beyond_bounded_histories() -> None:
    records: list[dict[str, object]] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_max_phy_transitions=2,
        observer_max_phy_update_outcomes=2,
    )
    observer = BleLinkObserver("01:02:03:04:05:06", config=config, terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_read_phy_complete(1, 1))
    observer.handle_packet(_phy(1, 1))
    observer.handle_packet(_phy(2, 1))
    observer.handle_packet(_phy(2, 2))
    observer.handle_packet(_phy(3, 3))
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))

    record = records[0]
    assert (record["tx_phy"], record["rx_phy"]) == ("coded", "coded")
    assert record["phy_transitions"] == (
        {"tx_phy": "2M", "rx_phy": "1M"},
        {"tx_phy": "2M", "rx_phy": "2M"},
    )
    assert record["phy_update_outcomes"] == (
        {"status_hex": "0x00", "status_name": "success", "effective_phy": {"tx_phy": "1M", "rx_phy": "1M"}},
        {"status_hex": "0x00", "status_name": "success", "effective_phy": {"tx_phy": "2M", "rx_phy": "1M"}},
    )


def test_observer_uses_injected_clock_for_terminal_session_duration() -> None:
    records: list[dict[str, object]] = []
    now = [10.0]
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        clock=lambda: now[0],
        terminal_callback=records.append,
    )
    observer.handle_packet(_connect())
    now[0] = 12.5
    disconnect = _packet(0x05, b"\x00\x42\x00\x13")
    observer.handle_packet(disconnect)
    observer.handle_packet(disconnect)

    assert len(records) == 1
    assert records[0]["duration_seconds"] == 2.5


def test_observer_records_failed_read_phy_snapshot_without_invalid_values() -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_read_phy_failure())
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x08"))

    assert records[0]["initial_phy_snapshot"] == {
        "status_hex": "0x1a",
        "status_name": "unsupported_remote_feature",
        "effective_phy": None,
    }
    assert records[0]["tx_phy"] is None


def test_observer_records_data_length_effective_transitions_with_bound() -> None:
    records: list[dict[str, object]] = []
    config = replace(DEFAULT_CONFIG.ble, observer_max_data_length_transitions=2)
    observer = BleLinkObserver("01:02:03:04:05:06", config=config, terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_data_length())
    observer.handle_packet(_data_length())
    observer.handle_packet(_data_length(max_tx_octets=200))
    observer.handle_packet(_data_length(max_tx_octets=199))
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x08"))

    record = records[0]
    assert record["data_length"] == {
        "max_tx_octets": 199,
        "max_tx_time": 2120,
        "max_rx_octets": 251,
        "max_rx_time": 2120,
    }
    assert record["data_length_transitions"] == (
        {"max_tx_octets": 251, "max_tx_time": 2120, "max_rx_octets": 251, "max_rx_time": 2120},
        {"max_tx_octets": 200, "max_tx_time": 2120, "max_rx_octets": 251, "max_rx_time": 2120},
    )


@pytest.mark.parametrize(
    ("reason", "expected", "reason_name"),
    [
        (0x13, "remote_requested", "remote_user_terminated"),
        (0x14, "remote_requested", "remote_low_resources"),
        (0x15, "remote_requested", "remote_power_off"),
        (0x16, "local_host", "local_host_terminated"),
        (0x08, "timeout", "supervision_timeout"),
        (0x22, "unknown", "unknown"),
    ],
)
def test_observer_disconnect_classifies_only_hci_reason_evidence(reason: int, expected: str, reason_name: str) -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00" + bytes((reason,))))
    assert records[0]["disconnect_class"] == expected
    assert records[0]["disconnect_reason_name"] == reason_name


@pytest.mark.parametrize("packet", [_packet(0x3E, b"\x03" + b"\x00" * 8), _packet(0x3E, b"\x06" + b"\x00" * 9)])
def test_parser_ignores_truncated_connection_parameter_events(packet: bytes) -> None:
    assert parse_hci_packet(packet) is None


@pytest.mark.parametrize(
    "packet",
    [
        _packet(0x3E, b"\x0c\x00\x42\x00\x01"),
        _packet(0x3E, b"\x01" + bytes(17)),
        _packet(0x3E, b"\x0a" + bytes(29)),
        _packet(0x05, b"\x00\x42\x00"),
        _packet(0x0E, b"\x01\x05\x14\x00"),
    ],
)
def test_parser_ignores_truncated_phy_connection_disconnect_and_read_results(packet: bytes) -> None:
    assert parse_hci_packet(packet) is None


def test_parser_does_not_finish_session_from_truncated_disconnect_frame() -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect())

    # The event declares five parameter bytes but contains only four.
    observer.handle_packet(b"\x04\x05\x05\x00\x42\x00\x13")

    assert records == []


@pytest.mark.parametrize("packet", [b"\x04\x05", b"\x04\x05\x00"])
def test_parser_records_short_hci_frames_in_debug_log(packet: bytes, caplog: pytest.LogCaptureFixture) -> None:
    debug_logger = logging.getLogger("tests.ble_link.malformed_packet")

    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        assert parse_hci_packet(packet, logger=debug_logger) is None

    assert any(
        record.name == debug_logger.name and getattr(record, "debug_event", None) == "ble_link_malformed_packet"
        for record in caplog.records
    )


def test_parser_silences_empty_unrelated_event_and_reports_truncated_header(
    caplog: pytest.LogCaptureFixture,
) -> None:
    debug_logger = logging.getLogger("tests.ble_link.unrelated_empty_event")

    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        assert parse_hci_packet(b"\x04\xff\x00", logger=debug_logger) is None

    assert not [record for record in caplog.records if record.name == debug_logger.name]

    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        assert parse_hci_packet(b"\x04\xff", logger=debug_logger) is None

    malformed = [
        record
        for record in caplog.records
        if record.name == debug_logger.name and getattr(record, "debug_event", None) == "ble_link_malformed_packet"
    ]
    assert len(malformed) == 1
    assert malformed[0].__dict__["debug_fields"]["detail"] == "truncated_hci_event"


def test_parser_ignores_acl_malformed_and_mismatched_packets() -> None:
    assert parse_hci_packet(b"\x02\x00\x00") is None
    assert parse_hci_packet(b"\x02\x05\x04\x00\x42\x00\x13") is None
    assert parse_hci_packet(b"\x04\x3e\x10\x01") is None
    assert parse_hci_packet(_connect(address=b"\x10\x10\x10\x10\x10\x10")) is not None


class _FakeSocket:
    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.closed = False
        self.options: list[tuple[int, int, bytes]] = []
        self.file_descriptor = 37

    def fileno(self) -> int:
        return self.file_descriptor

    def bind(self, _address: tuple[int, int]) -> None:
        raise AssertionError("HCI observer must use native libc bind")

    def setblocking(self, _flag: bool) -> None:
        return

    def setsockopt(self, level: int, option: int, value: bytes) -> None:
        self.options.append((level, option, value))

    def recv(self, _size: int) -> bytes:
        raise BlockingIOError

    def send(self, payload: bytes) -> int:
        self.sent.append(payload)
        return len(payload)

    def close(self) -> None:
        self.closed = True


class _FailingSendSocket(_FakeSocket):
    def send(self, payload: bytes) -> int:
        self.sent.append(payload)
        raise OSError("command send failed")


class _QueueShutdownSocket(_FakeSocket):
    def __init__(self) -> None:
        super().__init__()
        self.packets: queue.Queue[bytes] = queue.Queue()
        self.extra_packet_returned = threading.Event()
        self.connect_commands_sent = threading.Event()
        self.close_called = threading.Event()

    def recv(self, _size: int) -> bytes:
        try:
            packet = self.packets.get(timeout=0.005)
        except queue.Empty as error:
            raise BlockingIOError from error
        if packet == b"\x04\xff\x00":
            self.extra_packet_returned.set()
        return packet

    def send(self, payload: bytes) -> int:
        self.sent.append(payload)
        if len(self.sent) == 2:
            self.connect_commands_sent.set()
        return len(payload)

    def close(self) -> None:
        super().close()
        self.close_called.set()


def test_observer_close_stops_idle_reader_before_shutdown_deadline() -> None:
    class ModeAwareSocket(_FakeSocket):
        def __init__(self) -> None:
            super().__init__()
            self.blocking = False
            self.receive_entered = threading.Event()
            self.release_receive = threading.Event()

        def setblocking(self, flag: bool) -> None:
            self.blocking = flag

        def recv(self, _size: int) -> bytes:
            self.receive_entered.set()
            if self.blocking:
                self.release_receive.wait()
                return b""
            raise BlockingIOError

    fake = ModeAwareSocket()
    diagnostics: list[str] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_poll_seconds=0.001,
        observer_join_timeout_seconds=0.03,
    )
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
    )
    observer._diagnostic = lambda event, **_fields: diagnostics.append(event)

    async def scenario() -> None:
        await observer.start()
        reader = observer._reader
        try:
            assert reader is not None
            assert await asyncio.to_thread(fake.receive_entered.wait, 1.0)
            await asyncio.wait_for(observer.close(), timeout=1.0)
            assert not reader.is_alive()
            assert "ble_link_observer_reader_timeout" not in diagnostics
        finally:
            with suppress(Exception):
                await asyncio.wait_for(observer.close(), timeout=1.0)
            fake.release_receive.set()
            if reader is not None:
                await asyncio.to_thread(reader.join, 1.0)
                assert not reader.is_alive()

    asyncio.run(scenario())


class _RestartLifecycleSocket(_FakeSocket):
    def __init__(self, *, late_packet: bytes | None = None, stall_first_receive: bool = False) -> None:
        super().__init__()
        self.receive_entered = threading.Event()
        self.release_receive = threading.Event()
        self.packet_returned = threading.Event()
        self._late_packet = late_packet
        self._stall_first_receive = stall_first_receive
        self._packets: queue.Queue[bytes] = queue.Queue()

    def recv(self, _size: int) -> bytes:
        self.receive_entered.set()
        if self._stall_first_receive:
            self._stall_first_receive = False
            # This barrier models a reader delayed across the bounded close deadline.
            self.release_receive.wait()
            if self._late_packet is not None:
                self.packet_returned.set()
                return self._late_packet
        try:
            packet = self._packets.get_nowait()
        except queue.Empty as error:
            raise BlockingIOError from error
        self.packet_returned.set()
        return packet

    def feed(self, packet: bytes) -> None:
        self._packets.put_nowait(packet)


def test_hci_filter_uses_exact_linux_filter_abi() -> None:
    value = hci_filter_bytes()
    assert len(value) == 16
    type_mask = int.from_bytes(value[0:4], "little")
    event_low = int.from_bytes(value[4:8], "little")
    event_high = int.from_bytes(value[8:12], "little")
    opcode = int.from_bytes(value[12:14], "little")
    assert value[14:16] == b"\x00\x00"
    assert type_mask == 1 << 0x04
    assert event_low == (1 << 0x05) | (1 << 0x0E)
    assert event_high == 1 << (0x3E - 32)
    assert opcode == 0


def test_native_hci_bind_passes_exact_sockaddr_hci_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, bytes, int]] = []

    class FakeBind:
        argtypes: object
        restype: object

        def __call__(self, file_descriptor: int, address: ctypes.c_void_p, length: int) -> int:
            calls.append((file_descriptor, ctypes.string_at(address, length), length))
            return 0

    class FakeLibc:
        bind = FakeBind()

    monkeypatch.setattr(ble_link_observability.ctypes, "CDLL", lambda *_args, **_kwargs: FakeLibc())
    _native_hci_bind(37, 31, 3, 7)
    _native_hci_bind(37, 31, 0, 0)

    assert calls == [
        (37, b"\x1f\x00\x03\x00\x07\x00", 6),
        (37, b"\x1f\x00\x00\x00\x00\x00", 6),
    ]


def test_native_hci_bind_preserves_errno(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeBind:
        argtypes: object
        restype: object

        def __call__(self, _file_descriptor: int, _address: ctypes.c_void_p, _length: int) -> int:
            ctypes.set_errno(errno.EACCES)
            return -1

    class FakeLibc:
        bind = FakeBind()

    monkeypatch.setattr(ble_link_observability.ctypes, "CDLL", lambda *_args, **_kwargs: FakeLibc())
    with pytest.raises(OSError) as raised:
        _native_hci_bind(37, 31, 3, 7)

    assert raised.value.errno == errno.EACCES


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="libc HCI bind errno behavior is Linux-specific")
def test_native_hci_bind_preserves_real_libc_errno() -> None:
    previous_errno = ctypes.get_errno()
    try:
        ctypes.set_errno(0)
        with pytest.raises(OSError) as raised:
            _native_hci_bind(-1, 31, 0, 0)
    finally:
        ctypes.set_errno(previous_errno)

    assert raised.value.errno == errno.EBADF


@pytest.mark.parametrize(
    ("family", "dev", "channel"),
    [(-1, 3, 7), (0x10000, 3, 7), (31, -1, 7), (31, 0xFFFF, 7), (31, 3, -1), (31, 3, 0x10000)],
)
def test_native_hci_bind_rejects_out_of_range_sockaddr_fields(
    family: int, dev: int, channel: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ble_link_observability.ctypes, "CDLL", lambda *_args, **_kwargs: pytest.fail("libc.bind called")
    )

    with pytest.raises(ValueError):
        _native_hci_bind(37, family, dev, channel)


@pytest.mark.parametrize("adapter", ["hci", "hci+1", "hci 1", "hci1x", "HCI1", "hci-1", "hci65535"])
def test_observer_rejects_invalid_adapter_name_before_socket_creation(adapter: str) -> None:
    factory_calls: list[bool] = []
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        adapter=adapter,
        socket_factory=lambda *_args: factory_calls.append(True),  # type: ignore[return-value]
        native_bind=lambda *_args: None,
    )

    asyncio.run(observer.start())

    assert observer.observer_status == "degraded"
    assert factory_calls == []


def test_observer_sends_read_phy_tracks_transition_and_finishes_once() -> None:
    fake = _FakeSocket()
    native_bind_calls: list[tuple[int, int, int, int]] = []
    records: list[dict[str, object]] = []
    clock_value = [10.0]
    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        adapter="hci3",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda fd, family, dev, channel: native_bind_calls.append((fd, family, dev, channel)),
        config=config,
        clock=lambda: clock_value[0],
        terminal_callback=records.append,
    )
    workers: tuple[threading.Thread | None, threading.Thread | None] = (None, None)
    try:
        asyncio.run(observer.start())
        workers = observer._reader, observer._processor
        assert observer._reader is not None and observer._reader.daemon
        assert observer._processor is not None and observer._processor.daemon
        assert native_bind_calls == [(37, 31, 3, 0)]
        assert fake.options == [(0, 2, hci_filter_bytes())]
        # This scenario drives packets and the fake clock synchronously.
        observer._stop.set()
        for worker in workers:
            assert worker is not None
            worker.join(1)
            assert not worker.is_alive()
        observer.handle_packet(_connect())
        assert fake.sent == [b"\x01\x30\x20\x02\x42\x00", b"\x01\x05\x14\x02\x42\x00"]
        observer.handle_packet(_read_phy_complete(1, 1))
        observer.handle_packet(_read_rssi_complete())
        observer.handle_packet(_phy(3, 3))
        clock_value[0] = 40.1
        observer._poll_rssi()
        assert fake.sent[-1] == b"\x01\x05\x14\x02\x42\x00"
        assert len(fake.sent) == 3
        clock_value[0] = 12.5
        observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x08"))
        observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
        asyncio.run(observer.close())
        assert len(records) == 1
        assert records[0]["initial_connection_parameters"] == {
            "interval_ms": 30.0,
            "latency": 3,
            "supervision_timeout_ms": 2000.0,
        }
        assert records[0]["final_connection_parameters"] == records[0]["initial_connection_parameters"]
        assert records[0]["connection_parameter_requests"] == ()
        assert records[0]["connection_parameter_updates"] == ()
        assert records[0]["tx_phy"] == "coded"
        assert records[0]["disconnect_reason_hex"] == "0x08"
        assert records[0]["observer_status"] == "available"
        assert records[0]["dropped_packets"] == 0
    finally:
        asyncio.run(observer.close())
        for worker in workers:
            if worker is not None:
                worker.join(1)
                assert not worker.is_alive()


def test_observer_tracks_bounded_connection_parameter_handshake() -> None:
    records: list[dict[str, object]] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_max_connection_parameter_requests=1,
        observer_max_connection_parameter_updates=1,
    )
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        config=config,
        terminal_callback=records.append,
    )
    observer.handle_packet(_connect(interval=36, latency=3, supervision_timeout=42))
    observer.handle_packet(_remote_connection_request())
    observer.handle_packet(_remote_connection_request(min_interval=8, max_interval=16))
    observer.handle_packet(_connection_update(interval=12, latency=0, supervision_timeout=400))
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x08"))

    assert len(records) == 1
    record = records[0]
    assert record["initial_connection_parameters"] == {
        "interval_ms": 45.0,
        "latency": 3,
        "supervision_timeout_ms": 420.0,
    }
    assert record["connection_parameter_requests"] == (
        {
            "min_interval_ms": 7.5,
            "max_interval_ms": 15.0,
            "latency": 4,
            "supervision_timeout_ms": 4000.0,
        },
    )
    assert record["connection_parameter_updates"] == (
        {
            "status_hex": "0x00",
            "status_name": "success",
            "effective_parameters": {
                "interval_ms": 15.0,
                "latency": 0,
                "supervision_timeout_ms": 4000.0,
            },
        },
    )
    assert record["final_connection_parameters"] == {
        "interval_ms": 15.0,
        "latency": 0,
        "supervision_timeout_ms": 4000.0,
    }
    assert "interval_ms" not in record
    assert record["disconnect_reason_hex"] == "0x08"


def test_observer_stops_update_history_at_exact_cap_but_tracks_latest_parameters() -> None:
    records: list[dict[str, object]] = []
    config = replace(DEFAULT_CONFIG.ble, observer_max_connection_parameter_updates=1)
    observer = BleLinkObserver("01:02:03:04:05:06", config=config, terminal_callback=records.append)
    observer.handle_packet(_connect(interval=36, latency=3, supervision_timeout=42))
    observer.handle_packet(_connection_update(interval=12, latency=0, supervision_timeout=400))
    observer.handle_packet(_connection_update(interval=10, latency=1, supervision_timeout=320))
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))

    record = records[0]
    assert record["connection_parameter_updates"] == (
        {
            "status_hex": "0x00",
            "status_name": "success",
            "effective_parameters": {
                "interval_ms": 15.0,
                "latency": 0,
                "supervision_timeout_ms": 4000.0,
            },
        },
    )
    assert record["final_connection_parameters"] == {
        "interval_ms": 12.5,
        "latency": 1,
        "supervision_timeout_ms": 3200.0,
    }


def test_observer_records_rejected_update_without_effective_parameters() -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect(interval=36, latency=3, supervision_timeout=42))
    observer.handle_packet(_connection_update(status=0x0D, interval=24, latency=0, supervision_timeout=200))
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x08"))

    assert records[0]["connection_parameter_updates"] == (
        {
            "status_hex": "0x0d",
            "status_name": "connection_rejected_limited_resources",
            "effective_parameters": None,
        },
    )
    assert records[0]["final_connection_parameters"] == {
        "interval_ms": 45.0,
        "latency": 3,
        "supervision_timeout_ms": 420.0,
    }


def test_observer_permission_error_is_failure_open() -> None:
    observer = BleLinkObserver(
        "01:02:03:04:05:06", socket_factory=lambda *_args: (_ for _ in ()).throw(PermissionError())
    )
    asyncio.run(observer.start())
    asyncio.run(observer.close())


def test_observer_warns_once_when_initial_phy_and_rssi_commands_fail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _FailingSendSocket()
    debug_logger = logging.getLogger("tests.ble_link.initial_send_failure_debug")
    warning_logger = logging.getLogger("tests.ble_link.initial_send_failure_warning")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        debug_logger=debug_logger,
        warning_logger=warning_logger,
    )

    async def scenario() -> None:
        await observer.start()
        try:
            observer.handle_packet(_connect())
            assert observer.observer_status == "degraded"
            observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    with (
        caplog.at_level(logging.DEBUG, logger=debug_logger.name),
        caplog.at_level(logging.WARNING, logger=warning_logger.name),
    ):
        asyncio.run(scenario())

    assert len(fake.sent) == 2
    assert observer.observer_status == "degraded"
    warnings = [record for record in caplog.records if record.name == warning_logger.name]
    assert [record.getMessage() for record in warnings] == ["BLE link observer unavailable"]
    assert fake.closed


def test_observer_does_not_rearm_failure_warning_while_degraded_across_sessions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _FailingSendSocket()
    warning_logger = logging.getLogger("tests.ble_link.repeated_send_failure_warning")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        warning_logger=warning_logger,
    )

    async def scenario() -> None:
        await observer.start()
        try:
            for _ in range(2):
                observer.handle_packet(_connect())
                assert observer.observer_status == "degraded"
                observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    with caplog.at_level(logging.WARNING, logger=warning_logger.name):
        asyncio.run(scenario())

    warnings = [record for record in caplog.records if record.name == warning_logger.name]
    assert [record.getMessage() for record in warnings] == ["BLE link observer unavailable"]
    assert len(fake.sent) == 4
    assert fake.closed


def test_observer_receive_failure_degrades_and_warns_without_stopping_collection(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingReceiveSocket(_FakeSocket):
        def __init__(self) -> None:
            super().__init__()
            self.receive_attempted = threading.Event()

        def recv(self, _size: int) -> bytes:
            self.receive_attempted.set()
            raise OSError("receive failed")

    fake = FailingReceiveSocket()
    debug_logger = logging.getLogger("tests.ble_link.receive_failure_debug")
    warning_logger = logging.getLogger("tests.ble_link.receive_failure_warning")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        debug_logger=debug_logger,
        warning_logger=warning_logger,
    )

    async def scenario() -> None:
        await observer.start()
        try:
            assert await asyncio.to_thread(fake.receive_attempted.wait, 1.0)
            for _ in range(100):
                if observer.observer_status == "degraded" and "receive failed" in caplog.text:
                    break
                await asyncio.sleep(0.005)
            assert observer.observer_status == "degraded"
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    with (
        caplog.at_level(logging.DEBUG, logger=debug_logger.name),
        caplog.at_level(logging.WARNING, logger=warning_logger.name),
    ):
        asyncio.run(scenario())

    warnings = [record for record in caplog.records if record.name == warning_logger.name]
    assert [record.getMessage() for record in warnings] == ["BLE link observer unavailable"]
    assert fake.closed


def test_observer_start_failure_keeps_traceback_in_debug_ring_and_warning_separate(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    debug_logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.ble_link.debug"))
    warning_logger = logging.getLogger("tests.ble_link.warning")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: (_ for _ in ()).throw(PermissionError("raw HCI payload must stay private")),
        debug_logger=debug_logger,
        warning_logger=warning_logger,
    )

    try:
        with caplog.at_level(logging.WARNING, logger=warning_logger.name):
            asyncio.run(observer.start())
    finally:
        close_debug_logging(debug_logger)

    entries = [
        cast(dict[str, object], json.loads(line)) for line in (tmp_path / "debug.jsonl").read_text().splitlines()
    ]
    failure = cast(
        dict[str, object], next(entry for entry in entries if entry["event"] == "ble_link_observer_start_failed")
    )
    traceback = cast(str, failure["traceback"])
    assert "PermissionError" in traceback
    assert "raw HCI payload must stay private" in traceback
    warning_records = [record for record in caplog.records if record.name == warning_logger.name]
    assert [record.getMessage() for record in warning_records] == ["BLE link observer unavailable"]
    assert "raw HCI payload must stay private" not in caplog.text


def test_observer_restart_refuses_a_live_reader_after_bounded_close() -> None:
    sockets: list[_RestartLifecycleSocket] = []
    factory_calls: list[_RestartLifecycleSocket] = []
    workers: list[threading.Thread] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_poll_seconds=0.001,
        observer_join_timeout_seconds=0.03,
    )

    def socket_factory(*_args: object) -> _RestartLifecycleSocket:
        sock = _RestartLifecycleSocket(stall_first_receive=True)
        sockets.append(sock)
        factory_calls.append(sock)
        return sock

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=socket_factory,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
    )

    async def scenario() -> None:
        try:
            await observer.start()
            reader, processor = observer._reader, observer._processor
            assert reader is not None and processor is not None
            workers.extend((reader, processor))
            assert await asyncio.to_thread(sockets[0].receive_entered.wait, 1)
            await asyncio.wait_for(observer.close(), timeout=1)
            assert reader.is_alive()
            await asyncio.to_thread(processor.join, 1)
            assert not processor.is_alive()

            await observer.start()
            reader = observer._reader
            processor = observer._processor
            workers.extend(worker for worker in (reader, processor) if worker is not None and worker not in workers)
            assert len(factory_calls) == 1
        finally:
            for sock in sockets:
                sock.release_receive.set()
            for worker in (observer._reader, observer._processor):
                if worker is not None and worker not in workers:
                    workers.append(worker)
            with suppress(Exception):
                await asyncio.wait_for(observer.close(), timeout=1)
            for worker in workers:
                await asyncio.to_thread(worker.join, 1)
                assert not worker.is_alive()

    asyncio.run(scenario())


def test_observer_restart_drops_late_packet_from_previous_session() -> None:
    old_socket = _RestartLifecycleSocket(late_packet=_connect(), stall_first_receive=True)
    new_socket = _RestartLifecycleSocket()
    sockets = [old_socket, new_socket]
    factory_calls: list[_RestartLifecycleSocket] = []
    workers: list[threading.Thread] = []
    records: list[dict[str, object]] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_poll_seconds=0.001,
        observer_join_timeout_seconds=0.03,
    )

    def socket_factory(*_args: object) -> _RestartLifecycleSocket:
        sock = sockets[len(factory_calls)]
        factory_calls.append(sock)
        return sock

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=socket_factory,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=records.append,
    )

    async def scenario() -> None:
        try:
            await observer.start()
            old_reader, old_processor = observer._reader, observer._processor
            assert old_reader is not None and old_processor is not None
            workers.extend((old_reader, old_processor))
            assert await asyncio.to_thread(old_socket.receive_entered.wait, 1)
            await asyncio.wait_for(observer.close(), timeout=1)
            assert old_reader.is_alive()
            await asyncio.to_thread(old_processor.join, 1)
            assert not old_processor.is_alive()

            old_socket.release_receive.set()
            await asyncio.to_thread(old_reader.join, 1)
            assert not old_reader.is_alive()
            assert old_socket.packet_returned.is_set()

            await observer.start()
            new_reader, new_processor = observer._reader, observer._processor
            assert new_reader is not None and new_processor is not None
            workers.extend((new_reader, new_processor))
            assert len(factory_calls) == 2
            new_socket.feed(_packet(0x05, b"\x00\x42\x00\x13"))
            assert await asyncio.to_thread(new_socket.packet_returned.wait, 1)
            await asyncio.wait_for(observer.close(), timeout=1)
            assert records == []
        finally:
            for sock in sockets:
                sock.release_receive.set()
            for worker in (observer._reader, observer._processor):
                if worker is not None and worker not in workers:
                    workers.append(worker)
            with suppress(Exception):
                await asyncio.wait_for(observer.close(), timeout=1)
            for worker in workers:
                await asyncio.to_thread(worker.join, 1)
                assert not worker.is_alive()

    asyncio.run(scenario())


def test_observer_shutdown_drains_queued_disconnect_before_finalizing() -> None:
    fake = _FakeSocket()
    records: list[dict[str, object]] = []
    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=records.append,
    )
    asyncio.run(observer.start())
    observer.handle_packet(_connect())
    observer._queue.put(_packet(0x05, b"\x00\x42\x00\x13"))
    asyncio.run(observer.close())
    assert records and records[0]["disconnect_reason_hex"] == "0x13"


def test_reader_delivers_hci_timeline_to_terminal_callback() -> None:
    class ReceivingSocket(_FakeSocket):
        def __init__(self) -> None:
            super().__init__()
            self.received: queue.Queue[bytes] = queue.Queue()

        def recv(self, _size: int) -> bytes:
            try:
                return self.received.get_nowait()
            except queue.Empty as error:
                raise BlockingIOError from error

    fake = ReceivingSocket()
    callback_received = threading.Event()
    records: list[dict[str, object]] = []

    def terminal_callback(record: dict[str, object]) -> None:
        records.append(record)
        callback_received.set()

    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=terminal_callback,
    )

    async def scenario() -> None:
        await observer.start()
        try:
            fake.received.put(_connect())
            fake.received.put(_packet(0x05, b"\x00\x42\x00\x13"))
            assert await asyncio.to_thread(callback_received.wait, 1.0)
            assert len(records) == 1
            assert records[0]["disconnect_reason_hex"] == "0x13"
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    asyncio.run(scenario())
    assert fake.closed


def test_reader_failure_does_not_end_active_physical_session() -> None:
    class ConnectThenFailSocket(_FakeSocket):
        def __init__(self) -> None:
            super().__init__()
            self.failure_raised = threading.Event()
            self.connection_commands_sent = threading.Event()
            self._first_receive = True

        def recv(self, _size: int) -> bytes:
            if self._first_receive:
                self._first_receive = False
                return _connect()
            self.failure_raised.set()
            raise OSError("reader failed after connection")

        def send(self, payload: bytes) -> int:
            self.sent.append(payload)
            if len(self.sent) == 2:
                self.connection_commands_sent.set()
            return len(payload)

    fake = ConnectThenFailSocket()
    records: list[dict[str, object]] = []
    debug_logger = logging.getLogger("tests.ble_link.active_receive_failure")
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        terminal_callback=records.append,
        debug_logger=debug_logger,
    )

    async def scenario() -> None:
        await observer.start()
        try:
            assert await asyncio.to_thread(fake.failure_raised.wait, 1.0)
            assert await asyncio.to_thread(fake.connection_commands_sent.wait, 1.0)
            for _ in range(5):
                await asyncio.sleep(0.01)
                assert records == []
            assert observer.observer_status == "degraded"
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    asyncio.run(scenario())
    assert len(records) == 1
    assert records[0]["disconnect_reason_hex"] is None
    assert records[0]["observer_status"] == "degraded"
    assert fake.closed


def test_rssi_worker_polls_at_thirty_seconds_and_not_before_each_deadline() -> None:
    class PollingSocket(_FakeSocket):
        def __init__(self) -> None:
            super().__init__()
            self.rssi_requests = 0
            self.second_rssi_request = threading.Event()
            self.third_rssi_request = threading.Event()

        def send(self, payload: bytes) -> int:
            self.sent.append(payload)
            if payload[1:3] == b"\x05\x14":
                self.rssi_requests += 1
                if self.rssi_requests == 2:
                    self.second_rssi_request.set()
                elif self.rssi_requests == 3:
                    self.third_rssi_request.set()
            return len(payload)

    fake = PollingSocket()
    clock_value = [0.0]
    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.002)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        clock=lambda: clock_value[0],
    )

    async def scenario() -> None:
        await observer.start()
        try:
            observer.handle_packet(_connect())
            assert fake.rssi_requests == 1
            clock_value[0] = 29.999
            await asyncio.sleep(0.03)
            assert fake.rssi_requests == 1
            clock_value[0] = 30.0
            assert await asyncio.to_thread(fake.second_rssi_request.wait, 1.0)
            assert fake.rssi_requests == 2
            clock_value[0] = 59.999
            await asyncio.sleep(0.03)
            assert fake.rssi_requests == 2
            assert not fake.third_rssi_request.is_set()
        finally:
            await asyncio.wait_for(observer.close(), timeout=1.0)

    asyncio.run(scenario())
    assert fake.closed


def test_observer_shutdown_deadline_does_not_block_loop_on_full_queue_and_stalled_callback() -> None:
    fake = _FakeSocket()
    callback_entered = threading.Event()
    callback_release = threading.Event()
    close_returned = threading.Event()
    records: list[dict[str, object]] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_queue_max_packets=1,
        observer_poll_seconds=0.001,
        observer_join_timeout_seconds=0.03,
    )

    def terminal_callback(record: dict[str, object]) -> None:
        records.append(record)
        callback_entered.set()
        callback_release.wait()

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=terminal_callback,
        clock=lambda: 0.0,
    )
    watchdog = threading.Thread(target=lambda: _release_after_timeout(close_returned, callback_release), daemon=True)
    watchdog.start()

    async def scenario() -> tuple[threading.Thread, float]:
        await observer.start()
        observer.handle_packet(_connect())
        observer._queue.put(_packet(0x05, b"\x00\x42\x00\x13"))
        assert callback_entered.wait(timeout=1)
        observer._queue.put(b"queued behind blocked callback")
        processor = observer._processor
        assert processor is not None
        loop_progressed = asyncio.Event()
        asyncio.get_running_loop().call_later(0.001, loop_progressed.set)
        close_task = asyncio.create_task(observer.close())
        await asyncio.wait_for(loop_progressed.wait(), timeout=0.05)
        assert not close_task.done()
        started = time.monotonic()
        await close_task
        await observer.start()
        assert observer._processor is processor
        return processor, time.monotonic() - started

    try:
        processor, elapsed = asyncio.run(scenario())
        close_returned.set()
        assert elapsed < 0.15
    finally:
        callback_release.set()
        close_returned.set()
        watchdog.join(timeout=1)

    processor.join(timeout=1)
    assert not watchdog.is_alive()
    assert not processor.is_alive()
    assert len(records) == 1


def test_observer_shutdown_finalizer_is_bounded_off_loop_without_processor() -> None:
    callback_entered = threading.Event()
    callback_release = threading.Event()
    close_returned = threading.Event()
    records: list[dict[str, object]] = []
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_poll_seconds=0.001,
        observer_join_timeout_seconds=0.03,
    )

    def terminal_callback(record: dict[str, object]) -> None:
        records.append(record)
        callback_entered.set()
        callback_release.wait()

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        config=config,
        terminal_callback=terminal_callback,
        clock=lambda: 0.0,
    )
    observer.handle_packet(_connect())
    watchdog = threading.Thread(target=lambda: _release_after_timeout(close_returned, callback_release), daemon=True)
    watchdog.start()

    async def scenario() -> tuple[threading.Thread, float]:
        loop_progressed = asyncio.Event()
        asyncio.get_running_loop().call_later(0.001, loop_progressed.set)
        close_task = asyncio.create_task(observer.close())
        await asyncio.wait_for(loop_progressed.wait(), timeout=0.05)
        assert not close_task.done()
        started = time.monotonic()
        await close_task
        finalizer = observer._shutdown_finalizer
        assert finalizer is not None
        return finalizer, time.monotonic() - started

    try:
        finalizer, elapsed = asyncio.run(scenario())
        close_returned.set()
        assert callback_entered.is_set()
        assert elapsed < 0.15
    finally:
        callback_release.set()
        close_returned.set()
        watchdog.join(timeout=1)

    finalizer.join(timeout=1)
    assert not watchdog.is_alive()
    assert not finalizer.is_alive()
    assert len(records) == 1


class _ProcessSignal(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


def _assert_child_module_path(expected_module_path: str) -> None:
    assert Path(ble_link_observability.__file__).resolve() == Path(expected_module_path)


def _blocked_reader_child(entered: _ProcessSignal, close_returned: _ProcessSignal, expected_module_path: str) -> None:
    _assert_child_module_path(expected_module_path)
    blocked = threading.Event()

    class BlockedSocket(_FakeSocket):
        def recv(self, _size: int) -> bytes:
            entered.set()
            blocked.wait()
            return b""

        def close(self) -> None:
            self.closed = True

    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001, observer_join_timeout_seconds=0.02)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: BlockedSocket(),  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
    )

    async def scenario() -> None:
        await observer.start()
        if not entered.wait(1.0):
            raise RuntimeError("reader did not enter blocked receive")
        await observer.close()
        close_returned.set()

    asyncio.run(scenario())


def _blocked_parser_child(entered: _ProcessSignal, close_returned: _ProcessSignal, expected_module_path: str) -> None:
    _assert_child_module_path(expected_module_path)
    packets: queue.Queue[bytes] = queue.Queue()
    blocked = threading.Event()

    class PacketSocket(_FakeSocket):
        def recv(self, _size: int) -> bytes:
            try:
                return packets.get(timeout=0.005)
            except queue.Empty as error:
                raise BlockingIOError from error

        def close(self) -> None:
            self.closed = True

    def terminal_callback(_record: dict[str, object]) -> None:
        entered.set()
        blocked.wait()

    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001, observer_join_timeout_seconds=0.02)
    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: PacketSocket(),  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=terminal_callback,
    )

    async def scenario() -> None:
        await observer.start()
        packets.put(_connect())
        packets.put(_packet(0x05, b"\x00\x42\x00\x13"))
        if not entered.wait(1.0):
            raise RuntimeError("parser callback did not enter blocked state")
        await observer.close()
        close_returned.set()

    asyncio.run(scenario())


def _blocked_finalizer_child(
    entered: _ProcessSignal, close_returned: _ProcessSignal, expected_module_path: str
) -> None:
    _assert_child_module_path(expected_module_path)
    blocked = threading.Event()

    def terminal_callback(_record: dict[str, object]) -> None:
        entered.set()
        blocked.wait()

    config = replace(DEFAULT_CONFIG.ble, observer_poll_seconds=0.001, observer_join_timeout_seconds=0.02)
    observer = BleLinkObserver("01:02:03:04:05:06", config=config, terminal_callback=terminal_callback)
    observer.handle_packet(_connect())

    async def scenario() -> None:
        await observer.close()
        close_returned.set()

    asyncio.run(scenario())


def _assert_blocked_worker_does_not_hold_process_open(
    target: Callable[[_ProcessSignal, _ProcessSignal, str], None],
) -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("public observer process-exit check requires fork")
    context = multiprocessing.get_context("fork")
    entered = context.Event()
    close_returned = context.Event()
    module_path = str(Path(ble_link_observability.__file__).resolve())
    process = context.Process(target=target, args=(entered, close_returned, module_path))
    started = False
    try:
        process.start()
        started = True
        assert entered.wait(1.0), "worker did not enter its blocked public callback"
        assert close_returned.wait(1.0), "public close did not return within its bound"
        process.join(timeout=1.0)
        assert process.exitcode == 0, "blocked daemon worker kept the child process alive"
    finally:
        if started and process.is_alive():
            process.terminate()
            process.join(timeout=1.0)
        if started and process.is_alive():
            process.kill()
            process.join(timeout=1.0)
        if started:
            process.close()


def test_blocked_reader_does_not_hold_child_process_open() -> None:
    _assert_blocked_worker_does_not_hold_process_open(_blocked_reader_child)


def test_blocked_parser_callback_does_not_hold_child_process_open() -> None:
    _assert_blocked_worker_does_not_hold_process_open(_blocked_parser_child)


def test_blocked_shutdown_finalizer_does_not_hold_child_process_open() -> None:
    _assert_blocked_worker_does_not_hold_process_open(_blocked_finalizer_child)


def test_close_drains_a_full_public_packet_queue_after_callback_releases(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _QueueShutdownSocket()
    callback_entered = threading.Event()
    callback_release = threading.Event()
    records: list[dict[str, object]] = []
    debug_logger = logging.getLogger("tests.ble_link.public_queue_shutdown")
    config = replace(
        DEFAULT_CONFIG.ble,
        observer_queue_max_packets=1,
        observer_poll_seconds=0.005,
        observer_join_timeout_seconds=0.5,
    )

    def terminal_callback(record: dict[str, object]) -> None:
        records.append(record)
        callback_entered.set()
        callback_release.wait()

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=lambda *_args: None,
        config=config,
        terminal_callback=terminal_callback,
        debug_logger=debug_logger,
    )

    async def scenario() -> None:
        closing: asyncio.Task[None] | None = None
        try:
            await observer.start()
            fake.packets.put(_connect())
            assert await asyncio.to_thread(fake.connect_commands_sent.wait, 1.0)
            fake.packets.put(_packet(0x05, b"\x00\x42\x00\x13"))
            assert await asyncio.to_thread(callback_entered.wait, 1.0)
            fake.packets.put(b"\x04\xff\x00")
            assert await asyncio.to_thread(fake.extra_packet_returned.wait, 1.0)
            await asyncio.sleep(0.02)
            closing = asyncio.create_task(observer.close())
            assert await asyncio.to_thread(fake.close_called.wait, 1.0)
            await asyncio.sleep(0.04)
            assert not closing.done()
            callback_release.set()
            await asyncio.wait_for(closing, timeout=0.4)
        finally:
            callback_release.set()
            if closing is None:
                closing = asyncio.create_task(observer.close())
            await asyncio.wait_for(closing, timeout=1.0)

    with caplog.at_level(logging.DEBUG, logger=debug_logger.name):
        asyncio.run(scenario())

    assert len(records) == 1
    shutdown_errors = {
        "ble_link_observer_processor_stopped",
        "ble_link_observer_processor_timeout",
        "ble_link_observer_finalizer_timeout",
    }
    observed = {getattr(record, "debug_event", None) for record in caplog.records if record.name == debug_logger.name}
    assert not shutdown_errors.intersection(observed)
    assert fake.closed


def _release_after_timeout(close_returned: threading.Event, callback_release: threading.Event) -> None:
    if not close_returned.wait(timeout=0.2):
        callback_release.set()


def test_observer_native_bind_failure_closes_socket() -> None:
    fake = _FakeSocket()

    def fail_native_bind(_fd: int, _family: int, _dev: int, _channel: int) -> None:
        raise OSError(97, "address family not supported")

    observer = BleLinkObserver(
        "01:02:03:04:05:06",
        socket_factory=lambda *_args: fake,  # type: ignore[reportArgumentType]
        native_bind=fail_native_bind,
    )
    asyncio.run(observer.start())

    assert fake.closed
    assert observer.observer_status == "degraded"


def test_close_observer_waits_for_cleanup_before_propagating_cancellation() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowObserver:
        async def close(self) -> None:
            entered.set()
            await release.wait()

    async def scenario() -> None:
        task = asyncio.create_task(close_observer(SlowObserver()))  # type: ignore[arg-type]
        await entered.wait()
        task.cancel()
        release.set()
        with suppress(asyncio.CancelledError):
            await task
        assert task.done()

    asyncio.run(scenario())
