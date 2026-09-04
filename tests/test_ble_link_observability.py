import asyncio
import ctypes
import errno
import json
import logging
import threading
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import cast

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
    ("reason", "expected"),
    [
        (0x13, "remote_requested"),
        (0x14, "remote_requested"),
        (0x15, "remote_requested"),
        (0x16, "local_host"),
        (0x08, "timeout"),
        (0x22, "unknown"),
    ],
)
def test_observer_disconnect_classifies_only_hci_reason_evidence(reason: int, expected: str) -> None:
    records: list[dict[str, object]] = []
    observer = BleLinkObserver("01:02:03:04:05:06", terminal_callback=records.append)
    observer.handle_packet(_connect())
    observer.handle_packet(_packet(0x05, b"\x00\x42\x00" + bytes((reason,))))
    assert records[0]["disconnect_class"] == expected


@pytest.mark.parametrize("packet", [_packet(0x3E, b"\x03" + b"\x00" * 8), _packet(0x3E, b"\x06" + b"\x00" * 9)])
def test_parser_ignores_truncated_connection_parameter_events(packet: bytes) -> None:
    assert parse_hci_packet(packet) is None


def test_parser_ignores_acl_malformed_and_mismatched_packets() -> None:
    assert parse_hci_packet(b"\x02\x00\x00") is None
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

    assert calls == [(37, b"\x1f\x00\x03\x00\x07\x00", 6)]


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
        local_name="omi",
    )
    asyncio.run(observer.start())
    assert native_bind_calls == [(37, 31, 3, 0)]
    assert fake.options == [(0, 2, hci_filter_bytes())]
    observer.handle_packet(_connect())
    assert fake.sent == [b"\x01\x30\x20\x02\x42\x00"]
    observer.handle_packet(_read_phy_complete(1, 1))
    observer.handle_packet(_phy(3, 3))
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
