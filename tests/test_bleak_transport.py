import asyncio
import warnings
from collections.abc import Callable, Coroutine, Iterator, Mapping
from dataclasses import replace
from functools import wraps
from struct import pack
from typing import cast
from uuid import UUID

import pytest
from bleak.backends.device import BLEDevice
from bleak.exc import BleakCharacteristicNotFoundError, BleakDeviceNotFoundError, BleakError
from dbus_fast import Message, MessageType, Variant

import omi_collector.capture.adapters.bleak_transport as bleak_transport
from omi_collector.capture.adapters.bleak_transport import (
    BleakRingTransport,
    ClientFactory,
    RingGattValidationError,
    validate_ring_characteristics,
)
from omi_collector.capture.application.collector import RingTransferError
from omi_collector.capture.application.operational_telemetry import (
    TIME_READ_UUID,
    TIME_SERVICE_UUID,
    TIME_WRITE_UUID,
)
from omi_collector.capture.application.ring_transport import (
    CONTROL_CHARACTERISTIC_UUID,
    RING_SERVICE_UUID,
    STATUS_CHARACTERISTIC_UUID,
    CandidateUnavailableError,
    ControlNotificationStream,
    NotificationOverflowError,
    NotificationProtocolError,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)
from omi_collector.capture.domain.ring_protocol import CMD_STOP, RECORD_SIZE, encode_info_command
from omi_collector.config import DEFAULT_CONFIG


def _async_test(function: Callable[[], Coroutine[object, object, None]]) -> Callable[[], None]:
    @wraps(function)
    def wrapper() -> None:
        asyncio.run(function())

    return wrapper


@pytest.fixture(autouse=True)
def _skip_production_preflight_settle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit sessions fast while production retains the upstream 500 ms settle."""
    monkeypatch.setattr(
        bleak_transport,
        "DEFAULT_CONFIG",
        replace(DEFAULT_CONFIG, ble=replace(DEFAULT_CONFIG.ble, preflight_settle_seconds=1e-9)),
    )


class FakeCharacteristic:
    def __init__(self, uuid: str, properties: object, *, obj: object = None) -> None:
        self.uuid = uuid
        self.properties = properties
        self.obj = obj


class FakeService:
    def __init__(self, uuid: str, characteristics: list[FakeCharacteristic]) -> None:
        self.uuid = uuid
        self.characteristics = characteristics

    def get_characteristic(self, uuid: object) -> FakeCharacteristic | None:
        return next((item for item in self.characteristics if item.uuid == str(uuid)), None)


class GetterServices:
    """Service collection whose typed lookup misses before string fallback."""

    def __init__(self, service: FakeService | None) -> None:
        self.service = service
        self.lookups: list[object] = []

    def get_service(self, uuid: object) -> FakeService | None:
        self.lookups.append(uuid)
        return self.service if not isinstance(uuid, UUID) else None

    def __iter__(self) -> Iterator[object]:
        return iter(())


class TypedGetterServices(GetterServices):
    def get_service(self, uuid: object) -> FakeService | None:
        self.lookups.append(uuid)
        return self.service if isinstance(uuid, UUID) else None


class MappingService:
    """Service exposing characteristics through the mapping-shaped backend API."""

    def __init__(self, uuid: str, characteristics: Mapping[str, FakeCharacteristic]) -> None:
        self.uuid = uuid
        self.characteristics = characteristics


class NamedProperty:
    def __init__(self, name: str) -> None:
        self.name = name


class BrokenProperties:
    def __iter__(self) -> Iterator[object]:
        raise TypeError("not iterable")


class FakeClient:
    def __init__(self, events: list[tuple[object, ...]], **options: bool) -> None:
        self.events = events
        self.services = [_ring_service()]
        self.callback: Callable[[object, bytearray], None] | None = None
        self.fail_notify = options.get("fail_notify", False)
        self.disconnect_on_connect = options.get("disconnect_on_connect", False)
        self.disconnect_on_notify = options.get("disconnect_on_notify", False)
        self.fail_stop = options.get("fail_stop", False)
        self.fail_preflight = options.get("fail_preflight", False)
        self.disconnect_on_preflight = options.get("disconnect_on_preflight", False)
        self.fail_disconnect = options.get("fail_disconnect", False)
        self.disconnect_callback: Callable[[object], None] | None = None
        self.disconnected = False
        self.preflight_callback_states: list[bool] = []

    async def connect(self) -> None:
        self.events.append(("connect",))
        if self.disconnect_on_connect and self.disconnect_callback is not None:
            self.disconnect_callback(self)

    async def disconnect(self) -> None:
        self.events.append(("disconnect",))
        self.disconnected = True
        if self.fail_disconnect:
            raise RuntimeError("disconnect failed")

    async def start_notify(self, characteristic: object, callback: Callable[[object, bytearray], None]) -> None:
        self.events.append(("notify", characteristic))
        if self.fail_notify:
            raise RuntimeError("subscribe failed")
        self.callback = callback
        if self.disconnect_on_notify and self.disconnect_callback is not None:
            self.disconnect_callback(self)

    async def stop_notify(self, characteristic: object) -> None:
        self.events.append(("stop_notify", characteristic))
        if self.fail_stop:
            raise RuntimeError("stop notify failed")

    async def read_gatt_char(self, characteristic: object, *, use_cached: bool = False) -> bytearray:
        self.events.append(("read", characteristic, use_cached))
        return bytearray(pack("<IIII", 10, 2, 20, 1))

    async def write_gatt_char(self, characteristic: object, data: bytes, *, response: bool = False) -> None:
        self.events.append(("write", characteristic, data, response))
        if data == bytes((CMD_STOP,)):
            self.preflight_callback_states.append(self.callback is not None)
            if self.fail_preflight:
                raise RuntimeError("preflight failed")
            if self.disconnect_on_preflight and self.disconnect_callback is not None:
                self.disconnect_callback(self)


class WarningMtuClient(FakeClient):
    @property
    def mtu_size(self) -> int:
        warnings.warn("public MTU metadata was accessed", UserWarning, stacklevel=2)
        return 23


def _ring_service(*, control_properties: list[str] | None = None, control_obj: object = None) -> FakeService:
    return FakeService(
        RING_SERVICE_UUID,
        [
            FakeCharacteristic(
                CONTROL_CHARACTERISTIC_UUID,
                control_properties or ["write", "notify"],
                obj=control_obj,
            ),
            FakeCharacteristic(STATUS_CHARACTERISTIC_UUID, ["read"]),
        ],
    )


class FakeBus:
    def __init__(
        self,
        reply: object = None,
        *,
        error: BaseException | None = None,
        events: list[tuple[object, ...]] | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.reply = reply
        self.error = error
        self.events = events
        self.delay_seconds = delay_seconds
        self.messages: list[Message] = []

    async def call(self, message: Message) -> object:
        self.messages.append(message)
        if self.events is not None:
            self.events.append(("dbus", message))
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.error is not None:
            raise self.error
        return self.reply


class FakeBackend:
    def __init__(self, bus: FakeBus | None) -> None:
        self._bus = bus


def test_validate_ring_characteristics_checks_uuid_and_properties() -> None:
    validated = validate_ring_characteristics([_ring_service()])

    assert cast(FakeCharacteristic, validated.control).uuid == CONTROL_CHARACTERISTIC_UUID
    with pytest.raises(RingGattValidationError, match="write and notify"):
        validate_ring_characteristics([_ring_service(control_properties=["write"])])
    with pytest.raises(RingGattValidationError, match="missing ring service"):
        validate_ring_characteristics([])


def test_validate_ring_characteristics_supports_mapping_and_typed_properties() -> None:
    control = FakeCharacteristic(CONTROL_CHARACTERISTIC_UUID, ["write", "notify"])
    status = FakeCharacteristic(STATUS_CHARACTERISTIC_UUID, ["read"])
    service = MappingService(
        RING_SERVICE_UUID,
        {CONTROL_CHARACTERISTIC_UUID: control, STATUS_CHARACTERISTIC_UUID: status},
    )
    services = {RING_SERVICE_UUID: service}

    validated = validate_ring_characteristics(services)

    assert validated.service is service
    assert validated.control is control
    assert validated.status is status

    control.properties = [NamedProperty("CharacteristicProperty.WRITE"), NamedProperty("notify")]
    status.properties = [NamedProperty("CharacteristicProperty.READ")]
    assert validate_ring_characteristics([service]).control is control


@pytest.mark.parametrize(
    ("services", "message"),
    [
        (GetterServices(None), "missing ring service"),
        ([FakeService(RING_SERVICE_UUID, [])], "missing ring control characteristic"),
        (
            [FakeService(RING_SERVICE_UUID, [FakeCharacteristic(CONTROL_CHARACTERISTIC_UUID, ["write", "notify"])])],
            "missing ring status characteristic",
        ),
        (
            [
                FakeService(
                    RING_SERVICE_UUID,
                    [
                        FakeCharacteristic(CONTROL_CHARACTERISTIC_UUID, ["write", "notify"]),
                        FakeCharacteristic(STATUS_CHARACTERISTIC_UUID, ["notify"]),
                    ],
                )
            ],
            "must support read",
        ),
    ],
)
def test_validate_ring_characteristics_rejects_malformed_shapes(services: object, message: str) -> None:
    with pytest.raises(RingGattValidationError, match=message):
        validate_ring_characteristics(services)


def test_validate_ring_characteristics_handles_property_edge_cases() -> None:
    no_properties = FakeCharacteristic(CONTROL_CHARACTERISTIC_UUID, None)
    service = FakeService(RING_SERVICE_UUID, [no_properties])
    with pytest.raises(RingGattValidationError, match="write and notify"):
        validate_ring_characteristics([service])

    broken = FakeCharacteristic(CONTROL_CHARACTERISTIC_UUID, BrokenProperties())
    with pytest.raises(RingGattValidationError, match="write and notify"):
        validate_ring_characteristics([FakeService(RING_SERVICE_UUID, [broken])])


def test_validate_ring_characteristics_uses_getter_string_fallback() -> None:
    service = _ring_service()
    services = GetterServices(service)

    assert validate_ring_characteristics(services).service is service
    assert isinstance(services.lookups[0], UUID)
    assert services.lookups[1] == RING_SERVICE_UUID


def test_validate_ring_characteristics_accepts_typed_service_getter() -> None:
    service = _ring_service()

    assert validate_ring_characteristics(TypedGetterServices(service)).service is service


@_async_test
async def test_bluez_att_mtu_query_requires_live_backend_seam() -> None:
    client = FakeClient([])
    characteristic = _ring_service().characteristics[0]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert (
            await bleak_transport._read_bluez_att_mtu(
                cast(bleak_transport.BleakClientLike, client), characteristic, None, timeout_seconds=0.1
            )
            is None
        )
    assert not caught


@_async_test
async def test_notification_stream_validates_and_closes_idempotently() -> None:
    with pytest.raises(ValueError, match="buffer byte budget must be positive"):
        ControlNotificationStream(0)

    stream = ControlNotificationStream(buffer_bytes=64)
    assert stream.__aiter__() is stream
    stream.feed(memoryview(b"queued"))
    assert stream.buffered_bytes == len(b"queued")
    stream.close()
    assert stream.buffered_bytes == 0
    stream.close()
    assert stream.buffered_bytes == 0
    stream.feed(b"ignored")

    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()


@_async_test
async def test_notification_stream_disconnect_is_terminal_and_idempotent() -> None:
    stream = ControlNotificationStream()
    stream.fail_disconnected()
    stream.fail_disconnected()

    with pytest.raises(RingTransportDisconnectedError, match="disconnected"):
        await stream.__anext__()

    stream.close()
    stream.feed(b"ignored")


@_async_test
async def test_notification_stream_rejects_repeated_empty_payloads_without_queue_growth() -> None:
    stream = ControlNotificationStream(buffer_bytes=64)
    for _ in range(1000):
        stream.feed(b"")

    assert stream.buffered_bytes == 0
    assert stream._queue.qsize() == 0
    with pytest.raises(NotificationProtocolError, match="must not be empty"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_close_after_overflow_discards_pending_bytes() -> None:
    stream = ControlNotificationStream(buffer_bytes=3)
    stream.feed(b"one")
    stream.feed(b"two")
    assert stream.buffered_bytes == 3

    stream.close()
    stream.close()
    assert stream.buffered_bytes == 0
    with pytest.raises(NotificationOverflowError, match="overflowed"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_close_after_disconnect_discards_pending_bytes() -> None:
    stream = ControlNotificationStream(buffer_bytes=3)
    stream.feed(b"one")
    stream.fail_disconnected()
    assert stream.buffered_bytes == 3

    stream.close()
    stream.close()
    assert stream.buffered_bytes == 0
    with pytest.raises(RingTransportDisconnectedError, match="disconnected"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_disconnect_drains_queued_data_in_order() -> None:
    stream = ControlNotificationStream(buffer_bytes=17)
    payloads = [b"first", b"second", b"third"]
    for payload in payloads:
        stream.feed(payload)

    stream.fail_disconnected()

    for payload in payloads:
        assert await stream.__anext__() == payload
    assert stream.buffered_bytes == 0
    with pytest.raises(RingTransportDisconnectedError, match="disconnected"):
        await stream.__anext__()
    assert stream.buffered_bytes == 0


@_async_test
async def test_notification_stream_disconnect_drains_full_queue_then_errors() -> None:
    stream = ControlNotificationStream(buffer_bytes=6)
    stream.feed(b"one")
    stream.feed(b"two")
    stream.fail_disconnected()
    stream.feed(b"ignored")

    assert await stream.__anext__() == b"one"
    assert await stream.__anext__() == b"two"
    with pytest.raises(RingTransportDisconnectedError, match="disconnected"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_overflow_remains_fatal_after_disconnect() -> None:
    stream = ControlNotificationStream(buffer_bytes=3)
    stream.feed(b"one")
    stream.feed(b"two")
    stream.fail_disconnected()

    assert await stream.__anext__() == b"one"
    with pytest.raises(NotificationOverflowError, match="overflowed"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_accepts_exact_byte_boundary_and_accounts_dequeue() -> None:
    stream = ControlNotificationStream(buffer_bytes=8)
    stream.feed(b"1234")
    stream.feed(b"5678")
    assert stream.buffered_bytes == 8

    assert await stream.__anext__() == b"1234"
    assert stream.buffered_bytes == 4
    assert await stream.__anext__() == b"5678"
    assert stream.buffered_bytes == 0


@_async_test
async def test_notification_stream_rejects_first_over_budget_payload_and_drains_fifo() -> None:
    stream = ControlNotificationStream(buffer_bytes=5)
    stream.feed(b"1234")
    stream.feed(b"56")
    stream.feed(b"ignored")

    assert stream.buffered_bytes == 4
    assert await stream.__anext__() == b"1234"
    assert stream.buffered_bytes == 0
    with pytest.raises(NotificationOverflowError, match="overflowed"):
        await stream.__anext__()


@_async_test
async def test_notification_stream_default_budget_accepts_full_maximum_read() -> None:
    audio_bytes = 4096 * RECORD_SIZE
    full_notifications, remainder = divmod(audio_bytes, 19)
    notification_count = full_notifications + (1 if remainder else 0)
    control_payloads = (
        b"\x01\x00",
        b"\x05" + (0).to_bytes(8, "big") + (4096).to_bytes(4, "big"),
        b"\x04\x00" + (4096).to_bytes(8, "big"),
    )
    stream = ControlNotificationStream()

    for payload in control_payloads:
        stream.feed(payload)
    for index in range(notification_count):
        chunk_size = min(19, audio_bytes - index * 19)
        stream.feed(b"\x03" + bytes(chunk_size))

    expected_bytes = sum(map(len, control_payloads)) + audio_bytes + notification_count
    assert stream.buffered_bytes == expected_bytes

    for payload in control_payloads:
        assert await stream.__anext__() == payload
    for index in range(notification_count):
        payload = await stream.__anext__()
        assert payload[0] == 0x03
        assert len(payload) == 1 + min(19, audio_bytes - index * 19)
    assert stream.buffered_bytes == 0


def test_transport_defaults_project_from_central_config() -> None:
    stream = ControlNotificationStream()
    transport = BleakRingTransport("fake")

    assert stream._buffer_bytes == DEFAULT_CONFIG.memory.notification_buffer_bytes
    assert transport._notification_buffer_bytes == DEFAULT_CONFIG.memory.notification_buffer_bytes


def _factory_for(client: FakeClient) -> ClientFactory:
    def factory(_device: str | object, callback: Callable[[object], None]) -> FakeClient:
        client.disconnect_callback = callback
        return client

    return cast(ClientFactory, factory)


class _LinkObserver:
    def __init__(self, events: list[tuple[object, ...]], *, fail_start: bool = False) -> None:
        self.events = events
        self.fail_start = fail_start

    async def start(self) -> None:
        self.events.append(("observer_start",))
        if self.fail_start:
            raise RuntimeError("observer unavailable")

    async def close(self) -> None:
        self.events.append(("observer_close",))


@_async_test
async def test_link_observer_starts_before_bleak_connect_and_closes_after_success() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    observer = _LinkObserver(events)
    transport = BleakRingTransport(
        "fake",
        client_factory=_factory_for(client),
        link_observer_factory=lambda *_args, **_kwargs: cast(object, observer),  # type: ignore[return-value]
    )
    await transport.connect()
    await transport.disconnect()
    assert [event[0] for event in events if event[0] in {"observer_start", "connect", "observer_close"}] == [
        "observer_start",
        "connect",
        "observer_close",
    ]


@_async_test
async def test_link_observer_failure_and_close_are_diagnostic_only() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    observer = _LinkObserver(events, fail_start=True)
    transport = BleakRingTransport(
        "fake",
        client_factory=_factory_for(client),
        link_observer_factory=lambda *_args, **_kwargs: cast(object, observer),  # type: ignore[return-value]
    )
    await transport.connect()
    await transport.disconnect()
    assert "connect" in [event[0] for event in events]
    assert "observer_close" in [event[0] for event in events]


@_async_test
async def test_connect_preflights_stop_before_subscribe_and_reads_fresh_status() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    selected: list[object] = []

    async def choose_device() -> str:
        selected.append("selected")
        return "fake-device"

    def factory(device: str | object, callback: Callable[[object], None]) -> FakeClient:
        selected.append(device)
        del callback
        return client

    transport = BleakRingTransport("unused", client_factory=cast(ClientFactory, factory), device_selector=choose_device)
    session = await transport.connect()
    await session.write_control(encode_info_command())
    status = await session.read_status()

    assert selected == ["selected", "fake-device"]
    assert [event[0] for event in events] == ["connect", "write", "notify", "write", "read"]
    assert events[1][2] == bytes((CMD_STOP,))
    assert events[1][3] is True
    assert client.preflight_callback_states == [False]
    assert events[3][3] is True
    assert events[4][2] is False
    assert status.unread_packets == 2
    await transport.disconnect()
    assert events[-2][0] == "stop_notify"
    assert events[-1][0] == "disconnect"


@_async_test
async def test_notification_callback_copies_mutable_payload() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)

    def factory(_device: str | object, _callback: Callable[[object], None]) -> FakeClient:
        return client

    session = await BleakRingTransport("fake", client_factory=cast(ClientFactory, factory)).connect()
    assert client.callback is not None
    payload = bytearray(b"before")
    client.callback(object(), payload)
    payload[:] = b"after!"

    assert await session.notifications().__anext__() == b"before"


@_async_test
async def test_notification_queue_overflow_is_fatal() -> None:
    client = FakeClient([])

    def factory(_device: str | object, _callback: Callable[[object], None]) -> FakeClient:
        return client

    session = await BleakRingTransport(
        "fake", client_factory=cast(ClientFactory, factory), notification_buffer_bytes=3
    ).connect()
    assert client.callback is not None
    client.callback(object(), bytearray(b"one"))
    client.callback(object(), bytearray(b"two"))

    assert await session.notifications().__anext__() == b"one"
    with pytest.raises(NotificationOverflowError, match="overflowed"):
        await session.notifications().__anext__()


@_async_test
async def test_disconnect_callback_wakes_notification_consumer() -> None:
    client = FakeClient([])

    def factory(_device: str | object, callback: Callable[[object], None]) -> FakeClient:
        client.disconnect_callback = callback
        return client

    session = await BleakRingTransport("fake", client_factory=cast(ClientFactory, factory)).connect()
    pending = asyncio.create_task(cast(Coroutine[object, object, bytes], session.notifications().__anext__()))
    await asyncio.sleep(0)
    assert client.disconnect_callback is not None
    client.disconnect_callback(client)

    with pytest.raises(RingTransportDisconnectedError, match="disconnected"):
        await pending


@_async_test
async def test_connect_failure_disconnects_client() -> None:
    client = FakeClient([], fail_notify=True)

    def factory(_device: str | object, _callback: Callable[[object], None]) -> FakeClient:
        return client

    transport = BleakRingTransport("fake", client_factory=cast(ClientFactory, factory))
    with pytest.raises(RuntimeError, match="subscribe failed"):
        await transport.connect()

    assert client.disconnected


@_async_test
async def test_preflight_write_failure_disconnects_before_subscribe() -> None:
    client = FakeClient([], fail_preflight=True)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RuntimeError, match="preflight failed"):
        await transport.connect()

    assert client.disconnected
    assert not any(event[0] == "notify" for event in client.events)


@_async_test
async def test_preflight_disconnect_fails_before_subscribe_and_cleans_client() -> None:
    client = FakeClient([], disconnect_on_preflight=True)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RingTransportDisconnectedError, match="preflight"):
        await transport.connect()

    assert client.disconnected
    assert not any(event[0] == "notify" for event in client.events)


@_async_test
async def test_cancelling_preflight_settle_disconnects_before_subscribe() -> None:
    client = FakeClient([])
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    original_config = bleak_transport.DEFAULT_CONFIG
    bleak_transport.DEFAULT_CONFIG = replace(
        DEFAULT_CONFIG, ble=replace(DEFAULT_CONFIG.ble, preflight_settle_seconds=60.0)
    )
    try:
        task = asyncio.create_task(transport.connect())
        for _ in range(10):
            await asyncio.sleep(0)
            if any(event[0] == "write" and event[2] == bytes((CMD_STOP,)) for event in client.events):
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        bleak_transport.DEFAULT_CONFIG = original_config

    assert client.disconnected
    assert not any(event[0] == "notify" for event in client.events)


@_async_test
async def test_preflight_repeats_once_for_each_reconnected_physical_session() -> None:
    events: list[tuple[object, ...]] = []
    clients = [FakeClient(events), FakeClient(events)]

    def factory(_device: str | object, callback: Callable[[object], None]) -> FakeClient:
        client = clients.pop(0)
        client.disconnect_callback = callback
        return client

    transport = BleakRingTransport("fake", client_factory=cast(ClientFactory, factory))
    await transport.connect()
    await transport.disconnect()
    await transport.connect()
    await transport.disconnect()

    preflights = [event for event in events if event[0] == "write" and event[2] == bytes((CMD_STOP,))]
    assert len(preflights) == 2


@_async_test
async def test_destructive_clear_command_is_not_exposed() -> None:
    client = FakeClient([])

    def factory(_device: str | object, _callback: Callable[[object], None]) -> FakeClient:
        return client

    session = await BleakRingTransport("fake", client_factory=cast(ClientFactory, factory)).connect()
    with pytest.raises(ValueError, match="must not be empty"):
        await session.write_control(b"")
    with pytest.raises(ValueError, match="unsupported or destructive"):
        await session.write_control(b"\x13")


@_async_test
async def test_optional_time_write_reports_absence_and_performed_write() -> None:
    missing_client = FakeClient([])
    missing_transport = BleakRingTransport("fake", client_factory=_factory_for(missing_client))
    missing_session = await missing_transport.connect()

    assert await missing_session.write_optional_characteristic(TIME_WRITE_UUID, b"\x00" * 4) is False
    writes = [event for event in missing_client.events if event[0] == "write"]
    assert len(writes) == 1
    assert writes[0][2] == bytes((CMD_STOP,))
    await missing_transport.disconnect()

    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    client.services = [
        _ring_service(),
        FakeService(
            TIME_SERVICE_UUID,
            [
                FakeCharacteristic(TIME_READ_UUID, ["read"]),
                FakeCharacteristic(TIME_WRITE_UUID, ["write"]),
            ],
        ),
    ]
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    session = await transport.connect()

    assert await session.write_optional_characteristic(TIME_WRITE_UUID, b"\x01" * 4) is True
    assert events[-1][0] == "write"
    await transport.disconnect()


@_async_test
async def test_optional_read_returns_bytes_for_present_characteristic() -> None:
    for payload in (bytearray(b"\x01\x02"), bytearray()):
        events: list[tuple[object, ...]] = []
        client = FakeClient(events)
        client.services = [
            _ring_service(),
            FakeService(TIME_SERVICE_UUID, [FakeCharacteristic(TIME_READ_UUID, ["read"])]),
        ]

        async def read_gatt_char(
            _characteristic: object,
            *,
            use_cached: bool = False,
            payload: bytearray = payload,
            events: list[tuple[object, ...]] = events,
        ) -> bytearray:
            events.append(("read", use_cached))
            return payload

        client.read_gatt_char = read_gatt_char  # type: ignore[method-assign]
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        session = await transport.connect()

        assert await session.read_optional_characteristic(TIME_READ_UUID.upper()) == bytes(payload)
        assert events[-1] == ("read", False)
        await transport.disconnect()


@_async_test
async def test_optional_read_returns_none_when_missing_or_not_readable() -> None:
    missing_client = FakeClient([])
    missing_transport = BleakRingTransport("fake", client_factory=_factory_for(missing_client))
    missing_session = await missing_transport.connect()
    assert await missing_session.read_optional_characteristic(TIME_READ_UUID) is None
    await missing_transport.disconnect()

    client = FakeClient([])
    client.services = [
        _ring_service(),
        FakeService(TIME_SERVICE_UUID, [FakeCharacteristic(TIME_READ_UUID, ["notify"])]),
    ]
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    session = await transport.connect()
    assert await session.read_optional_characteristic(TIME_READ_UUID) is None
    assert not any(event[0] == "read" for event in client.events)
    await transport.disconnect()


@_async_test
async def test_optional_read_normalizes_backend_failures_and_disconnects() -> None:
    for disconnected, expected in (
        (False, RingTransportUnavailableError),
        (True, RingTransportDisconnectedError),
    ):
        client = FakeClient([])
        client.services = [
            _ring_service(),
            FakeService(TIME_SERVICE_UUID, [FakeCharacteristic(TIME_READ_UUID, ["read"])]),
        ]

        async def fail_read(_characteristic: object, *, use_cached: bool = False) -> bytearray:
            del use_cached
            raise BleakError("link unavailable")

        client.read_gatt_char = fail_read  # type: ignore[method-assign]
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        session = await transport.connect()
        if disconnected:
            session.handle_disconnect()

        with pytest.raises(expected, match="optional read"):
            await session.read_optional_characteristic(TIME_READ_UUID)
        await transport.disconnect()


@_async_test
async def test_optional_read_requires_an_open_session() -> None:
    client = FakeClient([])
    client.services = [
        _ring_service(),
        FakeService(TIME_SERVICE_UUID, [FakeCharacteristic(TIME_READ_UUID, ["read"])]),
    ]
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    session = await transport.connect()
    await transport.disconnect()

    with pytest.raises(RingTransportDisconnectedError, match="closed"):
        await session.read_optional_characteristic(TIME_READ_UUID)


@_async_test
async def test_session_context_and_close_are_idempotent() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    session = await transport.connect()

    async with session as entered:
        assert entered is session
    await session.close()
    with pytest.raises(RingTransportDisconnectedError, match="closed"):
        await session.read_status()
    session.handle_disconnect()

    assert [event[0] for event in events].count("stop_notify") == 1
    assert [event[0] for event in events].count("disconnect") == 1


@_async_test
async def test_session_close_reports_cleanup_failures_but_attempts_both_steps() -> None:
    for fail_stop, fail_disconnect in [
        (True, False),
        (False, True),
        (True, True),
    ]:
        events: list[tuple[object, ...]] = []
        client = FakeClient(events, fail_stop=fail_stop, fail_disconnect=fail_disconnect)
        session = await BleakRingTransport("fake", client_factory=_factory_for(client)).connect()

        with pytest.raises(RingTransportUnavailableError, match="closing ring session"):
            await session.close()
        assert [event[0] for event in events][-2:] == ["stop_notify", "disconnect"]


@_async_test
async def test_cancelling_hanging_stop_notify_still_attempts_disconnect() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    stop_started = asyncio.Event()

    async def hang_stop(characteristic: object) -> None:
        events.append(("stop_notify", characteristic))
        stop_started.set()
        await asyncio.Future()

    client.stop_notify = hang_stop  # type: ignore[method-assign]
    session = await BleakRingTransport("fake", client_factory=_factory_for(client)).connect()
    closing = asyncio.create_task(session.close())
    await stop_started.wait()
    closing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await closing
    assert [event[0] for event in events][-1] == "disconnect"


@_async_test
async def test_session_context_cleanup_failure_does_not_mask_primary_or_cancellation() -> None:
    for primary in (RingTransferError("range loss"), asyncio.CancelledError()):
        client = FakeClient([], fail_stop=True, fail_disconnect=True)
        session = await BleakRingTransport("fake", client_factory=_factory_for(client)).connect()

        with pytest.raises(type(primary)):
            async with session:
                raise primary


@_async_test
async def test_transport_reuses_session_and_disconnect_is_idempotent() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    first = await transport.connect()
    assert await transport.connect() is first
    await transport.disconnect()
    await transport.disconnect()

    assert [event[0] for event in events].count("connect") == 1
    assert [event[0] for event in events].count("disconnect") == 1


@_async_test
async def test_transport_rejects_invalid_queue_and_disconnects_unstarted_client() -> None:
    with pytest.raises(ValueError, match="buffer byte budget must be positive"):
        BleakRingTransport("fake", notification_buffer_bytes=0)

    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    transport._client = cast(bleak_transport.BleakClientLike, client)
    await transport.disconnect()

    assert events == [("disconnect",)]


@_async_test
async def test_transport_context_and_direct_selector() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    transport = BleakRingTransport("unused", client_factory=_factory_for(client), device_selector="selected")

    async with transport as session:
        assert session is await transport.connect()
    assert client.disconnected


@_async_test
async def test_disconnect_callback_during_connect_fails_and_cleans_up() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events, disconnect_on_connect=True)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RingTransportUnavailableError, match="while connecting"):
        await transport.connect()
    assert client.disconnected


@_async_test
async def test_disconnect_callback_during_startup_cleans_session() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events, disconnect_on_notify=True)
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RingTransportUnavailableError, match="startup"):
        await transport.connect()
    assert client.disconnected
    assert [event[0] for event in events][-2:] == ["stop_notify", "disconnect"]


@_async_test
async def test_failed_validation_suppresses_disconnect_cleanup_error() -> None:
    client = FakeClient([], fail_disconnect=True)
    client.services = []
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RingGattValidationError, match="missing ring service"):
        await transport.connect()
    assert client.disconnected


def test_default_client_factory_is_constructed_without_real_bleak(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[object, object]] = []

    class ConstructedClient:
        def __init__(self, device: object, *, disconnected_callback: object) -> None:
            calls.append((device, disconnected_callback))

    monkeypatch.setattr(bleak_transport, "BleakClient", ConstructedClient)

    def callback(_client: object) -> None:
        pass

    result = bleak_transport._default_client_factory("fake-address", callback)

    assert isinstance(result, ConstructedClient)
    assert calls == [("fake-address", callback)]


@_async_test
async def test_expected_backend_connect_failure_is_unavailable() -> None:
    client = FakeClient([])

    async def fail_connect() -> None:
        raise BleakError("not in range")

    client.connect = fail_connect  # type: ignore[method-assign]
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))

    with pytest.raises(RingTransportUnavailableError, match="connecting to Omi"):
        await transport.connect()
    assert client.disconnected


@_async_test
async def test_stale_scanner_candidate_is_distinguished_from_address_fallback() -> None:
    client = FakeClient([])

    async def fail_connect() -> None:
        raise BleakError("org.bluez.Error.UnknownObject")

    client.connect = fail_connect  # type: ignore[method-assign]
    candidate = BLEDevice("AA:BB", "omi", object())
    transport = BleakRingTransport(
        "AA:BB",
        client_factory=cast(
            ClientFactory, lambda _device, callback: (setattr(client, "disconnect_callback", callback), client)[1]
        ),
        device_selector=candidate,
    )

    with pytest.raises(CandidateUnavailableError):
        await transport.connect()
    assert client.disconnected


@_async_test
async def test_session_start_unknown_object_is_not_classified_as_stale_candidate() -> None:
    client = FakeClient([])

    async def fail_start_notify(_characteristic: object, _callback: Callable[[object, bytearray], None]) -> None:
        raise BleakError("org.bluez.Error.UnknownObject")

    client.start_notify = fail_start_notify  # type: ignore[method-assign]
    candidate = BLEDevice("AA:BB", "omi", object())
    transport = BleakRingTransport("AA:BB", client_factory=_factory_for(client), device_selector=candidate)

    with pytest.raises(RingTransportUnavailableError) as raised:
        await transport.connect()
    assert not isinstance(raised.value, CandidateUnavailableError)
    assert client.disconnected


@pytest.mark.parametrize(
    ("error", "stale"),
    [
        (BleakDeviceNotFoundError("AA:BB"), True),
        (BleakError("org.bluez.Error.UnknownObject"), True),
        (BleakCharacteristicNotFoundError("missing-control"), False),
        (BleakError("characteristic not found"), False),
    ],
)
def test_stale_candidate_classifier_only_accepts_device_object_failures(error: BaseException, stale: bool) -> None:
    assert bleak_transport._is_stale_candidate_error(error) is stale


@_async_test
async def test_expected_backend_write_failure_is_unavailable() -> None:
    client = FakeClient([])
    transport = BleakRingTransport("fake", client_factory=_factory_for(client))
    session = await transport.connect()

    async def fail_write(_characteristic: object, _data: bytes, *, response: bool = False) -> None:
        del response
        raise BleakError("link unavailable")

    client.write_gatt_char = fail_write  # type: ignore[method-assign]
    with pytest.raises(RingTransportUnavailableError, match="writing ring control"):
        await session.write_control(encode_info_command())
    await transport.disconnect()


@_async_test
async def test_gatt_handshake_debug_events_follow_connection_boundaries() -> None:
    events: list[tuple[str, Mapping[str, object]]] = []
    client = WarningMtuClient([])
    client.services = [_ring_service(control_obj=("/org/bluez/path", {"MTU": 498}))]
    bus = FakeBus(
        Message(message_type=MessageType.METHOD_RETURN, reply_serial=1, signature="v", body=[Variant("q", 498)]),
        events=cast(list[tuple[object, ...]], client.events),
    )
    client._backend = FakeBackend(bus)  # type: ignore[attr-defined]

    def record(event: str, *args: object, **fields: object) -> None:
        del args
        events.append((event, fields))

    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await transport.connect()
            await transport.disconnect()
        assert not caught
    finally:
        bleak_transport.debug_event = original_debug_event

    names = [event for event, _fields in events]
    expected = [
        "ble_gatt_connect_initiated",
        "ble_gatt_connect_returned",
        "ble_gatt_services_resolved",
        "ble_gatt_required_characteristics_validated",
        "ble_gatt_att_mtu_observed",
        "ble_gatt_notifications_enabled",
        "ble_gatt_local_disconnect_intent",
    ]
    assert [name for name in names if name in expected] == expected
    mtu_event = next(fields for name, fields in events if name == "ble_gatt_att_mtu_observed")
    assert mtu_event["att_mtu"] == 498
    assert mtu_event["att_mtu_source"] == "bluez_dbus_property_get"
    assert [event[0] for event in client.events[:4]] == ["connect", "dbus", "write", "notify"]
    assert len(bus.messages) == 1
    assert bus.messages[0].path == "/org/bluez/path"
    assert bus.messages[0].member == "Get"
    assert bus.messages[0].body == ["org.bluez.GattCharacteristic1", "MTU"]


@_async_test
async def test_missing_bluez_att_mtu_is_diagnostic_only() -> None:
    debug_events: list[tuple[str, Mapping[str, object]]] = []
    client = FakeClient([])

    def record(event: str, *args: object, **fields: object) -> None:
        del args
        debug_events.append((event, fields))

    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        await transport.connect()
        await transport.disconnect()
    finally:
        bleak_transport.debug_event = original_debug_event

    observed = next(fields for event, fields in debug_events if event == "ble_gatt_att_mtu_observed")
    assert observed["att_mtu"] is None
    assert observed["att_mtu_source"] == "unavailable"
    assert [event[0] for event in client.events[:3]] == ["connect", "write", "notify"]
    assert client.preflight_callback_states == [False]


@_async_test
async def test_bluez_att_mtu_query_failures_are_nonfatal_without_warnings() -> None:
    replies = [
        Message(
            message_type=MessageType.ERROR,
            error_name="org.bluez.Error.NoSuchProperty",
            reply_serial=1,
            signature="s",
            body=["MTU"],
        ),
        Message(message_type=MessageType.METHOD_RETURN, reply_serial=1, signature="", body=[]),
        Message(message_type=MessageType.METHOD_RETURN, reply_serial=1, signature="s", body=["498"]),
        Message(
            message_type=MessageType.METHOD_RETURN,
            reply_serial=1,
            signature="v",
            body=[Variant("u", 498)],
        ),
        Message(
            message_type=MessageType.METHOD_RETURN,
            reply_serial=1,
            signature="v",
            body=[Variant("q", 22)],
        ),
    ]
    for reply in replies:
        client = FakeClient([])
        client.services = [_ring_service(control_obj=("/org/bluez/path", {}))]
        bus = FakeBus(reply)
        client._backend = FakeBackend(bus)  # type: ignore[attr-defined]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert (
                await bleak_transport._read_bluez_att_mtu(
                    cast(bleak_transport.BleakClientLike, client),
                    client.services[0].characteristics[0],
                    None,
                    timeout_seconds=0.1,
                )
                is None
            )
        assert not caught


@_async_test
async def test_bluez_att_mtu_query_timeout_is_nonfatal_and_precedes_preflight() -> None:
    events: list[tuple[object, ...]] = []
    client = FakeClient(events)
    client.services = [_ring_service(control_obj=("/org/bluez/path", {}))]
    reply = Message(message_type=MessageType.METHOD_RETURN, reply_serial=1, signature="v", body=[Variant("q", 498)])
    bus = FakeBus(reply, events=events, delay_seconds=0.01)
    client._backend = FakeBackend(bus)  # type: ignore[attr-defined]
    transport = BleakRingTransport("fake", client_factory=_factory_for(client), att_mtu_query_timeout_seconds=0.001)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        session = await transport.connect()
        await transport.disconnect()
    assert not caught
    assert [event[0] for event in events[:4]] == ["connect", "dbus", "write", "notify"]
    assert session.peer_disconnected is False


def test_att_mtu_query_timeout_must_be_positive() -> None:
    with pytest.raises(ValueError, match="att_mtu_query_timeout_seconds"):
        BleakRingTransport("fake", att_mtu_query_timeout_seconds=0.0)


@_async_test
async def test_gatt_disconnect_callback_is_unexpected_only_without_local_intent() -> None:
    events: list[str] = []
    client = FakeClient([])

    def record(event: str, *args: object, **fields: object) -> None:
        del args, fields
        events.append(event)

    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        session = await transport.connect()
        assert client.disconnect_callback is not None
        client.disconnect_callback(client)
        assert events.count("ble_gatt_unexpected_disconnect") == 1

        async def disconnect_with_callback() -> None:
            if client.disconnect_callback is not None:
                client.disconnect_callback(client)

        client.disconnect = disconnect_with_callback  # type: ignore[method-assign]
        await transport.disconnect()
    finally:
        bleak_transport.debug_event = original_debug_event
    assert events.count("ble_gatt_unexpected_disconnect") == 1
    assert events.count("ble_gatt_local_disconnect_intent") == 1
    assert session.peer_disconnected


@_async_test
async def test_delayed_local_disconnect_callback_after_transport_return_is_ignored() -> None:
    debug_events: list[str] = []
    callback: Callable[[object], None] | None = None
    client = FakeClient([])

    def record(event: str, *args: object, **fields: object) -> None:
        del args, fields
        debug_events.append(event)

    def factory(_device: str | object, disconnected: Callable[[object], None]) -> FakeClient:
        nonlocal callback
        callback = disconnected
        return client

    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=cast(ClientFactory, factory))
        await transport.connect()
        await transport.disconnect()
        assert callback is not None
        callback(client)
    finally:
        bleak_transport.debug_event = original_debug_event

    assert debug_events.count("ble_gatt_local_disconnect_intent") == 1
    assert "ble_gatt_unexpected_disconnect" not in debug_events


@_async_test
async def test_stale_client_callback_cannot_change_new_generation() -> None:
    debug_events: list[str] = []
    clients = [FakeClient([]), FakeClient([])]
    first_client = clients[0]
    callbacks: list[Callable[[object], None]] = []

    def record(event: str, *args: object, **fields: object) -> None:
        del args, fields
        debug_events.append(event)

    def factory(_device: str | object, callback: Callable[[object], None]) -> FakeClient:
        callbacks.append(callback)
        return clients.pop(0)

    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=cast(ClientFactory, factory))
        first = await transport.connect()
        await transport.disconnect()
        second = await transport.connect()
        callbacks[0](first_client)
        assert not second.peer_disconnected
        await transport.disconnect()
    finally:
        bleak_transport.debug_event = original_debug_event

    assert not first.peer_disconnected
    assert "ble_gatt_unexpected_disconnect" not in debug_events


@_async_test
async def test_cancelled_disconnect_timeout_keeps_late_callback_local() -> None:
    debug_events: list[str] = []
    client = FakeClient([])
    stop_started = asyncio.Event()
    original_config = bleak_transport.DEFAULT_CONFIG

    def record(event: str, *args: object, **fields: object) -> None:
        del args, fields
        debug_events.append(event)

    async def hang_stop(_characteristic: object) -> None:
        stop_started.set()
        await asyncio.Future()

    async def hang_disconnect() -> None:
        await asyncio.Future()

    client.stop_notify = hang_stop  # type: ignore[method-assign]
    client.disconnect = hang_disconnect  # type: ignore[method-assign]
    bleak_transport.DEFAULT_CONFIG = replace(
        original_config,
        ble=replace(original_config.ble, cancelled_disconnect_timeout_seconds=1e-9),
    )
    original_debug_event = bleak_transport.debug_event
    bleak_transport.debug_event = record
    try:
        transport = BleakRingTransport("fake", client_factory=_factory_for(client))
        session = await transport.connect()
        closing = asyncio.create_task(session.close())
        await stop_started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert client.disconnect_callback is not None
        client.disconnect_callback(client)
    finally:
        bleak_transport.debug_event = original_debug_event
        bleak_transport.DEFAULT_CONFIG = original_config

    assert debug_events.count("ble_gatt_local_disconnect_intent") == 1
    assert "ble_gatt_unexpected_disconnect" not in debug_events
