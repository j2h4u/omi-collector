"""Bleak implementation of the Omi ring transport boundary."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from math import isfinite
from typing import NoReturn, Protocol, Self, cast
from uuid import UUID

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakDeviceNotFoundError, BleakError
from dbus_fast import Message, MessageType, Variant

from ...config import DEFAULT_CONFIG
from ..application.operational_telemetry import (
    BATTERY_SERVICE_UUID,
    BATTERY_UUID,
    DEVICE_INFO_SERVICE_UUID,
    FIRMWARE_UUID,
    HARDWARE_UUID,
    MANUFACTURER_UUID,
    MODEL_UUID,
    TIME_READ_UUID,
    TIME_SERVICE_UUID,
    TIME_WRITE_UUID,
)
from ..application.presence import PresenceCallback
from ..application.ring_transport import (
    CONTROL_CHARACTERISTIC_UUID,
    RING_SERVICE_UUID,
    STATUS_CHARACTERISTIC_UUID,
    CandidateUnavailableError,
    ControlNotificationStream,
    RingSession,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)
from ..domain.ring_protocol import CMD_ADVANCE, CMD_INFO, CMD_READ, CMD_STOP, RingStatus, parse_status
from .ble_link_observability import BleLinkObserver, TerminalCallback, close_observer
from .debug_logging import debug_event, debug_exception

Device = str | BLEDevice
DeviceSelector = Callable[[], Device | Awaitable[Device]]
DisconnectCallback = Callable[[object], None]
_MIN_ATT_MTU = 23
_MAX_ATT_MTU = 517
_MAX_DBUS_ERROR_NAME_CHARS = 128


class PresenceScannerLike(Protocol):
    async def start(self) -> object: ...

    async def stop(self) -> object: ...


ScannerFactory = Callable[..., PresenceScannerLike]


class RingGattValidationError(ValueError):
    """The connected peer did not expose the expected Omi ring GATT shape."""


class BleakPresenceObserver:
    """Active-scan observer for one exact address, without BlueZ cache lookup."""

    def __init__(
        self,
        address: str,
        adapter: str = DEFAULT_CONFIG.ble.adapter_name,
        *,
        scanner_factory: ScannerFactory | None = None,
    ) -> None:
        self._address = _normalize_address(address)
        self._adapter = adapter
        self._scanner_factory = scanner_factory or BleakScanner
        self._scanner: PresenceScannerLike | None = None

    async def start(self, callback: PresenceCallback) -> None:
        """Start a fresh scan and forward only matching detection callbacks."""
        if self._scanner is not None:
            return

        def detected(device: BLEDevice, _advertisement: object) -> None:
            if _normalize_address(device.address) == self._address:
                # Keep the exact ephemeral BLEDevice, including its BlueZ
                # object path.  A later BleakClient can then connect directly
                # without a second address lookup after the scanner stops.
                callback(device)

        scanner = self._scanner_factory(
            detection_callback=detected,
            bluez={"adapter": self._adapter, "filters": {"DuplicateData": True}},
        )
        self._scanner = scanner
        try:
            await scanner.start()
        except BaseException:
            # A failed BlueZ start must not pin the observer to a dead scanner.
            # Teardown is best effort and must never replace the start error.
            self._scanner = None
            with suppress(BaseException):
                await scanner.stop()
            raise

    async def stop(self) -> None:
        """Stop active observation before a GATT provider is constructed."""
        scanner = self._scanner
        if scanner is not None:
            await scanner.stop()
            self._scanner = None


def _normalize_address(address: str) -> str:
    return address.strip().lower()


class BleakClientLike(Protocol):
    services: object

    async def connect(self) -> object:
        """Connect to the selected peer."""

    async def disconnect(self) -> object:
        """Disconnect from the selected peer."""

    async def start_notify(
        self,
        characteristic: object,
        callback: Callable[[object, bytearray], Awaitable[None] | None],
    ) -> object:
        """Subscribe to a characteristic."""

    async def stop_notify(self, characteristic: object) -> object:
        """Remove a characteristic subscription."""

    async def read_gatt_char(self, characteristic: object, *, use_cached: bool = False) -> bytes | bytearray:
        """Read a characteristic value."""
        ...

    async def write_gatt_char(self, characteristic: object, data: bytes, *, response: bool = False) -> object:
        """Write a characteristic value."""


ClientFactory = Callable[[Device, DisconnectCallback], BleakClientLike]
LinkObserverFactory = Callable[..., BleLinkObserver]


@dataclass(slots=True)
class _ClientLifecycle:
    """Identity-bound state for one client generation."""

    generation: int
    client: BleakClientLike | None = None
    local_disconnect_intent: bool = False


@dataclass(frozen=True, slots=True)
class RingCharacteristics:
    """Validated service and characteristics used by a ring session."""

    service: object
    control: object
    status: object


def _normal_uuid(value: object) -> str:
    return str(value).lower()


def _property_names(characteristic: object) -> set[str]:
    properties = getattr(characteristic, "properties", None)
    if properties is None or isinstance(properties, (str, bytes)):
        return set()
    try:
        values = cast(Iterable[object], properties)
        return {str(getattr(value, "name", value)).rsplit(".", 1)[-1].lower() for value in values}
    except TypeError:
        return set()


def _find_service(services: object, expected_uuid: str) -> object | None:
    getter = getattr(services, "get_service", None)
    if callable(getter):
        service = getter(UUID(expected_uuid))
        if service is None:
            service = getter(expected_uuid)
        if service is not None:
            return service
    candidates = services.values() if isinstance(services, Mapping) else cast(Iterable[object], services)
    return next(
        (candidate for candidate in candidates if _normal_uuid(getattr(candidate, "uuid", "")) == expected_uuid), None
    )


def _find_characteristic(service: object, expected_uuid: str) -> object | None:
    getter = getattr(service, "get_characteristic", None)
    if callable(getter):
        characteristic = getter(UUID(expected_uuid))
        if characteristic is None:
            characteristic = getter(expected_uuid)
        if characteristic is not None:
            return characteristic
    characteristics = getattr(service, "characteristics", ())
    if isinstance(characteristics, Mapping):
        candidates = characteristics.values()
    else:
        candidates = cast(Iterable[object], characteristics)
    return next(
        (candidate for candidate in candidates if _normal_uuid(getattr(candidate, "uuid", "")) == expected_uuid),
        None,
    )


def validate_ring_characteristics(services: object) -> RingCharacteristics:
    """Validate and return the ring GATT service and its required properties."""
    service = _find_service(services, RING_SERVICE_UUID)
    if service is None or _normal_uuid(getattr(service, "uuid", "")) != RING_SERVICE_UUID:
        raise RingGattValidationError(f"missing ring service {RING_SERVICE_UUID}")
    control = _find_characteristic(service, CONTROL_CHARACTERISTIC_UUID)
    if control is None or _normal_uuid(getattr(control, "uuid", "")) != CONTROL_CHARACTERISTIC_UUID:
        raise RingGattValidationError(f"missing ring control characteristic {CONTROL_CHARACTERISTIC_UUID}")
    control_properties = _property_names(control)
    if not {"write", "notify"}.issubset(control_properties):
        raise RingGattValidationError("ring control characteristic must support write and notify")
    status = _find_characteristic(service, STATUS_CHARACTERISTIC_UUID)
    if status is None or _normal_uuid(getattr(status, "uuid", "")) != STATUS_CHARACTERISTIC_UUID:
        raise RingGattValidationError(f"missing ring status characteristic {STATUS_CHARACTERISTIC_UUID}")
    if "read" not in _property_names(status):
        raise RingGattValidationError("ring status characteristic must support read")
    return RingCharacteristics(service=service, control=control, status=status)


class BleakRingSession(RingSession):
    """A connected, notification-first Omi ring session."""

    def __init__(
        self,
        client: BleakClientLike,
        characteristics: RingCharacteristics,
        buffer_bytes: int,
        *,
        debug_logger: logging.Logger | None = None,
        client_lifecycle: _ClientLifecycle | None = None,
    ) -> None:
        self._client = client
        self._characteristics = characteristics
        self._stream = ControlNotificationStream(buffer_bytes)
        self._notifications_started = False
        self._closed = False
        self._peer_disconnected = False
        self._local_disconnect_intent = False
        self._debug_logger = debug_logger
        self._client_lifecycle = client_lifecycle
        self._optional_characteristics = self._discover_optional_characteristics(client.services)

    @staticmethod
    def _discover_optional_characteristics(services: object) -> dict[str, object]:
        """Index optional operational characteristics without making them required."""
        found: dict[str, object] = {}
        for service_uuid, characteristic_uuids in (
            (BATTERY_SERVICE_UUID, (BATTERY_UUID,)),
            (DEVICE_INFO_SERVICE_UUID, (MODEL_UUID, FIRMWARE_UUID, HARDWARE_UUID, MANUFACTURER_UUID)),
            (TIME_SERVICE_UUID, (TIME_READ_UUID, TIME_WRITE_UUID)),
        ):
            service = _find_service(services, service_uuid)
            if service is None:
                continue
            for characteristic_uuid in characteristic_uuids:
                characteristic = _find_characteristic(service, characteristic_uuid)
                if characteristic is not None:
                    found[characteristic_uuid] = characteristic
        return found

    async def start(self) -> None:
        """Subscribe before allowing any control writes."""
        callback: Callable[[object, bytearray], None] = self._notification_callback
        try:
            await self._client.start_notify(self._characteristics.control, callback)
        except Exception as error:  # backend-specific setup failure
            debug_exception("ble_gatt_notifications_enable_failed", error, logger=self._debug_logger)
            raise
        self._notifications_started = True
        debug_event("ble_gatt_notifications_enabled", logger=self._debug_logger)

    def notifications(self) -> AsyncIterator[bytes]:
        return self._stream

    async def read_status(self) -> RingStatus:
        self._ensure_open()
        try:
            payload = await self._client.read_gatt_char(self._characteristics.status, use_cached=False)
        except RingTransportDisconnectedError:
            raise
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            raise _unavailable("reading ring status", error) from error
        return parse_status(bytes(payload))

    async def read_optional_characteristic(self, uuid: str) -> bytes | None:
        """Read one indexed optional characteristic, returning ``None`` if absent."""
        characteristic = self._optional_characteristics.get(uuid.lower())
        if characteristic is None or "read" not in _property_names(characteristic):
            return None
        self._ensure_open()
        try:
            payload = await self._client.read_gatt_char(characteristic, use_cached=False)
        except RingTransportDisconnectedError:
            raise
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            if self._peer_disconnected:
                raise RingTransportDisconnectedError("Omi disconnected during optional read") from error
            raise RingTransportUnavailableError("Omi optional read unavailable") from error
        return bytes(payload)

    async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
        """Write one optional characteristic, reporting whether it exists."""
        characteristic = self._optional_characteristics.get(uuid.lower())
        if characteristic is None:
            return False
        properties = _property_names(characteristic)
        if not {"write", "write-without-response"}.intersection(properties):
            return False
        self._ensure_open()
        try:
            await self._client.write_gatt_char(characteristic, bytes(value), response=True)
            return True
        except RingTransportDisconnectedError:
            raise
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            if self._peer_disconnected:
                raise RingTransportDisconnectedError("Omi disconnected during optional write") from error
            raise RingTransportUnavailableError("Omi optional write unavailable") from error

    async def write_control(self, payload: bytes) -> None:
        self._ensure_open()
        if not payload:
            raise ValueError("ring control payload must not be empty")
        if payload[0] not in {CMD_STOP, CMD_INFO, CMD_READ, CMD_ADVANCE}:
            raise ValueError("unsupported or destructive ring control command")
        try:
            await self._client.write_gatt_char(self._characteristics.control, bytes(payload), response=True)
        except RingTransportDisconnectedError:
            raise
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            if self._peer_disconnected:
                raise RingTransportDisconnectedError("Omi disconnected during ring write") from error
            raise _unavailable("writing ring control", error) from error

    def handle_disconnect(self) -> None:
        if not self._closed:
            self._peer_disconnected = True
            self._stream.fail_disconnected()

    @property
    def peer_disconnected(self) -> bool:
        return self._peer_disconnected

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stream.close()
        stop_error, cancelled = await self._stop_notifications()
        disconnect_error, cancelled = await self._disconnect_client(cancelled)
        if cancelled is not None:
            raise cancelled
        error = stop_error or disconnect_error
        if error is not None:
            raise _cleanup_unavailable("closing ring session", error) from error

    async def _stop_notifications(self) -> tuple[BaseException | None, asyncio.CancelledError | None]:
        if not self._notifications_started:
            return None, None
        try:
            await self._client.stop_notify(self._characteristics.control)
        except asyncio.CancelledError as error:
            return None, error
        except Exception as error:  # noqa: BLE001  # backend-specific cleanup failure
            debug_exception("ble_gatt_notifications_disable_failed", error, logger=self._debug_logger)
            return error, None
        return None, None

    async def _disconnect_client(
        self, cancelled: asyncio.CancelledError | None
    ) -> tuple[BaseException | None, asyncio.CancelledError | None]:
        try:
            if cancelled is None:
                self._local_disconnect_intent = True
                self._mark_local_disconnect_intent()
                debug_event("ble_gatt_local_disconnect_intent", logger=self._debug_logger, reason="session_close")
                await self._client.disconnect()
            else:
                self._local_disconnect_intent = True
                self._mark_local_disconnect_intent()
                await _disconnect_after_cancellation(
                    self._client, logger=self._debug_logger, reason="session_close_after_cancellation"
                )
        except asyncio.CancelledError as exc:
            return None, cancelled or exc
        except Exception as exc:  # noqa: BLE001  # pragma: no cover - backend-specific cleanup failure
            debug_exception("ble_gatt_disconnect_failed", exc, logger=self._debug_logger, reason="session_close")
            return exc, cancelled
        return None, cancelled

    def _mark_local_disconnect_intent(self) -> None:
        if self._client_lifecycle is not None:
            self._client_lifecycle.local_disconnect_intent = True

    @property
    def local_disconnect_intent(self) -> bool:
        """Whether a client disconnect is currently an intentional local cleanup."""
        return self._local_disconnect_intent

    async def __aenter__(self) -> Self:
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type: object, _exc_value: object, traceback: object) -> None:
        try:
            await self.close()
        except asyncio.CancelledError:
            raise
        except RingTransportDisconnectedError, RingTransportUnavailableError:
            if exc_type is None:
                raise

    def _notification_callback(self, _characteristic: object, payload: bytearray) -> None:
        self._stream.feed(payload)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RingTransportDisconnectedError("ring session is closed")


class BleakRingTransport:
    """Injectable Bleak transport; production defaults never scan implicitly."""

    def __init__(  # noqa: PLR0913
        self,
        address: str,
        *,
        client_factory: ClientFactory | None = None,
        device_selector: DeviceSelector | Device | None = None,
        notification_buffer_bytes: int = DEFAULT_CONFIG.memory.notification_buffer_bytes,
        att_mtu_query_timeout_seconds: float = DEFAULT_CONFIG.ble.att_mtu_query_timeout_seconds,
        adapter: str = DEFAULT_CONFIG.ble.adapter_name,
        phy_policy: str = "auto",
        local_name: str | None = None,
        link_observer_factory: LinkObserverFactory | None = None,
        link_terminal_callback: TerminalCallback | None = None,
        debug_logger: logging.Logger | None = None,
    ) -> None:
        if notification_buffer_bytes < 1:
            raise ValueError("notification buffer byte budget must be positive")
        if (
            isinstance(att_mtu_query_timeout_seconds, bool)
            or not isinstance(att_mtu_query_timeout_seconds, (int, float))
            or not isfinite(att_mtu_query_timeout_seconds)
            or att_mtu_query_timeout_seconds <= 0
        ):
            raise ValueError("att_mtu_query_timeout_seconds must be positive")
        self._address = address
        self._client_factory = client_factory or _default_client_factory
        self._device_selector = device_selector
        self._notification_buffer_bytes = notification_buffer_bytes
        self._att_mtu_query_timeout_seconds = att_mtu_query_timeout_seconds
        self._adapter = adapter
        self._phy_policy = phy_policy
        self._local_name = local_name
        self._link_observer_factory = link_observer_factory
        self._link_terminal_callback = link_terminal_callback
        self._debug_logger = debug_logger
        self._link_observer: BleLinkObserver | None = None
        self._client: BleakClientLike | None = None
        self._client_lifecycle: _ClientLifecycle | None = None
        self._client_generation = 0
        self._session: BleakRingSession | None = None
        self._connect_disconnect: RingTransportUnavailableError | None = None

    async def connect(self) -> BleakRingSession:
        if self._session is not None:
            return self._session
        self._connect_disconnect = None
        client: BleakClientLike | None = None
        candidate_selected = False
        try:
            device = await self._select_device()
            candidate_selected = not isinstance(device, str)
            self._client_generation += 1
            lifecycle = _ClientLifecycle(self._client_generation)
            self._client_lifecycle = lifecycle

            def disconnected_callback(callback_client: object) -> None:
                self._on_disconnect(callback_client, lifecycle)

            client = self._client_factory(device, disconnected_callback)
            lifecycle.client = client
            self._client = client
            await self._start_link_observer()
            await self._connect_client(client, candidate_selected)
            if self._connect_disconnect is not None:
                raise self._connect_disconnect
            services = client.services
            debug_event("ble_gatt_services_resolved", logger=self._debug_logger)
            characteristics = validate_ring_characteristics(services)
            debug_event("ble_gatt_required_characteristics_validated", logger=self._debug_logger)
            await self._observe_att_mtu(client, characteristics)
            session = BleakRingSession(
                client,
                characteristics,
                self._notification_buffer_bytes,
                debug_logger=self._debug_logger,
                client_lifecycle=lifecycle,
            )
            self._session = session
            await self._run_ring_preflight(client, characteristics, session)
            await session.start()
            if session.peer_disconnected:
                raise RingTransportUnavailableError("Omi disconnected during ring session startup")
            return session
        except RingGattValidationError, RingTransportUnavailableError, RingTransportDisconnectedError:
            await self._cleanup_failed_connect_if_needed(client)
            await self._close_link_observer()
            raise
        except asyncio.CancelledError:
            await self._cleanup_failed_connect_if_needed(client)
            await self._close_link_observer()
            raise
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            debug_exception("ble_gatt_setup_failed", error, logger=self._debug_logger)
            await self._cleanup_failed_connect_if_needed(client)
            await self._close_link_observer()
            raise _unavailable("starting Omi session", error) from error
        except Exception as error:
            debug_exception("ble_gatt_setup_failed", error, logger=self._debug_logger)
            await self._cleanup_failed_connect_if_needed(client)
            await self._close_link_observer()
            raise

    async def _start_link_observer(self) -> None:
        """Start raw-HCI telemetry immediately before the GATT connect."""
        try:
            if self._link_observer_factory is None:
                observer = BleLinkObserver(
                    self._address,
                    adapter=self._adapter,
                    phy_policy=self._phy_policy,
                    local_name=self._local_name,
                    terminal_callback=self._link_terminal_callback,
                    debug_logger=self._debug_logger,
                )
            else:
                observer = self._link_observer_factory(
                    self._address,
                    adapter=self._adapter,
                    phy_policy=self._phy_policy,
                    local_name=self._local_name,
                    terminal_callback=self._link_terminal_callback,
                    debug_logger=self._debug_logger,
                )
            self._link_observer = observer
            await observer.start()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - observer is diagnostic-only
            from .debug_logging import debug_exception

            debug_exception("ble_link_observer_factory_failed", error, logger=self._debug_logger)

    async def _close_link_observer(self) -> None:
        observer, self._link_observer = self._link_observer, None
        await close_observer(observer)

    async def _run_ring_preflight(
        self, client: BleakClientLike, characteristics: RingCharacteristics, session: BleakRingSession
    ) -> None:
        # The official app sends this write-only STOP before subscribing,
        # then waits 500 ms before ring work begins:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L167-L169
        # The connector implements STOP as a write-only command here:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/devices/connectors/omi_connection.dart#L353-L362
        # Its INFO transaction runs only after notification subscription:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/devices/connectors/omi_connection.dart#L388-L420
        await client.write_gatt_char(characteristics.control, bytes((CMD_STOP,)), response=True)
        if session.peer_disconnected:
            raise RingTransportDisconnectedError("Omi disconnected during ring preflight")
        await asyncio.sleep(DEFAULT_CONFIG.ble.preflight_settle_seconds)
        if session.peer_disconnected:
            raise RingTransportDisconnectedError("Omi disconnected during ring preflight settle")

    async def _observe_att_mtu(self, client: BleakClientLike, characteristics: RingCharacteristics) -> int | None:
        att_mtu = await _read_bluez_att_mtu(
            client,
            characteristics.control,
            self._debug_logger,
            timeout_seconds=self._att_mtu_query_timeout_seconds,
        )
        if self._connect_disconnect is not None:
            raise self._connect_disconnect
        debug_event(
            "ble_gatt_att_mtu_observed",
            logger=self._debug_logger,
            att_mtu=att_mtu,
            att_mtu_source="bluez_dbus_property_get" if att_mtu is not None else "unavailable",
        )
        return att_mtu

    async def _connect_client(self, client: BleakClientLike, candidate_selected: bool) -> None:
        """Connect once, translating stale scanner objects only at this phase."""
        debug_event("ble_gatt_connect_initiated", logger=self._debug_logger)
        try:
            # Only the initial backend connect can prove that the scanner's
            # BlueZ object path has gone stale.  Later GATT setup failures must
            # remain ordinary transport failures; they are not evidence that
            # the candidate itself vanished.
            await client.connect()
        except (BleakError, OSError, TimeoutError, ConnectionError) as error:
            debug_exception("ble_gatt_connect_failed", error, logger=self._debug_logger)
            await self._raise_connect_failure(client, candidate_selected, error)
        else:
            debug_event("ble_gatt_connect_returned", logger=self._debug_logger)

    async def disconnect(self) -> None:
        session, client = self._session, self._client
        try:
            if session is not None:
                await session.close()
            elif client is not None:
                try:
                    lifecycle = self._client_lifecycle
                    if lifecycle is not None:
                        lifecycle.local_disconnect_intent = True
                    debug_event("ble_gatt_local_disconnect_intent", logger=self._debug_logger, reason="transport_close")
                    await client.disconnect()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # backend-specific cleanup failure
                    debug_exception(
                        "ble_gatt_disconnect_failed", error, logger=self._debug_logger, reason="transport_close"
                    )
                    raise _cleanup_unavailable("disconnecting ring transport", error) from error
        finally:
            self._session = None
            self._client = None
            self._client_lifecycle = None
            await self._close_link_observer()

    async def __aenter__(self) -> BleakRingSession:
        return await self.connect()

    async def __aexit__(self, exc_type: object, _exc_value: object, traceback: object) -> None:
        try:
            await self.disconnect()
        except asyncio.CancelledError:
            raise
        except RingTransportDisconnectedError, RingTransportUnavailableError:
            if exc_type is None:
                raise

    async def _select_device(self) -> Device:
        selector = self._device_selector
        if selector is None:
            return self._address
        selected = selector() if callable(selector) else selector
        if inspect.isawaitable(selected):
            selected = await selected
        return cast(Device, selected)

    async def _cleanup_failed_connect(self, client: BleakClientLike) -> None:
        session = self._session
        if session is not None:
            with suppress(Exception):
                await session.close()
        else:
            lifecycle = self._client_lifecycle
            if lifecycle is not None:
                lifecycle.local_disconnect_intent = True
            debug_event("ble_gatt_local_disconnect_intent", logger=self._debug_logger, reason="connect_cleanup")
            with suppress(Exception):
                await client.disconnect()
        self._session = None
        self._client = None
        self._client_lifecycle = None
        await self._close_link_observer()

    async def _cleanup_failed_connect_if_needed(self, client: BleakClientLike | None) -> None:
        if client is not None and self._client is not None:
            await self._cleanup_failed_connect(client)

    async def _raise_connect_failure(
        self, client: BleakClientLike | None, candidate_selected: bool, error: BaseException
    ) -> NoReturn:
        if client is not None:
            await self._cleanup_failed_connect(client)
        if candidate_selected and _is_stale_candidate_error(error):
            raise CandidateUnavailableError("scanner BLE device is stale in BlueZ") from error
        raise _unavailable("connecting to Omi", error) from error

    def _on_disconnect(self, client: object, lifecycle: _ClientLifecycle) -> None:
        if (
            lifecycle is not self._client_lifecycle
            or lifecycle.generation != self._client_generation
            or client is not self._client
            or lifecycle.client is not client
        ):
            return
        session = self._session
        if not lifecycle.local_disconnect_intent and (session is None or not session.local_disconnect_intent):
            debug_event("ble_gatt_unexpected_disconnect", logger=self._debug_logger)
        if session is None:
            self._connect_disconnect = RingTransportUnavailableError("Omi disconnected while connecting")
        else:
            session.handle_disconnect()


def _unavailable(operation: str, error: BaseException) -> RingTransportUnavailableError:
    """Normalize backend connection failures without hiding protocol errors."""
    return RingTransportUnavailableError(f"Omi transport unavailable while {operation}: {type(error).__name__}")


def _is_stale_candidate_error(error: BaseException) -> bool:
    """Recognize stale device objects without swallowing GATT lookup errors."""
    if isinstance(error, BleakDeviceNotFoundError):
        return True
    if not isinstance(error, BleakError):
        return False
    detail = str(error).lower().replace("_", " ")
    return any(marker in detail for marker in ("org.bluez.error.unknownobject", "unknown object", "no such object"))


def _cleanup_unavailable(
    operation: str, error: BaseException
) -> RingTransportDisconnectedError | RingTransportUnavailableError:
    if isinstance(error, RingTransportDisconnectedError):
        return error
    return _unavailable(operation, error)


class _BlueZBus(Protocol):
    async def call(self, message: Message) -> object:
        """Send one low-level D-Bus message and return its reply."""


def _att_mtu_query_seam(
    client: BleakClientLike, characteristic: object, logger: logging.Logger | None
) -> tuple[_BlueZBus, str] | None:
    try:
        backend = cast(object | None, getattr(cast(object, client), "_backend", None))
        bus = cast(_BlueZBus | None, getattr(backend, "_bus", None))
        obj = cast(object | None, getattr(cast(object, characteristic), "obj", None))
        if bus is None:
            raise LookupError("BlueZ D-Bus bus is unavailable")
        if isinstance(obj, (str, bytes, bytearray)) or not isinstance(obj, Sequence) or not obj:
            raise TypeError("BlueZ characteristic object is malformed")
        path = obj[0]
        if not isinstance(path, str) or not path:
            raise TypeError("BlueZ characteristic object path is malformed")
    except Exception:  # noqa: BLE001 - a diagnostic seam must never affect collection
        _att_mtu_query_failure("missing_private_seam", logger)
        return None
    return bus, path


def _att_mtu_query_failure(reason: str, logger: logging.Logger | None, *, error_name: str | None = None) -> None:
    if error_name is None:
        debug_event("ble_gatt_att_mtu_query_failed", logger=logger, reason=reason)
    else:
        debug_event("ble_gatt_att_mtu_query_failed", logger=logger, reason=reason, error_name=error_name)


def _sanitized_dbus_error_name(reply: object) -> str:
    error_name = getattr(reply, "error_name", None)
    if not isinstance(error_name, str) or not error_name or len(error_name) > _MAX_DBUS_ERROR_NAME_CHARS:
        return "<invalid>"
    if any(not (character.isalnum() or character in "._") for character in error_name):
        return "<invalid>"
    return error_name


def _decode_bluez_att_mtu_reply(reply: object, logger: logging.Logger | None) -> int | None:
    mtu_result: int | None = None
    reply_type = getattr(reply, "message_type", None)
    if reply_type == MessageType.ERROR:
        _att_mtu_query_failure("dbus_error_reply", logger, error_name=_sanitized_dbus_error_name(reply))
    elif reply_type != MessageType.METHOD_RETURN:
        _att_mtu_query_failure("malformed_reply_type", logger)
    elif getattr(reply, "signature", None) != "v":
        _att_mtu_query_failure("malformed_reply_signature", logger)
    else:
        body = getattr(reply, "body", None)
        if not isinstance(body, list) or len(body) != 1 or not isinstance(body[0], Variant):
            _att_mtu_query_failure("malformed_reply_body", logger)
        else:
            value = body[0]
            if value.signature != "q":
                _att_mtu_query_failure("malformed_mtu_variant", logger)
            else:
                mtu = cast(object, value.value)
                if isinstance(mtu, int) and not isinstance(mtu, bool) and _MIN_ATT_MTU <= mtu <= _MAX_ATT_MTU:
                    mtu_result = mtu
                else:
                    _att_mtu_query_failure("invalid_mtu_value", logger)
    return mtu_result


async def _read_bluez_att_mtu(
    client: BleakClientLike,
    characteristic: object,
    logger: logging.Logger | None,
    *,
    timeout_seconds: float,
) -> int | None:
    """Passively read the negotiated ATT MTU from BlueZ's live D-Bus property."""
    seam = _att_mtu_query_seam(client, characteristic, logger)
    if seam is None:
        return None
    bus, path = seam

    try:
        async with asyncio.timeout(timeout_seconds):
            reply = await bus.call(
                Message(
                    destination="org.bluez",
                    path=path,
                    interface="org.freedesktop.DBus.Properties",
                    member="Get",
                    signature="ss",
                    body=["org.bluez.GattCharacteristic1", "MTU"],
                )
            )
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        _att_mtu_query_failure("timeout", logger)
        return None
    except Exception:  # noqa: BLE001 - telemetry must not affect collection
        _att_mtu_query_failure("call_failed", logger)
        return None
    return _decode_bluez_att_mtu_reply(reply, logger)


async def _disconnect_after_cancellation(
    client: BleakClientLike,
    *,
    logger: logging.Logger | None = None,
    reason: str = "cancellation",
) -> None:
    """Make one bounded disconnect attempt after stop-notify was cancelled."""
    try:
        async with asyncio.timeout(DEFAULT_CONFIG.ble.cancelled_disconnect_timeout_seconds):
            debug_event("ble_gatt_local_disconnect_intent", logger=logger, reason=reason)
            await client.disconnect()
    except TimeoutError:
        return


def _default_client_factory(device: Device, callback: DisconnectCallback) -> BleakClientLike:
    bleak_callback = cast(Callable[[BleakClient], None], callback)
    return cast(BleakClientLike, BleakClient(device, disconnected_callback=bleak_callback))
