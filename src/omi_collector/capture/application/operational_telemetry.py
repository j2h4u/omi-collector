"""Privacy-safe operational observations for one pendant presence session."""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from inspect import isawaitable
from threading import Thread
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RingInfo, RingStatus

TIME_SERVICE_UUID = "19b10030-e8f2-537e-4f6c-d104768a1214"
TIME_WRITE_UUID = "19b10031-e8f2-537e-4f6c-d104768a1214"
TIME_READ_UUID = "19b10032-e8f2-537e-4f6c-d104768a1214"
BATTERY_SERVICE_UUID = "0000180f-0000-1000-8000-00805f9b34fb"
BATTERY_UUID = "00002a19-0000-1000-8000-00805f9b34fb"
DEVICE_INFO_SERVICE_UUID = "0000180a-0000-1000-8000-00805f9b34fb"
MODEL_UUID = "00002a24-0000-1000-8000-00805f9b34fb"
FIRMWARE_UUID = "00002a26-0000-1000-8000-00805f9b34fb"
HARDWARE_UUID = "00002a27-0000-1000-8000-00805f9b34fb"
MANUFACTURER_UUID = "00002a29-0000-1000-8000-00805f9b34fb"

_TELEMETRY_CONFIG = DEFAULT_CONFIG.telemetry
CLOCK_DRIFT_THRESHOLD_SECONDS = _TELEMETRY_CONFIG.clock_drift_threshold_seconds
MAX_U32 = 2**32 - 1
U32_BYTES = 4
BATTERY_MAX_PERCENT = 100
OPTIONAL_OPERATION_TIMEOUT_SECONDS = _TELEMETRY_CONFIG.optional_operation_timeout_seconds
HOST_CLOCK_PROBE_TIMEOUT_SECONDS = _TELEMETRY_CONFIG.host_clock_probe_timeout_seconds
METADATA_VALUE_MAX_CHARS = 128

type OperationalEvent = Mapping[str, object]
type OperationalEmitter = Callable[[OperationalEvent], object]
type OptionalReader = Callable[[str], Awaitable[bytes | None]]
type OptionalWriter = Callable[[str, bytes], Awaitable[object]]


class OperationalSession(Protocol):
    """Optional characteristic access kept separate from the ring contract."""

    async def read_optional_characteristic(self, uuid: str) -> bytes | None:
        """Return a value, or ``None`` when the characteristic is not exposed."""
        ...

    async def write_optional_characteristic(self, uuid: str, value: bytes) -> object:
        """Write an optional characteristic; false means no write was performed."""
        ...


@dataclass(frozen=True, slots=True)
class TelemetryClock:
    """Injectable wall-clock and trust source used by drift correction."""

    now: Callable[[], float] = time.time
    synchronized: Callable[[], bool] | None = None
    operation_timeout: float = OPTIONAL_OPERATION_TIMEOUT_SECONDS
    host_clock_probe_timeout: float = HOST_CLOCK_PROBE_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class _TimeSample:
    epoch: int | None
    before: float
    after: float
    outcome: str


def system_host_clock_synchronized(timeout: float = HOST_CLOCK_PROBE_TIMEOUT_SECONDS) -> bool:
    """Read systemd's NTP trust flag without invoking a shell."""
    try:
        result = subprocess.run(
            ("timedatectl", "show", "--property=NTPSynchronized", "--value"),
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
    except OSError, subprocess.SubprocessError:
        return False
    return result.returncode == 0 and result.stdout.strip().lower() == "yes"


async def collect_operational_telemetry(
    session: object,
    status: RingStatus | None,
    info: RingInfo,
    emit: OperationalEmitter | None,
    *,
    clock: TelemetryClock | None = None,
) -> None:
    """Best-effort observation and one-shot drift correction for a connection.

    Every backend failure is reduced to a stable classification.  No exception
    text, BLE address, characteristic payload, or audio data enters an event.
    """
    if emit is None:
        return
    telemetry_clock = clock or TelemetryClock()
    if telemetry_clock.operation_timeout <= 0:
        raise ValueError("optional operation timeout must be positive")
    if telemetry_clock.host_clock_probe_timeout <= 0:
        raise ValueError("host clock probe timeout must be positive")
    synchronized = telemetry_clock.synchronized or (
        lambda: system_host_clock_synchronized(telemetry_clock.host_clock_probe_timeout)
    )
    reader, writer = _optional_accessors(session)
    observation, sample = await _build_observation(
        reader, status, info, telemetry_clock.now, telemetry_clock.operation_timeout
    )
    await _emit(emit, observation)
    await _sync_clock(
        _ClockSync(
            reader,
            writer,
            sample,
            synchronized,
            emit,
            telemetry_clock.operation_timeout,
        )
    )


async def _build_observation(
    reader: OptionalReader | None,
    status: RingStatus | None,
    info: RingInfo,
    host_time: Callable[[], float],
    operation_timeout: float,
) -> tuple[dict[str, object], _TimeSample]:
    observation: dict[str, object] = {"event": "pendant_observation"}
    if status is not None:
        observation.update(
            used_bytes=status.used_bytes,
            unread_packets=status.unread_packets,
            free_bytes=status.free_bytes,
            rtc_valid=status.has_valid_rtc,
        )
    observation.update(
        read_sequence=info.read_sequence,
        write_sequence=info.write_sequence,
        capacity_packets=info.capacity_packets,
        dropped_packets=info.dropped_packets,
        packet_size=info.packet_size,
    )
    outcomes: dict[str, str] = {}
    battery, outcomes["battery"] = await _read_battery(reader, operation_timeout)
    if battery is not None:
        observation["battery_percent"] = battery
    for name, uuid in (
        ("model", MODEL_UUID),
        ("firmware", FIRMWARE_UUID),
        ("hardware", HARDWARE_UUID),
        ("manufacturer", MANUFACTURER_UUID),
    ):
        value, outcomes[name] = await _read_text(reader, uuid, operation_timeout)
        if value is not None:
            observation[name] = value
    sample = await _read_time(reader, host_time, operation_timeout)
    outcomes["device_time"] = sample.outcome
    if sample.epoch is not None:
        observation["device_time_epoch"] = sample.epoch
    observation["optional_outcomes"] = outcomes
    return observation, sample


@dataclass(frozen=True, slots=True)
class _ClockSync:
    reader: OptionalReader | None
    writer: OptionalWriter | None
    sample: _TimeSample
    host_clock_synchronized: Callable[[], bool]
    emit: OperationalEmitter
    operation_timeout: float


async def _sync_clock(sync: _ClockSync) -> None:
    reader = sync.reader
    writer = sync.writer
    sample = sync.sample
    host_clock_synchronized = sync.host_clock_synchronized
    emit = sync.emit
    operation_timeout = sync.operation_timeout
    if sample.epoch is None:
        await _emit(
            emit,
            {
                "event": "pendant_clock_sync",
                "action": "skipped",
                "outcome": "device_time_" + sample.outcome,
                "threshold_seconds": CLOCK_DRIFT_THRESHOLD_SECONDS,
            },
        )
        return
    midpoint = (sample.before + sample.after) / 2.0
    drift = float(sample.epoch) - midpoint
    event: dict[str, object] = {
        "event": "pendant_clock_sync",
        "drift_seconds": round(drift, 3),
        "threshold_seconds": CLOCK_DRIFT_THRESHOLD_SECONDS,
    }
    if abs(drift) <= CLOCK_DRIFT_THRESHOLD_SECONDS:
        event.update(action="none", outcome="within_threshold")
        await _emit(emit, event)
        return
    try:
        trusted = await _run_host_clock_probe(host_clock_synchronized, operation_timeout)
    except Exception:  # noqa: BLE001 - trust is best effort
        trusted = False
    event["host_ntp_synchronized"] = trusted
    if not trusted:
        event.update(action="none", outcome="host_unsynchronized")
        await _emit(emit, event)
        return
    target = int(midpoint)
    if not 0 <= target <= MAX_U32:
        event.update(action="none", outcome="target_unrepresentable")
        await _emit(emit, event)
        return
    if writer is None:
        event.update(action="none", outcome="time_write_missing")
        await _emit(emit, event)
        return
    await _write_and_verify(reader, writer, target, event, operation_timeout)
    await _emit(emit, event)


async def _write_and_verify(
    reader: OptionalReader | None,
    writer: OptionalWriter,
    target: int,
    event: dict[str, object],
    operation_timeout: float,
) -> None:
    try:
        performed = await _bounded_optional(
            writer(TIME_WRITE_UUID, target.to_bytes(U32_BYTES, "little")), operation_timeout
        )
    except _OptionalOperationTimeoutError:
        event.update(action="none", outcome="time_write_timeout")
        return
    except Exception:  # noqa: BLE001 - classify optional backend failures
        event.update(action="none", outcome="time_write_failed")
        return
    if performed is False:
        event.update(action="none", outcome="time_write_missing")
        return
    event["target_epoch"] = target
    try:
        verified = (
            _parse_u32(await _bounded_optional(reader(TIME_READ_UUID), operation_timeout))
            if reader is not None
            else None
        )
    except _OptionalOperationTimeoutError:
        verified = None
        event.update(action="written", outcome="verification_timeout")
        return
    except Exception:  # noqa: BLE001 - classify optional backend failures
        verified = None
    event.update(action="written", outcome="verified" if verified == target else "verification_failed")


def _optional_accessors(session: object) -> tuple[OptionalReader | None, OptionalWriter | None]:
    reader = getattr(session, "read_optional_characteristic", None)
    writer = getattr(session, "write_optional_characteristic", None)
    if not callable(reader):
        reader = getattr(session, "read_characteristic", None)
    if not callable(writer):
        writer = getattr(session, "write_characteristic", None)
    return cast(OptionalReader | None, reader), cast(OptionalWriter | None, writer)


async def _read_battery(reader: OptionalReader | None, operation_timeout: float) -> tuple[int | None, str]:
    if reader is None:
        return None, "unsupported"
    try:
        raw = await _bounded_optional(reader(BATTERY_UUID), operation_timeout)
    except _OptionalOperationTimeoutError:
        return None, "timeout"
    except Exception:  # noqa: BLE001 - optional characteristic
        return None, "read_failed"
    if raw is None:
        return None, "missing"
    if len(raw) != 1 or not 0 <= raw[0] <= BATTERY_MAX_PERCENT:
        return None, "malformed"
    return raw[0], "ok"


async def _read_text(reader: OptionalReader | None, uuid: str, operation_timeout: float) -> tuple[str | None, str]:
    if reader is None:
        return None, "unsupported"
    value: str | None = None
    outcome = "read_failed"
    try:
        raw = await _bounded_optional(reader(uuid), operation_timeout)
    except _OptionalOperationTimeoutError:
        return None, "timeout"
    except Exception:  # noqa: BLE001 - optional characteristic
        return None, "read_failed"
    if raw is None:
        outcome = "missing"
    else:
        try:
            value = raw.decode("utf-8")
        except UnicodeDecodeError:
            outcome = "malformed"
        else:
            if not value:
                outcome = "malformed"
            else:
                value = value[:METADATA_VALUE_MAX_CHARS]
                outcome = "ok"
    return value, outcome


async def _read_time(
    reader: OptionalReader | None, host_time: Callable[[], float], operation_timeout: float
) -> _TimeSample:
    before = host_time()
    if reader is None:
        return _TimeSample(None, before, before, "unsupported")
    try:
        raw = await _bounded_optional(reader(TIME_READ_UUID), operation_timeout)
        after = host_time()
    except _OptionalOperationTimeoutError:
        after = host_time()
        return _TimeSample(None, before, after, "timeout")
    except Exception:  # noqa: BLE001 - optional characteristic
        after = host_time()
        return _TimeSample(None, before, after, "read_failed")
    value = _parse_u32(raw)
    return _TimeSample(value, before, after, "ok" if value is not None else "malformed")


def _parse_u32(raw: bytes | None) -> int | None:
    if raw is None or len(raw) != U32_BYTES:
        return None
    return int.from_bytes(raw, "little")


class _OptionalOperationTimeoutError(TimeoutError):
    """An optional GATT operation exceeded its short preflight bound."""


async def _bounded_optional[T](awaitable: Awaitable[T], timeout: float) -> T:
    try:
        return await asyncio.wait_for(awaitable, timeout)
    except TimeoutError as error:
        raise _OptionalOperationTimeoutError from error


def _complete_host_probe(result: asyncio.Future[bool], value: bool | None, error: BaseException | None) -> None:
    if result.done():
        return
    if error is not None:
        result.set_exception(error)
    else:
        result.set_result(bool(value))


def _schedule_host_probe_result(
    loop: asyncio.AbstractEventLoop,
    result: asyncio.Future[bool],
    value: bool | None = None,
    error: BaseException | None = None,
) -> None:
    try:
        loop.call_soon_threadsafe(_complete_host_probe, result, value, error)
    except RuntimeError:
        return


def _invoke_host_probe(
    probe: Callable[[], bool], loop: asyncio.AbstractEventLoop, result: asyncio.Future[bool]
) -> None:
    try:
        value = bool(probe())
    except BaseException as error:  # noqa: BLE001 - classify in the caller
        _schedule_host_probe_result(loop, result, error=error)
    else:
        _schedule_host_probe_result(loop, result, value)


async def _run_host_clock_probe(probe: Callable[[], bool], timeout: float) -> bool:
    """Run an injected synchronous trust probe without blocking the event loop.

    A daemon thread is intentional here.  ``asyncio.to_thread`` leaves its
    default executor worker alive until ``asyncio.run`` shuts the executor
    down, which would make a timed-out probe extend the overall preflight.
    The probe has no GATT ownership; its result is discarded when the bound
    expires, while the caller proceeds only after this awaitable is cancelled.
    """
    if timeout <= 0:
        raise _OptionalOperationTimeoutError
    loop = asyncio.get_running_loop()
    result: asyncio.Future[bool] = loop.create_future()

    Thread(
        target=_invoke_host_probe,
        args=(probe, loop, result),
        name="omi-host-clock-probe",
        daemon=True,
    ).start()
    try:
        return await asyncio.wait_for(asyncio.shield(result), timeout)
    except TimeoutError as error:
        result.cancel()
        raise _OptionalOperationTimeoutError from error
    except asyncio.CancelledError:
        result.cancel()
        raise


async def _emit(emit: OperationalEmitter, event: dict[str, object]) -> None:
    result = emit(event)
    if isawaitable(result):
        await result
