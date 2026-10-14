"""Privacy-safe operational observations for one pendant presence session."""

# Optional adapter protocols are intentionally runtime-narrowed at their
# durability boundary.
# pyright: reportAny=false
# pyright: reportArgumentType=false

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG
from ..domain.clock_state import ClockIntent, ClockRange
from ..domain.ring_protocol import RingInfo, RingStatus
from .owned_work import invoke_owned, joined_to_thread
from .ports import ClockStatePort, StorageLeaseFactory
from .stop_intent import StopIntent
from .storage_fault import FatalInternalAudioStorageError

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
type InfoReader = Callable[[], Awaitable[RingInfo]]
type StatusReader = Callable[[], Awaitable[RingStatus | None]]
type ClockMutationLease = StorageLeaseFactory


def system_host_boot_id() -> str:
    """Read the kernel boot identity without relying on journal text."""
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return "unknown"
    return value or "unknown"


@dataclass(frozen=True, slots=True)
class TelemetryClock:
    """Injectable wall-clock and trust source used by drift correction."""

    now: Callable[[], float] = time.time
    synchronized: Callable[[float], bool] | None = None
    operation_timeout: float = OPTIONAL_OPERATION_TIMEOUT_SECONDS
    host_clock_probe_timeout: float = HOST_CLOCK_PROBE_TIMEOUT_SECONDS
    info_reader: InfoReader | None = None
    status_reader: StatusReader | None = None
    state_store: ClockStatePort | None = None
    monotonic: Callable[[], float] = time.monotonic
    host_boot_id: str = field(default_factory=system_host_boot_id)
    session_id: str = "native"
    mutation_lease: ClockMutationLease | None = None
    stop_intent: StopIntent | None = None


@dataclass(frozen=True, slots=True)
class _TimeSample:
    epoch: int | None
    before: float
    after: float
    monotonic_before: float
    monotonic_after: float
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
    except subprocess.TimeoutExpired as error:
        raise _OptionalOperationTimeoutError from error
    except (OSError, subprocess.SubprocessError):  # fmt: skip
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
    if emit is None and (clock is None or clock.state_store is None):
        return
    telemetry_clock = clock or TelemetryClock()
    _validate_telemetry_clock(telemetry_clock)
    if emit is None:
        emit = _discard_event
    synchronized = telemetry_clock.synchronized or system_host_clock_synchronized
    reader, writer = _optional_accessors(session)
    # Clock evidence is its own bounded stage.  It must not depend on status,
    # metadata, or a potentially slow operational emitter.
    sample = await _read_time(
        reader,
        telemetry_clock.now,
        telemetry_clock.monotonic,
        telemetry_clock.operation_timeout,
        telemetry_clock.stop_intent,
    )
    clock_events: list[dict[str, object]] = []
    sync = _ClockSync(
        reader,
        writer,
        sample,
        synchronized,
        clock_events.append,
        telemetry_clock.operation_timeout,
        telemetry_clock.host_clock_probe_timeout,
        info,
        telemetry_clock.info_reader,
        telemetry_clock.state_store,
        telemetry_clock.now,
        telemetry_clock.host_boot_id,
        telemetry_clock.session_id,
        telemetry_clock.monotonic,
        telemetry_clock.stop_intent,
    )
    await _sync_clock_with_mutation_lease(sync, telemetry_clock.mutation_lease)
    for event in clock_events:
        await _emit_safe(emit, event)
    if status is None and telemetry_clock.status_reader is not None:
        try:
            if telemetry_clock.stop_intent is not None:
                telemetry_clock.stop_intent.check()
            status = await _bounded_optional(telemetry_clock.status_reader(), telemetry_clock.operation_timeout)
        except asyncio.CancelledError:
            raise
        except FatalInternalAudioStorageError:
            raise
        except Exception:  # noqa: BLE001 - optional status is advisory
            status = None
    observation, sample = await _build_observation(
        reader,
        status,
        info,
        telemetry_clock.now,
        telemetry_clock.monotonic,
        telemetry_clock.operation_timeout,
        sample=sample,
        stop_intent=telemetry_clock.stop_intent,
    )
    # The journal is a projection, not the clock-recovery authority.  Keep a
    # slow or failed emitter from suppressing the durable clock decision.
    await _emit_safe(emit, observation)


def _validate_telemetry_clock(clock: TelemetryClock) -> None:
    if clock.stop_intent is not None:
        clock.stop_intent.check()
    if clock.operation_timeout <= 0:
        raise ValueError("optional operation timeout must be positive")
    if clock.host_clock_probe_timeout <= 0:
        raise ValueError("host clock probe timeout must be positive")


async def collect_battery_observation(
    session: object,
    info: RingInfo,
    emit: OperationalEmitter,
    *,
    operation_timeout: float,
    stop_intent: StopIntent | None = None,
) -> None:
    """Emit one bounded battery refresh without repeating connection telemetry."""
    if operation_timeout <= 0:
        raise ValueError("optional operation timeout must be positive")
    reader, _ = _optional_accessors(session)
    battery, outcome = await _read_battery(reader, operation_timeout, stop_intent)
    observation: dict[str, object] = {
        "event": "pendant_observation",
        "read_sequence": info.read_sequence,
        "write_sequence": info.write_sequence,
        "capacity_packets": info.capacity_packets,
        "dropped_packets": info.dropped_packets,
        "packet_size": info.packet_size,
        "optional_outcomes": {"battery": outcome},
    }
    if battery is not None:
        observation["battery_percent"] = battery
    await _emit_safe(emit, observation)


async def _build_observation(  # noqa: PLR0913, PLR0917 - operation inputs mirror the typed clock seam
    reader: OptionalReader | None,
    status: RingStatus | None,
    info: RingInfo,
    host_time: Callable[[], float],
    host_monotonic: Callable[[], float],
    operation_timeout: float,
    *,
    sample: _TimeSample | None = None,
    stop_intent: StopIntent | None = None,
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
    battery, outcomes["battery"] = await _read_battery(reader, operation_timeout, stop_intent)
    if battery is not None:
        observation["battery_percent"] = battery
    for name, uuid in (
        ("model", MODEL_UUID),
        ("firmware", FIRMWARE_UUID),
        ("hardware", HARDWARE_UUID),
        ("manufacturer", MANUFACTURER_UUID),
    ):
        value, outcomes[name] = await _read_text(reader, uuid, operation_timeout, stop_intent)
        if value is not None:
            observation[name] = value
    if sample is None:
        sample = await _read_time(reader, host_time, host_monotonic, operation_timeout, stop_intent)
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
    host_clock_synchronized: Callable[[float], bool]
    emit: OperationalEmitter
    operation_timeout: float
    host_clock_probe_timeout: float
    info_before: RingInfo
    info_reader: InfoReader | None
    state_store: ClockStatePort | None
    host_time: Callable[[], float]
    host_boot_id: str
    session_id: str
    host_monotonic: Callable[[], float]
    stop_intent: StopIntent | None


def _check_stop_intent(sync: _ClockSync) -> None:
    if sync.stop_intent is not None:
        sync.stop_intent.check()


async def _sync_clock_with_mutation_lease(sync: _ClockSync, mutation_lease: ClockMutationLease | None) -> None:
    """Keep clock evidence under the staging lease without interrupting a resumed writer."""
    if mutation_lease is None:
        await _sync_clock(sync)
        return
    _check_stop_intent(sync)
    try:
        lease = mutation_lease()
        entered = False
        primary: BaseException | None = None
        try:

            def enter() -> object:
                nonlocal entered
                result = lease.__enter__()
                entered = True
                return result

            await joined_to_thread(enter)
            await _sync_clock(sync)
        except BaseException as error:
            primary = error
            raise
        finally:
            if entered:
                await joined_to_thread(
                    lease.__exit__,
                    type(primary) if primary is not None else None,
                    primary,
                    primary.__traceback__ if primary is not None else None,
                )
    except asyncio.CancelledError:
        raise
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - a held writer lease defers optional clock mutation
        sync.emit(
            {
                "event": "pendant_clock_sync",
                "action": "none",
                "outcome": "storage_lease_unavailable",
                "boundary_sequence_min": sync.info_before.write_sequence,
            }
        )


async def _sync_clock(sync: _ClockSync) -> None:
    sample = sync.sample
    host_clock_synchronized = sync.host_clock_synchronized
    emit = sync.emit
    host_clock_probe_timeout = sync.host_clock_probe_timeout
    _check_stop_intent(sync)
    if sample.epoch is None:
        emit(
            {
                "event": "pendant_clock_sync",
                "action": "skipped",
                "outcome": "device_time_" + sample.outcome,
                "threshold_seconds": CLOCK_DRIFT_THRESHOLD_SECONDS,
            }
        )
        return
    midpoint = (sample.before + sample.after) / 2.0
    drift = float(sample.epoch) - midpoint
    event: dict[str, object] = {
        "event": "pendant_clock_sync",
        "drift_seconds": round(drift, 3),
        "threshold_seconds": CLOCK_DRIFT_THRESHOLD_SECONDS,
        "boundary_sequence_min": sync.info_before.write_sequence,
    }
    try:
        trusted = await _run_host_clock_probe(host_clock_synchronized, host_clock_probe_timeout)
    except _OptionalOperationTimeoutError:
        event.update(action="none", outcome="host_probe_timeout")
        sync.emit(event)
        return
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - trust is best effort
        trusted = False
    event["host_ntp_synchronized"] = trusted
    if not trusted:
        event.update(action="none", outcome="host_unsynchronized")
        sync.emit(event)
        return
    await _sync_trusted_state_clock(sync, event, drift)


async def _sync_trusted_state_clock(sync: _ClockSync, event: dict[str, object], drift: float) -> None:
    store = sync.state_store
    if store is None:
        event.update(action="none", outcome="evidence_persist_failed")
        sync.emit(event)
        return
    sample = sync.sample
    assert sample.epoch is not None
    midpoint = (sample.before + sample.after) / 2
    uncertainty = max(1.0, (sample.after - sample.before) / 2 + 1)
    try:
        previous = await joined_to_thread(store.state)
        if previous.intent is not None:
            await _reconcile_pending_state(store, previous.intent, sync.info_before, event)
        _check_stop_intent(sync)
        await joined_to_thread(
            store.observe,
            sync.info_before,
            offset_seconds=midpoint - sample.epoch,
            uncertainty_seconds=uncertainty,
            source_id=sync.session_id,
            tolerance_seconds=CLOCK_DRIFT_THRESHOLD_SECONDS,
        )
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - a missing durable range must block time writes
        event.update(action="none", outcome="evidence_persist_failed")
        sync.emit(event)
        return
    if abs(drift) <= CLOCK_DRIFT_THRESHOLD_SECONDS:
        event.update(action="none", outcome="within_threshold")
    else:
        await _correct_drift(sync, store, event, midpoint)
    sync.emit(event)


async def _correct_drift(sync: _ClockSync, store: ClockStatePort, event: dict[str, object], midpoint: float) -> None:
    sample = sync.sample
    assert sample.epoch is not None
    target = int(await joined_to_thread(sync.host_time))
    if not 0 <= target <= MAX_U32:
        event.update(action="none", outcome="target_unrepresentable")
        return
    if sync.writer is None:
        event.update(action="none", outcome="time_write_missing")
        return
    intent = ClockIntent(uuid4().hex, "prepared", sample.epoch, target, midpoint, sync.info_before.write_sequence)
    try:
        _check_stop_intent(sync)
        await joined_to_thread(store.prepare, intent)
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - no BLE write without a committed intent
        event.update(action="none", outcome="intent_persist_failed")
        return
    _check_stop_intent(sync)
    readback = await _write_and_verify(
        _ClockWrite(
            sync.reader,
            sync.writer,
            target,
            event,
            sync.operation_timeout,
            sync.host_time,
            sync.host_monotonic,
            sync.stop_intent,
        )
    )
    if event.get("outcome") in {"target_stale", "time_write_missing"}:
        try:
            await joined_to_thread(store.cancel_prepared, intent.operation_id)
        except FatalInternalAudioStorageError:
            raise
        except Exception:  # noqa: BLE001 - retained prepared intent is conservative
            event["outcome"] = "result_persist_failed"
        return
    info_after = await _read_boundary_after(sync, event)
    try:
        await _persist_clock_result(sync, intent, readback, info_after, event)
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - durable intent remains the recovery authority
        event["outcome"] = "result_persist_failed"


async def _persist_clock_result(
    sync: _ClockSync,
    intent: ClockIntent,
    readback: _ClockReadback,
    info_after: RingInfo | None,
    event: dict[str, object],
) -> None:
    store = sync.state_store
    assert store is not None
    await joined_to_thread(
        store.mark_uncertain,
        intent.operation_id,
        write_sequence_after=info_after.write_sequence if info_after is not None else None,
    )
    if event.get("outcome") != "verified" or info_after is None or readback.epoch is None:
        return
    boundary = sync.info_before.write_sequence
    ranges = (
        (ClockRange(boundary, info_after.write_sequence, None, None, "unknown", intent.operation_id),)
        if info_after.write_sequence > boundary
        else ()
    )
    await joined_to_thread(store.finish, intent.operation_id, ranges)
    await joined_to_thread(
        store.observe,
        info_after,
        offset_seconds=(readback.host_realtime_start + readback.host_realtime_end) / 2 - readback.epoch,
        uncertainty_seconds=max(1.0, (readback.host_realtime_end - readback.host_realtime_start) / 2 + 1),
        source_id=sync.session_id,
        tolerance_seconds=CLOCK_DRIFT_THRESHOLD_SECONDS,
    )
    if ranges:
        event["boundary_ambiguous"] = True


async def _read_boundary_after(sync: _ClockSync, event: dict[str, object]) -> RingInfo | None:
    if event.get("action") != "written" or sync.info_reader is None:
        return None
    try:
        info = await _bounded_optional(sync.info_reader(), sync.operation_timeout)
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - an unavailable boundary remains uncertain
        return None
    if isinstance(info, RingInfo):
        event["boundary_sequence_max"] = info.write_sequence
        return info
    return None


async def _reconcile_pending_state(
    store: ClockStatePort, intent: ClockIntent, info: RingInfo, event: dict[str, object]
) -> None:
    if info.write_sequence < intent.write_sequence_before:
        raise ValueError("device sequence regressed across a pending clock write")
    if intent.phase == "prepared" or intent.write_sequence_after is None:
        await joined_to_thread(store.mark_uncertain, intent.operation_id, write_sequence_after=info.write_sequence)
    unknown = (
        (
            ClockRange(
                intent.write_sequence_before,
                info.write_sequence,
                None,
                None,
                "unknown",
                intent.operation_id,
            ),
        )
        if info.write_sequence > intent.write_sequence_before
        else ()
    )
    await joined_to_thread(store.finish, intent.operation_id, unknown)
    event["reconciled_operations"] = 1
    if unknown:
        event["boundary_ambiguous"] = True


@dataclass(frozen=True, slots=True)
class _ClockWrite:
    reader: OptionalReader | None
    writer: OptionalWriter
    target: int
    event: dict[str, object]
    operation_timeout: float
    host_time: Callable[[], float]
    host_monotonic: Callable[[], float]
    stop_intent: StopIntent | None


@dataclass(frozen=True, slots=True)
class _ClockReadback:
    epoch: int | None
    host_realtime_start: float
    host_realtime_end: float
    host_monotonic_start: float
    host_monotonic_end: float


async def _write_and_verify(write: _ClockWrite) -> _ClockReadback:
    reader, writer, target, event = write.reader, write.writer, write.target, write.event
    write_started = await joined_to_thread(write.host_time)
    monotonic_started = await joined_to_thread(write.host_monotonic)
    if not target <= write_started <= target + 2:
        event.update(action="none", outcome="target_stale")
        return _ClockReadback(None, write_started, write_started, monotonic_started, monotonic_started)
    if write.stop_intent is not None:
        write.stop_intent.check()
    try:
        performed = await _bounded_optional(
            writer(TIME_WRITE_UUID, target.to_bytes(U32_BYTES, "little")), write.operation_timeout
        )
    except _OptionalOperationTimeoutError:
        event.update(action="none", outcome="time_write_timeout")
        finished = await joined_to_thread(write.host_time)
        return _ClockReadback(
            None, write_started, finished, monotonic_started, await joined_to_thread(write.host_monotonic)
        )
    except Exception:  # noqa: BLE001 - classify optional backend failures
        event.update(action="none", outcome="time_write_failed")
        finished = await joined_to_thread(write.host_time)
        return _ClockReadback(
            None, write_started, finished, monotonic_started, await joined_to_thread(write.host_monotonic)
        )
    if performed is False:
        event.update(action="none", outcome="time_write_missing")
        finished = await joined_to_thread(write.host_time)
        return _ClockReadback(
            None, write_started, finished, monotonic_started, await joined_to_thread(write.host_monotonic)
        )
    event["target_epoch"] = target
    try:
        verified = (
            _parse_u32(await _bounded_optional(reader(TIME_READ_UUID), write.operation_timeout))
            if reader is not None
            else None
        )
    except _OptionalOperationTimeoutError:
        verified = None
        event.update(action="written", outcome="verification_timeout")
        finished = await joined_to_thread(write.host_time)
        return _ClockReadback(
            None, write_started, finished, monotonic_started, await joined_to_thread(write.host_monotonic)
        )
    except Exception:  # noqa: BLE001 - classify optional backend failures
        verified = None
    read_finished = await joined_to_thread(write.host_time)
    allowance = max(1, int(read_finished - write_started) + 1)
    valid = verified is not None and target <= verified <= target + allowance
    event.update(action="written", outcome="verified" if valid else "verification_failed")
    return _ClockReadback(
        verified, write_started, read_finished, monotonic_started, await joined_to_thread(write.host_monotonic)
    )


def _optional_accessors(session: object) -> tuple[OptionalReader | None, OptionalWriter | None]:
    reader = getattr(session, "read_optional_characteristic", None)
    writer = getattr(session, "write_optional_characteristic", None)
    if not callable(reader):
        reader = getattr(session, "read_characteristic", None)
    if not callable(writer):
        writer = getattr(session, "write_characteristic", None)
    return cast(OptionalReader | None, reader), cast(OptionalWriter | None, writer)


async def _read_battery(
    reader: OptionalReader | None, operation_timeout: float, stop_intent: StopIntent | None = None
) -> tuple[int | None, str]:
    if reader is None:
        return None, "unsupported"
    try:
        if stop_intent is not None:
            stop_intent.check()
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


async def _read_text(
    reader: OptionalReader | None,
    uuid: str,
    operation_timeout: float,
    stop_intent: StopIntent | None = None,
) -> tuple[str | None, str]:
    if reader is None:
        return None, "unsupported"
    value: str | None = None
    outcome = "read_failed"
    try:
        if stop_intent is not None:
            stop_intent.check()
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
    reader: OptionalReader | None,
    host_time: Callable[[], float],
    host_monotonic: Callable[[], float],
    operation_timeout: float,
    stop_intent: StopIntent | None = None,
) -> _TimeSample:
    before = await joined_to_thread(host_time)
    monotonic_before = await joined_to_thread(host_monotonic)
    if reader is None:
        return _TimeSample(None, before, before, monotonic_before, monotonic_before, "unsupported")
    try:
        if stop_intent is not None:
            stop_intent.check()
        raw = await _bounded_optional(reader(TIME_READ_UUID), operation_timeout)
        after = await joined_to_thread(host_time)
        monotonic_after = await joined_to_thread(host_monotonic)
    except _OptionalOperationTimeoutError:
        after = await joined_to_thread(host_time)
        monotonic_after = await joined_to_thread(host_monotonic)
        return _TimeSample(None, before, after, monotonic_before, monotonic_after, "timeout")
    except Exception:  # noqa: BLE001 - optional characteristic
        after = await joined_to_thread(host_time)
        monotonic_after = await joined_to_thread(host_monotonic)
        return _TimeSample(None, before, after, monotonic_before, monotonic_after, "read_failed")
    value = _parse_u32(raw)
    return _TimeSample(
        value, before, after, monotonic_before, monotonic_after, "ok" if value is not None else "malformed"
    )


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


async def _run_host_clock_probe(probe: Callable[[float], bool], timeout: float) -> bool:
    """Run a timeout-aware trust probe and join its worker before returning."""
    if timeout <= 0:
        raise _OptionalOperationTimeoutError
    try:
        return bool(await joined_to_thread(probe, timeout))
    except _OptionalOperationTimeoutError:
        raise
    except TimeoutError as error:
        raise _OptionalOperationTimeoutError from error


async def _emit(emit: OperationalEmitter, event: dict[str, object]) -> None:
    await invoke_owned(emit, event)


async def _emit_safe(emit: OperationalEmitter, event: dict[str, object]) -> None:
    """Deliver auxiliary events without blocking or abandoning the event loop."""
    try:
        await _emit(emit, event)
    except asyncio.CancelledError:
        raise
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - telemetry is auxiliary
        return


def _discard_event(event: OperationalEvent) -> None:
    del event
