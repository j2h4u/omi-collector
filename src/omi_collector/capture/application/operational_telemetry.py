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
from inspect import isawaitable
from pathlib import Path
from threading import Thread
from typing import cast

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RingInfo, RingStatus
from .ports import (
    ClockCorrectionShape,
    ClockMembershipPort,
    ClockObservationShape,
    PublicationAuthorityPort,
    StorageLeaseFactory,
)
from .ports import (
    ClockDurabilityPort as ClockCorrectionSink,
)
from .ports import (
    ClockObservationPort as ClockObservationSink,
)

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
    synchronized: Callable[[], bool] | None = None
    operation_timeout: float = OPTIONAL_OPERATION_TIMEOUT_SECONDS
    host_clock_probe_timeout: float = HOST_CLOCK_PROBE_TIMEOUT_SECONDS
    info_reader: InfoReader | None = None
    status_reader: StatusReader | None = None
    correction_sink: ClockCorrectionSink | None = None
    observation_sink: ClockObservationSink | None = None
    membership_store: ClockMembershipPort | None = None
    monotonic: Callable[[], float] = time.monotonic
    host_boot_id: str = field(default_factory=system_host_boot_id)
    session_id: str = "native"
    publisher: PublicationAuthorityPort | None = None
    mutation_lease: ClockMutationLease | None = None


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
    if emit is None and (clock is None or (clock.observation_sink is None and clock.correction_sink is None)):
        return
    telemetry_clock = clock or TelemetryClock()
    if emit is None:
        emit = _discard_event
    if telemetry_clock.operation_timeout <= 0:
        raise ValueError("optional operation timeout must be positive")
    if telemetry_clock.host_clock_probe_timeout <= 0:
        raise ValueError("host clock probe timeout must be positive")
    synchronized = telemetry_clock.synchronized or (
        lambda: system_host_clock_synchronized(telemetry_clock.host_clock_probe_timeout)
    )
    observation_sink = (
        telemetry_clock.observation_sink
        if telemetry_clock.observation_sink is not None
        else telemetry_clock.correction_sink.observation_store
        if telemetry_clock.correction_sink is not None
        else None
    )
    reader, writer = _optional_accessors(session)
    # Clock evidence is its own bounded stage.  It must not depend on status,
    # metadata, or a potentially slow operational emitter.
    sample = await _read_time(
        reader,
        telemetry_clock.now,
        telemetry_clock.monotonic,
        telemetry_clock.operation_timeout,
    )
    clock_events: list[dict[str, object]] = []
    sync = _ClockSync(
        reader,
        writer,
        sample,
        synchronized,
        clock_events.append,
        telemetry_clock.operation_timeout,
        info,
        telemetry_clock.info_reader,
        telemetry_clock.correction_sink,
        telemetry_clock.now,
        observation_sink,
        telemetry_clock.host_boot_id,
        telemetry_clock.session_id,
        telemetry_clock.monotonic,
        telemetry_clock.membership_store,
        telemetry_clock.publisher,
    )
    await _sync_clock_with_mutation_lease(sync, telemetry_clock.mutation_lease)
    if status is None and telemetry_clock.status_reader is not None:
        try:
            status = await _bounded_optional(telemetry_clock.status_reader(), telemetry_clock.operation_timeout)
        except asyncio.CancelledError:
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
    )
    # The journal is a projection, not the clock-recovery authority.  Keep a
    # slow or failed emitter from suppressing the durable clock decision.
    await _emit_safe(emit, observation, telemetry_clock.operation_timeout)
    for event in clock_events:
        await _emit_safe(emit, event, telemetry_clock.operation_timeout)


async def _build_observation(  # noqa: PLR0913, PLR0917 - operation inputs mirror the typed clock seam
    reader: OptionalReader | None,
    status: RingStatus | None,
    info: RingInfo,
    host_time: Callable[[], float],
    host_monotonic: Callable[[], float],
    operation_timeout: float,
    *,
    sample: _TimeSample | None = None,
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
    if sample is None:
        sample = await _read_time(reader, host_time, host_monotonic, operation_timeout)
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
    info_before: RingInfo
    info_reader: InfoReader | None
    correction_sink: ClockCorrectionSink | None
    host_time: Callable[[], float]
    observation_sink: ClockObservationSink | None
    host_boot_id: str
    session_id: str
    host_monotonic: Callable[[], float]
    membership_store: ClockMembershipPort | None
    publisher: PublicationAuthorityPort | None


async def _sync_clock_with_mutation_lease(sync: _ClockSync, mutation_lease: ClockMutationLease | None) -> None:
    """Keep clock evidence under the staging lease without interrupting a resumed writer."""
    if mutation_lease is None:
        await _sync_clock(sync)
        return
    try:
        with mutation_lease():
            await _sync_clock(sync)
    except asyncio.CancelledError:
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
    operation_timeout = sync.operation_timeout
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
        trusted = await _run_host_clock_probe(host_clock_synchronized, operation_timeout)
    except Exception:  # noqa: BLE001 - trust is best effort
        trusted = False
    event["host_ntp_synchronized"] = trusted
    if not trusted:
        event.update(action="none", outcome="host_unsynchronized")
        sync.emit(event)
        return
    await _sync_trusted_clock(sync, event, drift)


async def _sync_trusted_clock(sync: _ClockSync, event: dict[str, object], drift: float) -> None:
    """Persist only clock evidence needed for a correction or reconciliation."""
    pending_operation = _pending_observation_operation(sync)
    observation_evidence = _persist_clock_observation(sync, pending_operation)
    if observation_evidence is None:
        event.update(action="none", outcome="evidence_persist_failed")
        sync.emit(event)
        return
    if pending_operation is not None:
        _reconcile_observation(sync, event, observation_evidence)
    if abs(drift) <= CLOCK_DRIFT_THRESHOLD_SECONDS:
        event.update(action="none", outcome="within_threshold")
        await _record_same_session_membership(sync, observation_evidence, sync.info_before.write_sequence)
        await _publish_ready(sync, event)
        sync.emit(event)
        return
    target = int(sync.host_time())
    if not 0 <= target <= MAX_U32:
        event.update(action="none", outcome="target_unrepresentable")
        sync.emit(event)
        return
    if sync.writer is None:
        event.update(action="none", outcome="time_write_missing")
        sync.emit(event)
        return
    correction = _prepare_clock_intent(sync, event, target, drift, observation_evidence)
    if correction is None:
        sync.emit(event)
        return
    readback = await _write_and_verify(
        _ClockWrite(
            sync.reader,
            sync.writer,
            target,
            event,
            sync.operation_timeout,
            sync.host_time,
            sync.host_monotonic,
        )
    )
    info_after = await _read_boundary_after(sync, event)
    post_observation = _persist_post_clock_observation(
        sync,
        correction,
        info_after,
        readback,
    )
    if (
        sync.observation_sink is not None
        and readback is not None
        and readback.epoch is not None
        and post_observation is None
    ):
        event["outcome"] = "result_persist_failed"
    elif info_after is not None and post_observation is not None:
        await _record_same_session_membership(sync, post_observation, info_after.write_sequence)
    _finish_clock_intent(sync, correction, event, info_after, readback.epoch if readback else None)
    await _publish_ready(sync, event)
    sync.emit(event)


async def _publish_ready(sync: _ClockSync, event: dict[str, object]) -> None:
    if sync.publisher is None:
        return
    try:
        result = sync.publisher.publish()
        if isawaitable(result):
            await _bounded_optional(cast(Awaitable[object], result), sync.operation_timeout)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - publication is a retryable projection
        event["publication"] = "failed"


def _persist_clock_observation(sync: _ClockSync, pending_operation: str | None) -> ClockObservationShape | None:
    sink = sync.observation_sink
    if sink is None:
        return None
    sample = sync.sample
    if sample.epoch is None:
        return None
    values: dict[str, object] = {
        "evidence_kind": "native_trusted",
        "session_id": sync.session_id,
        "host_boot_id": sync.host_boot_id,
        "host_realtime_start": sample.before,
        "host_realtime_end": sample.after,
        "host_monotonic_start": sample.monotonic_before,
        "host_monotonic_end": sample.monotonic_after,
        "device_epoch": sample.epoch,
        "info_sequence_min": sync.info_before.read_sequence,
        "info_sequence_max": sync.info_before.write_sequence,
    }
    initial = _initial_observation(sync, pending_operation)
    if pending_operation is not None and initial is not None:
        values.update(
            operation_id=pending_operation,
            observation_role="later",
            parent_observation_id=initial.observation_id,
        )
    try:
        return sink.append(**values)
    except Exception:  # noqa: BLE001 - no clock mutation without durable evidence
        return None


def _pending_observation_operation(sync: _ClockSync) -> str | None:
    sink = sync.correction_sink
    if sink is None:
        return None
    try:
        pending = sink.records()
    except Exception:  # noqa: BLE001 - operation binding is best effort
        return None
    candidates = tuple(
        item
        for item in pending
        if item.state == "unresolved" and item.boundary_sequence_min <= sync.info_before.write_sequence
    )
    operation_id = candidates[-1].operation_id if candidates else None
    return operation_id if isinstance(operation_id, str) and operation_id else None


def _initial_observation(sync: _ClockSync, operation_id: str | None) -> ClockObservationShape | None:
    if operation_id is None:
        return None
    sink = sync.observation_sink
    if sink is None:
        return None
    try:
        values = sink.records()
    except Exception:  # noqa: BLE001 - missing durable linkage leaves evidence unresolved
        return None
    return next(
        (item for item in reversed(values) if item.operation_id == operation_id and item.observation_role == "initial"),
        None,
    )


def _persist_post_clock_observation(
    sync: _ClockSync, correction: ClockCorrectionShape, info_after: RingInfo | None, readback: _ClockReadback | None
) -> ClockObservationShape | None:
    sink = sync.observation_sink
    if sink is None or readback is None or readback.epoch is None:
        return None
    boundary = info_after.write_sequence if info_after is not None else sync.info_before.write_sequence
    effective_boundary = (
        info_after.write_sequence
        if info_after is not None and info_after.write_sequence == sync.info_before.write_sequence
        else None
    )
    values: dict[str, object] = {
        "evidence_kind": "native_trusted",
        "session_id": sync.session_id,
        "host_boot_id": sync.host_boot_id,
        "host_realtime_start": readback.host_realtime_start,
        "host_realtime_end": readback.host_realtime_end,
        "host_monotonic_start": readback.host_monotonic_start,
        "host_monotonic_end": readback.host_monotonic_end,
        "device_epoch": readback.epoch,
        "info_sequence_min": sync.info_before.write_sequence,
        "info_sequence_max": boundary,
        "operation_id": correction.operation_id,
        "effective_boundary_sequence": effective_boundary,
    }
    operation_id = values["operation_id"]
    initial = _initial_observation(sync, operation_id if isinstance(operation_id, str) else None)
    if initial is not None:
        values.update(
            observation_role="later",
            parent_observation_id=initial.observation_id,
        )
    try:
        return sink.append(**values)
    except Exception:  # noqa: BLE001 - uncertain result remains unresolved
        return None


async def _record_same_session_membership(
    sync: _ClockSync, observation: ClockObservationShape, start_sequence: int
) -> None:
    store = sync.membership_store
    if store is None or sync.info_reader is None:
        return
    try:
        later = await _bounded_optional(sync.info_reader(), sync.operation_timeout)
    except Exception:  # noqa: BLE001 - disconnected sessions never extend membership
        return
    if isinstance(later, RingInfo) and later.write_sequence > start_sequence:
        store.record_membership(observation.observation_id, sync.session_id, start_sequence, later.write_sequence)


async def _read_boundary_after(sync: _ClockSync, event: dict[str, object]) -> RingInfo | None:
    if event.get("action") != "written" or sync.info_reader is None:
        return None
    try:
        info = await _bounded_optional(sync.info_reader(), sync.operation_timeout)
    except Exception:  # noqa: BLE001 - sequence evidence is best effort
        return None
    if isinstance(info, RingInfo):
        event["boundary_sequence_max"] = info.write_sequence
        return info
    return None


def _reconcile_observation(
    sync: _ClockSync, event: dict[str, object], observation: ClockObservationShape | None = None
) -> None:
    sink = sync.correction_sink
    if sink is None or observation is None:
        return
    if observation.operation_id is None or sync.observation_sink is None:
        event["reconciliation"] = "no_causal_operation"
        return
    try:
        reconciled = sink.reconcile_causal_observation(observation, near_zero_threshold=CLOCK_DRIFT_THRESHOLD_SECONDS)
    except Exception:  # noqa: BLE001 - unresolved evidence must remain durable
        event["reconciliation"] = "failed"
        return
    if isinstance(reconciled, tuple):
        event["reconciled_operations"] = len(reconciled)


def _prepare_clock_intent(
    sync: _ClockSync,
    event: dict[str, object],
    target: int,
    drift: float,
    observation: ClockObservationShape,
) -> ClockCorrectionShape | None:
    if sync.correction_sink is None:
        event.update(action="none", outcome="intent_unavailable")
        return None
    assert sync.sample.epoch is not None
    assert sync.correction_sink is not None
    try:
        correction = sync.correction_sink.prepare(sync.sample.epoch, target, drift, sync.info_before.write_sequence)
        _bind_observation_operation(sync, correction, observation)
        return sync.correction_sink.mark_unresolved(correction)
    except Exception:  # noqa: BLE001 - writing without durable intent is unsafe
        event.update(action="none", outcome="intent_persist_failed")
        return None


def _bind_observation_operation(
    sync: _ClockSync, correction: ClockCorrectionShape, observation: ClockObservationShape
) -> None:
    sink = sync.observation_sink
    if sink is None:
        return
    source = next((item for item in sink.records() if item.observation_id == observation.observation_id), None)
    if source is None or source.operation_id is not None:
        return
    sink.append(
        evidence_kind=source.evidence_kind,
        session_id=source.session_id,
        host_boot_id=source.host_boot_id,
        host_realtime_start=source.host_realtime_start,
        host_realtime_end=source.host_realtime_end,
        host_monotonic_start=source.host_monotonic_start,
        host_monotonic_end=source.host_monotonic_end,
        device_epoch=source.device_epoch,
        info_sequence_min=source.info_sequence_min,
        info_sequence_max=source.info_sequence_max,
        operation_id=correction.operation_id,
        observation_role="initial",
    )


def _finish_clock_intent(
    sync: _ClockSync,
    correction: ClockCorrectionShape,
    event: dict[str, object],
    info_after: RingInfo | None,
    verified_epoch: int | None,
) -> None:
    assert sync.correction_sink is not None
    outcome = event.get("outcome")
    state = (
        "resolved"
        if outcome == "verified"
        and info_after is not None
        and info_after.write_sequence == sync.info_before.write_sequence
        else "applied"
        if outcome == "verified" and info_after is not None
        else "not_applied"
        if outcome in {"time_write_missing", "target_stale"}
        else "unresolved"
    )
    try:
        boundary_max = (
            info_after.write_sequence
            if info_after is not None and state in {"applied", "resolved", "not_applied"}
            else None
        )
        durable_verified_epoch = verified_epoch if state in {"applied", "resolved"} else None
        sync.correction_sink.finish(
            correction,
            state=state,
            boundary_sequence_max=boundary_max,
            verified_epoch=durable_verified_epoch,
        )
    except Exception:  # noqa: BLE001 - prepared state truthfully records uncertainty
        event["outcome"] = "result_persist_failed"


@dataclass(frozen=True, slots=True)
class _ClockWrite:
    reader: OptionalReader | None
    writer: OptionalWriter
    target: int
    event: dict[str, object]
    operation_timeout: float
    host_time: Callable[[], float]
    host_monotonic: Callable[[], float]


@dataclass(frozen=True, slots=True)
class _ClockReadback:
    epoch: int | None
    host_realtime_start: float
    host_realtime_end: float
    host_monotonic_start: float
    host_monotonic_end: float


async def _write_and_verify(write: _ClockWrite) -> _ClockReadback:
    reader, writer, target, event = write.reader, write.writer, write.target, write.event
    write_started = write.host_time()
    monotonic_started = write.host_monotonic()
    if not target <= write_started <= target + 2:
        event.update(action="none", outcome="target_stale")
        return _ClockReadback(None, write_started, write_started, monotonic_started, monotonic_started)
    try:
        performed = await _bounded_optional(
            writer(TIME_WRITE_UUID, target.to_bytes(U32_BYTES, "little")), write.operation_timeout
        )
    except _OptionalOperationTimeoutError:
        event.update(action="none", outcome="time_write_timeout")
        finished = write.host_time()
        return _ClockReadback(None, write_started, finished, monotonic_started, write.host_monotonic())
    except Exception:  # noqa: BLE001 - classify optional backend failures
        event.update(action="none", outcome="time_write_failed")
        finished = write.host_time()
        return _ClockReadback(None, write_started, finished, monotonic_started, write.host_monotonic())
    if performed is False:
        event.update(action="none", outcome="time_write_missing")
        finished = write.host_time()
        return _ClockReadback(None, write_started, finished, monotonic_started, write.host_monotonic())
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
        finished = write.host_time()
        return _ClockReadback(None, write_started, finished, monotonic_started, write.host_monotonic())
    except Exception:  # noqa: BLE001 - classify optional backend failures
        verified = None
    read_finished = write.host_time()
    allowance = max(1, int(read_finished - write_started) + 1)
    valid = verified is not None and target <= verified <= target + allowance
    event.update(action="written", outcome="verified" if valid else "verification_failed")
    return _ClockReadback(verified, write_started, read_finished, monotonic_started, write.host_monotonic())


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
    reader: OptionalReader | None,
    host_time: Callable[[], float],
    host_monotonic: Callable[[], float],
    operation_timeout: float,
) -> _TimeSample:
    before = host_time()
    monotonic_before = host_monotonic()
    if reader is None:
        return _TimeSample(None, before, before, monotonic_before, monotonic_before, "unsupported")
    try:
        raw = await _bounded_optional(reader(TIME_READ_UUID), operation_timeout)
        after = host_time()
        monotonic_after = host_monotonic()
    except _OptionalOperationTimeoutError:
        after = host_time()
        monotonic_after = host_monotonic()
        return _TimeSample(None, before, after, monotonic_before, monotonic_after, "timeout")
    except Exception:  # noqa: BLE001 - optional characteristic
        after = host_time()
        monotonic_after = host_monotonic()
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


async def _emit_safe(emit: OperationalEmitter, event: dict[str, object], timeout: float) -> None:
    """Keep optional journal delivery from changing clock correctness."""
    try:
        await _bounded_optional(_emit(emit, event), timeout)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - telemetry is auxiliary
        return


def _discard_event(event: OperationalEvent) -> None:
    del event
