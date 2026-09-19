from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path
from shutil import rmtree
from struct import pack
from types import SimpleNamespace
from typing import cast

import pytest

import omi_collector.capture.application.operational_telemetry as operational_telemetry
from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.timeline_generations import GenerationResult
from omi_collector.capture.application.collector import TransferTimeouts
from omi_collector.capture.application.operational_telemetry import (
    BATTERY_UUID,
    FIRMWARE_UUID,
    HARDWARE_UUID,
    MANUFACTURER_UUID,
    MODEL_UUID,
    TIME_READ_UUID,
    TIME_WRITE_UUID,
    ClockCorrectionSink,
    OperationalEmitter,
    TelemetryClock,
    collect_operational_telemetry,
)
from omi_collector.capture.application.opportunistic_sync import run_opportunistic_collector
from omi_collector.capture.application.session_lifecycle import OpportunisticOptions, RetryPolicy
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo, RingStatus
from omi_collector.config import DEFAULT_CONFIG

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


class FakeOperationalSession:
    def __init__(self, values: dict[str, bytes | None], *, readback: bytes | None = None) -> None:
        self.values = values
        self.reads: list[str] = []
        self.writes: list[tuple[str, bytes]] = []
        self.readback = readback

    async def read_optional_characteristic(self, uuid: str) -> bytes | None:
        self.reads.append(uuid)
        if uuid == TIME_READ_UUID and self.readback is not None and self.writes:
            return self.readback
        return self.values.get(uuid)

    async def write_optional_characteristic(self, uuid: str, value: bytes) -> object:
        self.writes.append((uuid, value))


class FakeClockCorrectionSink:
    def __init__(self) -> None:
        self.finished: list[dict[str, object]] = []
        self.reconcile_calls = 0

    def prepare(
        self,
        observed_epoch: int,
        target_epoch: int,
        drift_seconds: float,
        boundary_sequence_min: int,
    ) -> object:
        return observed_epoch, target_epoch, drift_seconds, boundary_sequence_min

    def finish(self, correction: object, **values: object) -> object:
        self.finished.append({"correction": correction, **values})
        return correction

    def mark_unresolved(self, correction: object) -> object:
        return correction

    def reconcile_observation(
        self,
        observed_epoch: int,
        drift_seconds: float,
        boundary_sequence_max: int,
        *,
        near_zero_threshold: float,
    ) -> tuple[object, ...]:
        self.reconcile_calls += 1
        del observed_epoch, drift_seconds, boundary_sequence_max, near_zero_threshold
        return ()


def _event_emitter(events: list[dict[str, object]]) -> OperationalEmitter:
    def emit(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    return emit


def _info() -> RingInfo:
    return RingInfo(10, 12, 100, 2, RECORD_SIZE)


def _status() -> RingStatus:
    return RingStatus(123, 2, 456, 1)


def _clock_bundle(root: Path, start_sequence: int, timestamp: int) -> Path:
    raw = timestamp.to_bytes(4, "big") + b"x" * (RECORD_SIZE - 4)
    digest = sha256(raw).hexdigest()
    bundle = root / f"{start_sequence}-{start_sequence + 1}-{digest[:16]}"
    bundle.mkdir(parents=True)
    (bundle / "records.bin").write_bytes(raw)
    (bundle / "manifest.json").write_text(
        json.dumps(BundleManifest(2, start_sequence, start_sequence + 1, 1, RECORD_SIZE, digest).as_dict()),
        encoding="utf-8",
    )
    (bundle / "receipt.json").write_text(
        json.dumps(SealedReceipt("a" * 32, digest).as_dict()),
        encoding="utf-8",
    )
    return bundle


def _run(
    session: FakeOperationalSession,
    events: list[dict[str, object]],
    *,
    times: tuple[float, float] = (1000.0, 1000.0),
    synchronized: bool = True,
    operation_timeout: float = 0.5,
) -> None:
    ticks = iter((*times, *(times[-1] for _ in range(5))))
    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: synchronized,
                operation_timeout,
                correction_sink=FakeClockCorrectionSink(),
            ),
        )
    )


def test_telemetry_defaults_project_from_runtime_config() -> None:
    configured = DEFAULT_CONFIG.telemetry

    assert configured.clock_drift_threshold_seconds == operational_telemetry.CLOCK_DRIFT_THRESHOLD_SECONDS
    assert configured.optional_operation_timeout_seconds == operational_telemetry.OPTIONAL_OPERATION_TIMEOUT_SECONDS
    assert configured.host_clock_probe_timeout_seconds == operational_telemetry.HOST_CLOCK_PROBE_TIMEOUT_SECONDS
    assert operational_telemetry.METADATA_VALUE_MAX_CHARS == 128
    assert configured.optional_operation_timeout_seconds == TelemetryClock().operation_timeout
    assert configured.host_clock_probe_timeout_seconds == TelemetryClock().host_clock_probe_timeout


def test_system_clock_check_uses_supported_timedatectl_show_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="yes\n")

    monkeypatch.setattr(operational_telemetry.subprocess, "run", fake_run)

    assert operational_telemetry.system_host_clock_synchronized() is True
    assert calls == [
        (
            ("timedatectl", "show", "--property=NTPSynchronized", "--value"),
            {"capture_output": True, "check": False, "text": True, "timeout": 1.0},
        )
    ]


def test_boundary_drift_does_not_write_and_observation_is_safe() -> None:
    session = FakeOperationalSession(
        {
            BATTERY_UUID: b"87",
            TIME_READ_UUID: pack("<I", 1005),
            MODEL_UUID: b"CV1",
            FIRMWARE_UUID: b"1.2",
            HARDWARE_UUID: b"rev-a",
            MANUFACTURER_UUID: b"Omi",
        }
    )
    events: list[dict[str, object]] = []

    # A malformed battery is classified and never leaks its bytes; exactly 5 s is inclusive.
    session.values[BATTERY_UUID] = b"\xff\xff"
    _run(session, events)

    assert session.writes == []
    assert events[0]["event"] == "pendant_observation"
    assert events[0]["used_bytes"] == 123
    assert events[0]["rtc_valid"] is True
    assert events[0]["read_sequence"] == 10
    outcomes = events[0]["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["battery"] == "malformed"
    assert events[1]["outcome"] == "within_threshold"
    assert "address" not in str(events)
    assert "audio" not in str(events)


def test_unsynchronized_host_does_not_reconcile_near_zero_observation() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1005)})
    events: list[dict[str, object]] = []
    sink = FakeClockCorrectionSink()

    ticks = iter((1000.0, 1000.0, *(1000.0 for _ in range(5))))
    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: False,
                0.5,
                correction_sink=sink,
            ),
        )
    )

    assert sink.reconcile_calls == 0
    assert events[-1]["outcome"] == "host_unsynchronized"


def test_drift_writes_then_reads_back_once_in_order() -> None:
    target = pack("<I", 1000)
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=target,
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        return RingInfo(10, 14, 100, 2, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=info_after,
                correction_sink=FakeClockCorrectionSink(),
            ),
        )
    )

    assert session.writes == [(TIME_WRITE_UUID, target)]
    time_reads = [index for index, uuid in enumerate(session.reads) if uuid == TIME_READ_UUID]
    assert len(time_reads) == 2
    assert time_reads[-1] < session.reads.index(MODEL_UUID)
    assert events[-1]["outcome"] == "verified"
    assert events[-1]["target_epoch"] == 1000
    assert events[-1]["boundary_sequence_min"] == 12
    assert events[-1]["boundary_sequence_max"] == 14


def test_verified_zero_width_boundary_is_resolved_immediately() -> None:
    target = pack("<I", 1000)
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=target,
    )
    events: list[dict[str, object]] = []
    sink = FakeClockCorrectionSink()
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        return _info()

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=info_after,
                correction_sink=sink,
            ),
        )
    )

    assert sink.finished[-1]["state"] == "resolved"
    assert sink.finished[-1]["boundary_sequence_max"] == 12


def test_verified_write_without_post_boundary_stays_unresolved() -> None:
    target = pack("<I", 1000)
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=target,
    )
    events: list[dict[str, object]] = []
    sink = FakeClockCorrectionSink()
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(lambda: next(ticks), lambda: True, 0.5, correction_sink=sink),
        )
    )

    assert sink.finished[-1]["state"] == "unresolved"
    assert sink.finished[-1]["boundary_sequence_max"] is None


def test_incident_boundaries_use_trusted_near_zero_observation(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json")
    zero = store.mark_unresolved(store.prepare(1000, 1000, 0.0, 7192026))
    store.finish(zero, state="resolved", boundary_sequence_max=7192026, verified_epoch=1000)
    pending = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 7717545))
    initial = store.observation_store.native_trusted(
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1302,
        info_sequence_min=7717545,
        info_sequence_max=7717545,
        operation_id=pending.operation_id,
        observation_role="initial",
    )
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1005)})
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(0, 7861464, 100, 2, RECORD_SIZE),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                correction_sink=cast(ClockCorrectionSink, store),
            ),
        )
    )

    correction = next(item for item in store.records() if item.operation_id == pending.operation_id)
    assert correction.state == "applied"
    assert correction.boundary_sequence_max == 7861464
    assert events[-1]["outcome"] == "within_threshold"
    later = next(item for item in store.observation_store.records() if item.observation_role == "later")
    assert later.parent_observation_id == initial.observation_id
    assert later.operation_id == pending.operation_id


def test_native_clock_handoff_43_to_72_at_incident_frontier(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json")
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 43)},
        readback=pack("<I", 72),
    )
    events: list[dict[str, object]] = []
    info = RingInfo(0, 7_763_451, 100, 2, RECORD_SIZE)
    ticks = iter((72.0,) * 12)

    async def info_after() -> RingInfo:
        return info

    asyncio.run(
        collect_operational_telemetry(
            session,
            RingStatus(1, 1, 2, 1),
            info,
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=info_after,
                correction_sink=cast(ClockCorrectionSink, store),
            ),
        )
    )

    correction = store.records()[0]
    assert correction.observed_epoch == 43
    assert correction.target_epoch == 72
    assert correction.boundary_sequence_min == 7_763_451
    assert correction.state == "resolved"
    assert correction.verified_epoch == 72
    assert events[-1]["outcome"] == "verified"


def test_native_clock_handoff_publishes_raw_bundles_after_restart_without_ble(tmp_path: Path) -> None:
    capture_root = _capture_root(tmp_path)
    _clock_bundle(capture_root, 7_192_026, 43)
    _clock_bundle(capture_root, 7_763_451, 72)
    (tmp_path / "timeline-repairs.json").write_text(json.dumps({"version": 1, "repairs": []}), encoding="utf-8")
    staging = StagingStore.from_paths(
        StagingStore(tmp_path, capture_root).paths,
        publication_root=tmp_path / "published",
    )
    before_raw = {path.name: (path / "records.bin").read_bytes() for path in capture_root.iterdir() if path.is_dir()}
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 43)},
        readback=pack("<I", 72),
    )
    info = RingInfo(0, 7_717_545, 100, 2, RECORD_SIZE)
    frontier = RingInfo(0, 7_763_451, 100, 2, RECORD_SIZE)
    events: list[dict[str, object]] = []
    ticks = iter((72.0,) * 12)

    async def info_after() -> RingInfo:
        return frontier

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            info,
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=info_after,
                correction_sink=cast(ClockCorrectionSink, ClockCorrectionStore(staging.device_state_path)),
                publisher=staging.publish_timeline,
            ),
        )
    )

    durable_store = ClockCorrectionStore(staging.device_state_path)
    corrections = durable_store.records()
    assert corrections[0].observed_epoch == 43
    assert corrections[0].target_epoch == 72
    assert corrections[0].boundary_sequence_min == 7_717_545
    assert corrections[0].boundary_sequence_max == 7_763_451
    assert corrections[0].state == "resolved"
    observations = durable_store.observation_store.records()
    initial = next(item for item in observations if item.observation_role == "initial")
    later = next(item for item in observations if item.observation_role == "later")
    assert initial.device_epoch == 43
    assert later.device_epoch == 72
    assert later.parent_observation_id == initial.observation_id

    generation = staging.recover_and_publish()

    assert isinstance(generation, GenerationResult)
    assert generation.bundle_count == 2
    assert generation.record_count == 2
    assert (tmp_path / "published" / "current").is_symlink()
    assert {
        path.name: (path / "records.bin").read_bytes() for path in capture_root.iterdir() if path.is_dir()
    } == before_raw
    assert ClockCorrectionStore(staging.device_state_path).records()[0].state == "resolved"


def test_rtc_valid_is_telemetry_only_and_unsynchronized_host_does_not_write() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    _run(session, events, synchronized=False)

    assert session.writes == []
    assert events[0]["rtc_valid"] is True
    assert events[-1]["outcome"] == "host_unsynchronized"


def test_write_and_verification_failures_are_classified_without_retry() -> None:
    class WriteFail(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> None:
            self.writes.append((uuid, value))
            raise OSError("backend detail must stay private")

    write_events: list[dict[str, object]] = []
    write_fail = WriteFail({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    _run(write_fail, write_events)
    assert len(write_fail.writes) == 1
    assert write_events[-1]["outcome"] == "time_write_failed"

    verify_events: list[dict[str, object]] = []
    verify_fail = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1005),
    )
    _run(verify_fail, verify_events)
    assert len(verify_fail.writes) == 1
    assert verify_events[-1]["outcome"] == "verification_failed"


def test_verification_failure_keeps_unresolved_boundary_fields_empty(tmp_path: Path) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1005),
    )
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        return RingInfo(10, 14, 100, 2, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=info_after,
                correction_sink=cast(ClockCorrectionSink, store),
            ),
        )
    )

    correction = store.records()[0]
    assert correction.state == "unresolved"
    assert correction.boundary_sequence_max is None
    assert correction.verified_epoch is None


def test_missing_time_write_is_not_reported_as_performed() -> None:
    class MissingWriter(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
            self.writes.append((uuid, value))
            return False

    session = MissingWriter({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    _run(session, events)

    assert len(session.writes) == 1
    assert events[-1]["action"] == "none"
    assert events[-1]["outcome"] == "time_write_missing"
    assert "target_epoch" not in events[-1]


def test_optional_characteristic_failures_are_nonblocking() -> None:
    class Failing(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            self.reads.append(uuid)
            if uuid != BATTERY_UUID:
                raise OSError("must not be emitted")
            return bytes((42,))

    events: list[dict[str, object]] = []
    _run(Failing({}), events)

    assert events[0]["battery_percent"] == 42
    outcomes = events[0]["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["device_time"] == "read_failed"
    assert events[-1]["outcome"] == "device_time_read_failed"


def test_hanging_optional_reads_are_short_bounded_and_observation_still_emits() -> None:
    class Hanging(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            self.reads.append(uuid)
            if uuid == BATTERY_UUID:
                return bytes((42,))
            await asyncio.Future()
            return None

    events: list[dict[str, object]] = []
    asyncio.run(
        collect_operational_telemetry(
            Hanging({}),
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(operation_timeout=0.01),
        )
    )

    assert events[0]["battery_percent"] == 42
    outcomes = events[0]["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["model"] == "timeout"
    assert outcomes["device_time"] == "timeout"


def test_hanging_host_clock_probe_uses_operation_timeout() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []

    def hanging_probe() -> bool:
        time.sleep(0.2)
        return True

    started = time.monotonic()
    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                synchronized=hanging_probe,
                operation_timeout=0.01,
                host_clock_probe_timeout=1.0,
            ),
        )
    )

    assert time.monotonic() - started < 0.1
    assert events[-1]["outcome"] == "host_unsynchronized"


def test_presence_telemetry_reuses_first_info_without_duplicate_info_read(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(), (WriteStep(b"\x10", (b"\x02" + pack(">QQIQH", 10, 10, 100, 0, RECORD_SIZE),)),)
    )
    events: list[dict[str, object]] = []

    @asynccontextmanager
    async def provider(_candidate: object | None = None):
        yield session

    asyncio.run(
        run_opportunistic_collector(
            lambda _candidate: provider(_candidate),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            OpportunisticOptions(
                TransferTimeouts(1, 1),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=_event_emitter(events),
                host_clock_synchronized=lambda: False,
            ),
            runtime=OpportunisticRuntime(),
        )
    )

    assert session.writes == [b"\x10"]
    assert events[0]["read_sequence"] == 10
    assert events[0]["write_sequence"] == 10
