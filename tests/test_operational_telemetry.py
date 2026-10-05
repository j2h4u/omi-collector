from __future__ import annotations

import asyncio
import json
import multiprocessing
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from shutil import rmtree
from struct import pack
from types import SimpleNamespace
from typing import cast

import pytest

import omi_collector.capture.application.operational_telemetry as operational_telemetry
from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_corrections import ClockCorrection, ClockCorrectionStore
from omi_collector.capture.adapters.clock_memberships import ClockMembershipStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_store import StagingStore
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
    collect_battery_observation,
    collect_operational_telemetry,
)
from omi_collector.capture.application.opportunistic_sync import run_opportunistic_collector
from omi_collector.capture.application.ports import (
    ClockCorrectionShape,
    ClockObservationPort,
    ClockObservationShape,
    StorageLeasePort,
)
from omi_collector.capture.application.session_lifecycle import OpportunisticOptions, RetryPolicy
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo, RingStatus
from omi_collector.config import DEFAULT_CONFIG, CollectorConfig, ReadyConfig

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


@dataclass(frozen=True, slots=True)
class _FakeCorrection:
    operation_id: str
    state: str
    boundary_sequence_min: int


@dataclass(frozen=True, slots=True)
class _FakeObservation:
    observation_id: str
    evidence_kind: str
    session_id: str
    host_boot_id: str
    host_realtime_start: float
    host_realtime_end: float
    host_monotonic_start: float
    host_monotonic_end: float
    device_epoch: int
    info_sequence_min: int
    info_sequence_max: int
    operation_id: str | None
    effective_boundary_sequence: int | None
    observation_role: str
    parent_observation_id: str | None


class _FakeObservationStore:
    def append(  # noqa: PLR0913 - mirrors the required durable evidence port
        self,
        *,
        evidence_kind: str,
        session_id: str,
        host_boot_id: str,
        host_realtime_start: float,
        host_realtime_end: float,
        host_monotonic_start: float,
        host_monotonic_end: float,
        device_epoch: int,
        info_sequence_min: int,
        info_sequence_max: int,
        operation_id: str | None = None,
        effective_boundary_sequence: int | None = None,
        observation_id: str | None = None,
        observation_role: str = "standalone",
        parent_observation_id: str | None = None,
    ) -> ClockObservationShape:
        return _FakeObservation(
            observation_id or "test-observation",
            evidence_kind,
            session_id,
            host_boot_id,
            host_realtime_start,
            host_realtime_end,
            host_monotonic_start,
            host_monotonic_end,
            device_epoch,
            info_sequence_min,
            info_sequence_max,
            operation_id,
            effective_boundary_sequence,
            observation_role,
            parent_observation_id,
        )

    def records(self) -> tuple[ClockObservationShape, ...]:
        return ()


class FakeClockCorrectionSink:
    def __init__(self) -> None:
        self.finished: list[dict[str, object]] = []
        self.reconcile_calls = 0
        self._observations = _FakeObservationStore()

    @property
    def observation_store(self) -> ClockObservationPort:
        return self._observations

    def note_transport_closed(self) -> None:
        return None

    def prepare(
        self,
        observed_epoch: int,
        target_epoch: int,
        drift_seconds: float,
        boundary_sequence_min: int,
    ) -> ClockCorrectionShape:
        del observed_epoch, target_epoch, drift_seconds
        return _FakeCorrection("test-operation", "prepared", boundary_sequence_min)

    def finish(
        self,
        correction: ClockCorrectionShape,
        *,
        state: str,
        boundary_sequence_max: int | None,
        verified_epoch: int | None,
    ) -> ClockCorrectionShape:
        values = {
            "state": state,
            "boundary_sequence_max": boundary_sequence_max,
            "verified_epoch": verified_epoch,
        }
        self.finished.append({"correction": correction, **values})
        return correction

    def mark_unresolved(self, correction: ClockCorrectionShape) -> ClockCorrectionShape:
        return correction

    def records(self) -> tuple[ClockCorrectionShape, ...]:
        return ()

    def reconcile_causal_observation(
        self, observation: ClockObservationShape, *, near_zero_threshold: float
    ) -> tuple[ClockCorrectionShape, ...]:
        self.reconcile_calls += 1
        del observation, near_zero_threshold
        return ()


def _event_emitter(events: list[dict[str, object]]) -> OperationalEmitter:
    def emit(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    return emit


def _clock_event(events: list[dict[str, object]]) -> dict[str, object]:
    return next(event for event in events if event.get("event") == "pendant_clock_sync")


def _observation_event(events: list[dict[str, object]]) -> dict[str, object]:
    return next(event for event in events if event.get("event") == "pendant_observation")


def _info() -> RingInfo:
    return RingInfo(10, 12, 100, 2, RECORD_SIZE)


def _status() -> RingStatus:
    return RingStatus(123, 2, 456, 1)


def _clock_bundle(root: Path, start_sequence: int, timestamp: int) -> Path:
    raw = timestamp.to_bytes(4, "big") + bytes((2, 8, 0x55)) + bytes(RECORD_SIZE - 7)
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


def test_telemetry_without_emitter_or_clock_does_not_touch_session() -> None:
    session = FakeOperationalSession({TIME_READ_UUID: pack("<I", 1000)})

    asyncio.run(collect_operational_telemetry(session, _status(), _info(), None))

    assert session.reads == []
    assert session.writes == []


def test_telemetry_without_emitter_still_persists_observation(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    store = ClockCorrectionStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            None,
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=lambda: True,
                observation_sink=store.observation_store,
            ),
        )
    )

    observations = store.observation_store.records()
    assert len(observations) == 1
    assert observations[0].device_epoch == 1000
    assert session.writes == []


@pytest.mark.parametrize("timeout", [0, -0.1])
@pytest.mark.parametrize("timeout_field", ["operation_timeout", "host_clock_probe_timeout"])
def test_telemetry_rejects_nonpositive_timeouts_before_device_access(timeout: float, timeout_field: str) -> None:
    session = FakeOperationalSession({TIME_READ_UUID: pack("<I", 1000)})
    clock = (
        TelemetryClock(operation_timeout=timeout)
        if timeout_field == "operation_timeout"
        else TelemetryClock(host_clock_probe_timeout=timeout)
    )

    with pytest.raises(ValueError, match="timeout must be positive"):
        asyncio.run(collect_operational_telemetry(session, _status(), _info(), _event_emitter([]), clock=clock))

    assert session.reads == []
    assert session.writes == []


def test_supplied_status_skips_configured_status_reader() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    status_reads = 0

    async def read_status() -> RingStatus | None:
        nonlocal status_reads
        status_reads += 1
        return None

    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)
    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=lambda: False,
                status_reader=read_status,
            ),
        )
    )

    assert status_reads == 0
    assert _observation_event(events)["used_bytes"] == 123


def test_synchronization_probe_error_skips_time_write_and_durable_evidence(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

    def fail_sync_check() -> bool:
        raise OSError("private host probe detail")

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=fail_sync_check,
                correction_sink=cast(ClockCorrectionSink, store),
            ),
        )
    )

    clock_event = _clock_event(events)
    assert clock_event["outcome"] == "host_unsynchronized"
    assert clock_event["host_ntp_synchronized"] is False
    assert session.writes == []
    assert store.records() == ()
    assert store.observation_store.records() == ()
    assert "private host probe detail" not in str(events)


@pytest.mark.parametrize(
    ("contents", "error", "expected"),
    [
        ("  kernel-boot-id \n", None, "kernel-boot-id"),
        ("", None, "unknown"),
        (None, OSError("private"), "unknown"),
    ],
)
def test_system_host_boot_id_uses_kernel_file_boundary(
    monkeypatch: pytest.MonkeyPatch, contents: str | None, error: OSError | None, expected: str
) -> None:
    reads: list[tuple[Path, str | None]] = []

    def read_text(path: Path, *, encoding: str | None = None) -> str:
        reads.append((path, encoding))
        if path != Path("/proc/sys/kernel/random/boot_id"):
            raise AssertionError("unexpected path")
        if error is not None:
            raise error
        return contents or ""

    monkeypatch.setattr(Path, "read_text", read_text)

    assert operational_telemetry.system_host_boot_id() == expected
    assert reads == [(Path("/proc/sys/kernel/random/boot_id"), "ascii")]


@pytest.mark.parametrize(("host_time", "device_time"), [(-1.0, 100), (float(2**32), 100)])
def test_unrepresentable_clock_target_skips_time_write(host_time: float, device_time: int) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", device_time)})
    events: list[dict[str, object]] = []
    sink = FakeClockCorrectionSink()

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: host_time,
                synchronized=lambda: True,
                correction_sink=sink,
            ),
        )
    )

    assert _clock_event(events)["outcome"] == "target_unrepresentable"
    assert session.writes == []


@pytest.mark.parametrize("target", [0, 2**32 - 1])
def test_representable_clock_target_is_written_as_u32(target: int) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 100)},
        readback=pack("<I", target),
    )
    events: list[dict[str, object]] = []

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: float(target),
                synchronized=lambda: True,
                correction_sink=FakeClockCorrectionSink(),
            ),
        )
    )

    assert session.writes == [(TIME_WRITE_UUID, pack("<I", target))]
    assert _clock_event(events)["outcome"] == "verified"


def test_write_started_at_target_plus_two_is_allowed() -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1001),
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0, 1000.0, 1000.0, 1002.0, 1002.0, 1002.0, 1002.0, 1002.0))

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=lambda: True,
                correction_sink=FakeClockCorrectionSink(),
            ),
        )
    )

    assert session.writes == [(TIME_WRITE_UUID, pack("<I", 1000))]
    assert _clock_event(events)["outcome"] == "verified"


@pytest.mark.parametrize(("readback", "read_finished"), [(1001, 1000.0), (1003, 1002.0)])
def test_readback_allowance_includes_one_second_margin(readback: int, read_finished: float) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", readback),
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0, 1000.0, 1000.0, 1000.0, read_finished, read_finished, read_finished, read_finished))

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=lambda: True,
                correction_sink=FakeClockCorrectionSink(),
            ),
        )
    )

    assert _clock_event(events)["outcome"] == "verified"


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


@pytest.mark.parametrize(("payload", "expected"), [(b"\x00", 0), (b"\x64", 100)])
def test_battery_observation_reports_exact_percent_without_clock_access(payload: bytes, expected: int) -> None:
    session = FakeOperationalSession({BATTERY_UUID: payload, TIME_READ_UUID: pack("<I", 1000)})
    events: list[dict[str, object]] = []

    asyncio.run(collect_battery_observation(session, _info(), _event_emitter(events), operation_timeout=0.1))

    assert len(events) == 1
    assert events[0]["battery_percent"] == expected
    assert events[0]["optional_outcomes"] == {"battery": "ok"}
    assert session.reads == [BATTERY_UUID]
    assert session.writes == []


@pytest.mark.parametrize("payload", [b"\x65", b"", b"\x01\x02"])
def test_battery_observation_classifies_invalid_payload_without_value(payload: bytes) -> None:
    session = FakeOperationalSession({BATTERY_UUID: payload})
    events: list[dict[str, object]] = []

    asyncio.run(collect_battery_observation(session, _info(), _event_emitter(events), operation_timeout=0.1))

    assert len(events) == 1
    assert events[0]["optional_outcomes"] == {"battery": "malformed"}
    assert "battery_percent" not in events[0]


def test_battery_observation_classifies_missing_unsupported_and_failed_reads() -> None:
    class NoOptionalReader:
        pass

    class FailingReader(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            self.reads.append(uuid)
            raise OSError("private transport detail")

    for session, expected in (
        (FakeOperationalSession({BATTERY_UUID: None}), "missing"),
        (NoOptionalReader(), "unsupported"),
        (FailingReader({}), "read_failed"),
    ):
        events: list[dict[str, object]] = []
        asyncio.run(collect_battery_observation(session, _info(), _event_emitter(events), operation_timeout=0.1))
        assert len(events) == 1
        assert events[0]["optional_outcomes"] == {"battery": expected}
        assert "battery_percent" not in events[0]
        assert "private transport detail" not in str(events)


@pytest.mark.parametrize("operation_timeout", [0, -0.1])
def test_battery_observation_rejects_nonpositive_timeout_before_device_access(operation_timeout: float) -> None:
    session = FakeOperationalSession({BATTERY_UUID: b"\x32"})

    with pytest.raises(ValueError, match="timeout must be positive"):
        asyncio.run(
            collect_battery_observation(session, _info(), _event_emitter([]), operation_timeout=operation_timeout)
        )

    assert session.reads == []
    assert session.writes == []


def test_battery_observation_accepts_small_positive_timeout() -> None:
    session = FakeOperationalSession({BATTERY_UUID: b"\x32"})
    events: list[dict[str, object]] = []

    asyncio.run(collect_battery_observation(session, _info(), _event_emitter(events), operation_timeout=0.001))

    assert events[0]["battery_percent"] == 50
    assert events[0]["optional_outcomes"] == {"battery": "ok"}


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (SimpleNamespace(returncode=1, stdout="yes\n"), False),
        (SimpleNamespace(returncode=0, stdout="no\n"), False),
        (SimpleNamespace(returncode=0, stdout="  YeS \n"), True),
    ],
)
def test_system_clock_trust_requires_success_and_normalizes_stdout(
    monkeypatch: pytest.MonkeyPatch, result: SimpleNamespace, expected: bool
) -> None:
    monkeypatch.setattr(operational_telemetry.subprocess, "run", lambda *_args, **_kwargs: result)

    assert operational_telemetry.system_host_clock_synchronized() is expected


def test_system_clock_trust_fails_closed_on_os_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_run(*_args: object, **_kwargs: object) -> SimpleNamespace:
        raise OSError("private process detail")

    monkeypatch.setattr(operational_telemetry.subprocess, "run", fail_run)

    assert operational_telemetry.system_host_clock_synchronized() is False


def test_empty_firmware_is_malformed_and_nonempty_firmware_is_preserved() -> None:
    for firmware, expected_value, expected_outcome in ((b"", None, "malformed"), (b" 1.2 ", " 1.2 ", "ok")):
        events: list[dict[str, object]] = []
        session = FakeOperationalSession({FIRMWARE_UUID: firmware})
        asyncio.run(
            collect_operational_telemetry(
                session,
                _status(),
                _info(),
                _event_emitter(events),
                clock=TelemetryClock(now=lambda: 1000.0, synchronized=lambda: False),
            )
        )
        observation = _observation_event(events)
        outcomes = observation["optional_outcomes"]
        assert isinstance(outcomes, dict)
        assert outcomes["firmware"] == expected_outcome
        if expected_value is not None:
            assert observation["firmware"] == expected_value


def test_timedatectl_timeout_is_reported_as_host_probe_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout_run(*_args: object, timeout: float, **_kwargs: object) -> SimpleNamespace:
        raise operational_telemetry.subprocess.TimeoutExpired("timedatectl", timeout)

    monkeypatch.setattr(operational_telemetry.subprocess, "run", timeout_run)
    events: list[dict[str, object]] = []

    asyncio.run(
        collect_operational_telemetry(
            FakeOperationalSession({TIME_READ_UUID: pack("<I", 1010)}),
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(synchronized=operational_telemetry.system_host_clock_synchronized),
        )
    )

    assert _clock_event(events)["outcome"] == "host_probe_timeout"


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
    observation = _observation_event(events)
    assert observation["used_bytes"] == 123
    assert observation["rtc_valid"] is True
    assert observation["read_sequence"] == 10
    outcomes = observation["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["battery"] == "malformed"
    assert _clock_event(events)["outcome"] == "within_threshold"
    assert "address" not in str(events)
    assert "audio" not in str(events)


def test_healthy_clock_visit_persists_observation(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1005)})
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

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
                correction_sink=cast(ClockCorrectionSink, store),
            ),
        )
    )

    assert store.records() == ()
    observations = store.observation_store.records()
    assert len(observations) == 1
    assert observations[0].device_epoch == 1005
    assert observations[0].observation_role == "standalone"
    assert _clock_event(events)["outcome"] == "within_threshold"


def test_healthy_clock_visit_confirms_only_a_same_session_info_interval(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    memberships = ClockMembershipStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

    async def later_info() -> RingInfo:
        return RingInfo(100, 110, 100, 2, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(90, 100, 100, 2, RECORD_SIZE),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=later_info,
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
                membership_store=memberships,
                session_id="healthy-session",
            ),
        )
    )

    segments = memberships.segments(store.observation_store.records())
    assert segments.utc_for(100, 1000) == 1000.0
    assert segments.utc_for(110, 1000) is None
    assert _clock_event(events)["outcome"] == "within_threshold"


def test_disconnected_clock_visit_does_not_extend_membership(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    store = ClockCorrectionStore(tmp_path / "device.json")
    memberships = ClockMembershipStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

    async def disconnected_info() -> RingInfo:
        raise OSError("BLE disconnected")

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(90, 100, 100, 2, RECORD_SIZE),
            _event_emitter([]),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=disconnected_info,
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
                membership_store=memberships,
                session_id="disconnected-session",
            ),
        )
    )

    assert memberships.records() == ()


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
    assert _clock_event(events)["outcome"] == "host_unsynchronized"


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
    clock_event = _clock_event(events)
    assert clock_event["outcome"] == "verified"
    assert clock_event["target_epoch"] == 1000
    assert clock_event["boundary_sequence_min"] == 12
    assert clock_event["boundary_sequence_max"] == 14


def test_clock_write_confirms_post_readback_same_session_interval(tmp_path: Path) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 43)},
        readback=pack("<I", 72),
    )
    store = ClockCorrectionStore(tmp_path / "device.json")
    memberships = ClockMembershipStore(tmp_path / "device.json")
    ticks = iter((72.0,) * 12)
    infos = iter((RingInfo(90, 100, 100, 2, RECORD_SIZE), RingInfo(100, 110, 100, 2, RECORD_SIZE)))

    async def next_info() -> RingInfo:
        return next(infos)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(80, 90, 100, 2, RECORD_SIZE),
            _event_emitter([]),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.5,
                info_reader=next_info,
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
                membership_store=memberships,
                session_id="written-session",
            ),
        )
    )

    segments = memberships.segments(store.observation_store.records())
    assert segments.utc_for(100, 72) == 72.0
    assert segments.utc_for(110, 72) is None


def test_timed_out_post_write_readback_does_not_record_membership(tmp_path: Path) -> None:
    class DelayedReadback(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            if uuid == TIME_READ_UUID and self.writes:
                await asyncio.sleep(0.02)
            return await super().read_optional_characteristic(uuid)

    session = DelayedReadback(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 43)},
        readback=pack("<I", 72),
    )
    store = ClockCorrectionStore(tmp_path / "device.json")
    memberships = ClockMembershipStore(tmp_path / "device.json")
    ticks = iter((72.0,) * 12)

    async def later_info() -> RingInfo:
        return RingInfo(90, 100, 100, 2, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(80, 90, 100, 2, RECORD_SIZE),
            _event_emitter([]),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda: True,
                0.01,
                info_reader=later_info,
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
                membership_store=memberships,
            ),
        )
    )

    assert session.writes == [(TIME_WRITE_UUID, pack("<I", 72))]
    assert memberships.records() == ()


def test_verified_correction_skips_membership_when_store_is_absent(tmp_path: Path) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1000),
    )
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    info_reads = 0
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        nonlocal info_reads
        info_reads += 1
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
                observation_sink=store.observation_store,
            ),
        )
    )

    [correction] = store.records()
    assert correction.state == "applied"
    assert correction.boundary_sequence_max == 14
    assert info_reads == 1
    assert _clock_event(events)["outcome"] == "verified"


def test_unwritten_correction_does_not_read_post_write_boundary(tmp_path: Path) -> None:
    class MissingTimeWrite(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
            self.writes.append((uuid, value))
            return False

    session = MissingTimeWrite({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    info_reads = 0
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        nonlocal info_reads
        info_reads += 1
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
                observation_sink=store.observation_store,
            ),
        )
    )

    event = _clock_event(events)
    assert event["outcome"] == "time_write_missing"
    assert "boundary_sequence_max" not in event
    assert info_reads == 0


def test_clock_mutation_lease_guards_durable_sync_writes() -> None:
    active = False
    entries: list[str] = []

    class Lease:
        def require_active(self) -> None:
            return None

    @contextmanager
    def mutation_lease() -> Iterator[Lease]:
        nonlocal active
        entries.append("entered")
        active = True
        try:
            yield Lease()
        finally:
            active = False
            entries.append("exited")

    class GuardedSink(FakeClockCorrectionSink):
        def prepare(
            self, observed_epoch: int, target_epoch: int, drift_seconds: float, boundary_sequence_min: int
        ) -> ClockCorrectionShape:
            assert active
            return super().prepare(observed_epoch, target_epoch, drift_seconds, boundary_sequence_min)

        def mark_unresolved(self, correction: ClockCorrectionShape) -> ClockCorrectionShape:
            assert active
            return super().mark_unresolved(correction)

        def finish(
            self,
            correction: ClockCorrectionShape,
            *,
            state: str,
            boundary_sequence_max: int | None,
            verified_epoch: int | None,
        ) -> ClockCorrectionShape:
            assert active
            return super().finish(
                correction,
                state=state,
                boundary_sequence_max=boundary_sequence_max,
                verified_epoch=verified_epoch,
            )

        def reconcile_causal_observation(
            self, observation: ClockObservationShape, *, near_zero_threshold: float
        ) -> tuple[ClockCorrectionShape, ...]:
            assert active
            return super().reconcile_causal_observation(observation, near_zero_threshold=near_zero_threshold)

    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)}, readback=pack("<I", 1000)
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

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
                correction_sink=GuardedSink(),
                mutation_lease=mutation_lease,
            ),
        )
    )

    assert entries == ["entered", "exited"]
    assert _clock_event(events)["outcome"] == "verified"


def test_busy_clock_mutation_lease_defers_clock_sync_without_a_session_error() -> None:
    class BusyLease:
        def __enter__(self) -> StorageLeasePort:
            raise OSError("writer owns the storage lease")

        def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

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
                correction_sink=FakeClockCorrectionSink(),
                mutation_lease=lambda: BusyLease(),
            ),
        )
    )

    assert session.writes == []
    assert _clock_event(events)["outcome"] == "storage_lease_unavailable"


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


def test_equal_post_write_sequence_is_persisted_as_effective_boundary(tmp_path: Path) -> None:
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1000),
    )
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
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
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
            ),
        )
    )

    [correction] = store.records()
    later = next(item for item in store.observation_store.records() if item.observation_role == "later")
    assert correction.state == "resolved"
    assert correction.boundary_sequence_max == _info().write_sequence
    assert later.effective_boundary_sequence == _info().write_sequence
    assert _clock_event(events)["outcome"] == "verified"


def test_unresolved_operation_without_initial_observation_keeps_sample_standalone(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json")
    store.mark_unresolved(store.prepare(1010, 1000, 10.0, _info().write_sequence))
    events: list[dict[str, object]] = []
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    ticks = iter((1000.0,) * 8)

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
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
            ),
        )
    )

    [observation] = store.observation_store.records()
    assert observation.operation_id is None
    assert observation.observation_role == "standalone"
    assert store.records()[0].state == "unresolved"
    assert _clock_event(events)["reconciliation"] == "no_causal_operation"


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
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
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
    store.note_transport_closed()

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
    assert _clock_event(events)["outcome"] == "within_threshold"
    later = next(item for item in store.observation_store.records() if item.observation_role == "later")
    assert later.parent_observation_id == initial.observation_id
    assert later.operation_id == pending.operation_id


def test_native_clock_handoff_43_to_72_at_incident_frontier(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json")
    capture_root = _capture_root(tmp_path)
    published = tmp_path / "published"
    published.mkdir(mode=0o2750)
    published.chmod(0o2750)
    staging = StagingStore.from_paths(
        StagingStore(tmp_path / "spool", capture_root).paths,
        publication_root=published,
        config=CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02)),
    )
    retries: list[bool] = []
    authority = staging.create_publication_authority(lambda: retries.append(True))
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
                observation_sink=store.observation_store,
                publisher=authority,
                mutation_lease=staging.clock_mutation_lease,
            ),
        )
    )

    correction = store.records()[0]
    assert correction.observed_epoch == 43
    assert correction.target_epoch == 72
    assert correction.boundary_sequence_min == 7_763_451
    assert correction.state == "resolved"
    assert correction.verified_epoch == 72
    assert {"initial", "later"} <= {item.observation_role for item in store.observation_store.records()}
    clock_event = _clock_event(events)
    assert clock_event["outcome"] == "verified"
    assert clock_event.get("publication") != "failed"
    assert retries == []
    with staging.device_lock(recover_capture_temporaries=False, operation="capture_batch") as lease:
        staging.require_device_lock(lease)
    authority.close()


def test_native_clock_handoff_publishes_raw_bundles_after_restart_without_ble(tmp_path: Path) -> None:
    capture_root = _capture_root(tmp_path)
    _clock_bundle(capture_root, 7_717_544, 43)
    _clock_bundle(capture_root, 7_717_545, 72)
    (tmp_path / "timeline-repairs.json").write_text(json.dumps({"version": 1, "repairs": []}), encoding="utf-8")
    published = tmp_path / "published"
    published.mkdir(mode=0o2750)
    published.chmod(0o2750)
    staging = StagingStore.from_paths(
        StagingStore(tmp_path, capture_root).paths,
        publication_root=published,
        config=CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02)),
    )
    authority = staging.create_publication_authority()
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

    async def collect_from_bounded_child() -> None:
        task = asyncio.create_task(
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
                    publisher=authority,
                    mutation_lease=staging.clock_mutation_lease,
                ),
            )
        )
        await task

    asyncio.run(collect_from_bounded_child())
    assert _clock_event(events).get("publication") != "failed"

    durable_store = ClockCorrectionStore(staging.device_state_path)
    corrections = durable_store.records()
    assert corrections[0].observed_epoch == 43
    assert corrections[0].target_epoch == 72
    assert corrections[0].boundary_sequence_min == 7_717_545
    assert corrections[0].boundary_sequence_max == 7_763_451
    assert corrections[0].state == "applied"
    observations = durable_store.observation_store.records()
    initial = next(item for item in observations if item.observation_role == "initial")
    later = next(item for item in observations if item.observation_role == "later")
    assert initial.device_epoch == 43
    assert later.device_epoch == 72
    assert later.parent_observation_id == initial.observation_id

    staging.append_ready_closure(frontier.write_sequence + 1, "drained")
    assert staging.recover_and_publish() is not None
    ready = tuple(path for path in (tmp_path / "published").iterdir() if path.is_dir())
    assert len(ready) == 1
    assert tuple(capture_root.iterdir()) == ()
    assert all(json.loads((path / "manifest.json").read_text(encoding="utf-8"))["time_ranges"] for path in ready)
    assert ClockCorrectionStore(staging.device_state_path).records()[0].state == "applied"
    authority.close()


def test_rtc_valid_is_telemetry_only_and_unsynchronized_host_does_not_write() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    _run(session, events, synchronized=False)

    assert session.writes == []
    assert _observation_event(events)["rtc_valid"] is True
    assert _clock_event(events)["outcome"] == "host_unsynchronized"


def test_write_and_verification_failures_are_classified_without_retry() -> None:
    class WriteFail(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> None:
            self.writes.append((uuid, value))
            raise OSError("backend detail must stay private")

    write_events: list[dict[str, object]] = []
    write_fail = WriteFail({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    _run(write_fail, write_events)
    assert len(write_fail.writes) == 1
    assert _clock_event(write_events)["outcome"] == "time_write_failed"

    verify_events: list[dict[str, object]] = []
    verify_fail = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1005),
    )
    _run(verify_fail, verify_events)
    assert len(verify_fail.writes) == 1
    assert _clock_event(verify_events)["outcome"] == "verification_failed"


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


def test_real_clock_store_binds_initial_observation_to_unresolved_intent(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = ClockCorrectionStore(tmp_path / "device.json")
    ticks = iter((1000.0,) * 8)

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
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
            ),
        )
    )

    correction = store.records()[0]
    initial = next(item for item in store.observation_store.records() if item.observation_role == "initial")
    assert correction.state == "unresolved"
    assert initial.operation_id == correction.operation_id
    assert _clock_event(events)["outcome"] == "verification_failed"


def test_successive_same_visit_corrections_get_independent_causal_observations(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json")
    events: list[dict[str, object]] = []

    def collect(epoch: int, readback: int | None, session_id: str) -> None:
        session = FakeOperationalSession(
            {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", epoch)},
            readback=pack("<I", readback) if readback is not None else None,
        )
        asyncio.run(
            collect_operational_telemetry(
                session,
                _status(),
                _info(),
                _event_emitter(events),
                clock=TelemetryClock(
                    lambda: 1000.0,
                    lambda: True,
                    0.5,
                    correction_sink=cast(ClockCorrectionSink, store),
                    observation_sink=store.observation_store,
                    session_id=session_id,
                ),
            )
        )

    collect(1010, 1005, "visit-one")
    first = store.records()[0]
    assert first.state == "unresolved"

    collect(1010, 1005, "visit-one")
    corrections = store.records()
    first = next(item for item in corrections if item.operation_id == first.operation_id)
    second = next(item for item in corrections if item.operation_id != first.operation_id)
    assert first.state == "not_applied"
    assert second.state == "unresolved"
    second_initial = next(
        item
        for item in store.observation_store.records()
        if item.operation_id == second.operation_id and item.observation_role == "initial"
    )
    assert second_initial.parent_observation_id is not None

    store.note_transport_closed()
    collect(1000, None, "visit-two")
    corrections = store.records()
    second = next(item for item in corrections if item.operation_id == second.operation_id)
    assert second.state == "applied"
    later = next(
        item
        for item in store.observation_store.records()
        if item.operation_id == second.operation_id and item.observation_role == "later"
    )
    assert later.parent_observation_id == second_initial.observation_id
    assert all(event.get("reconciliation") != "no_causal_operation" for event in events)


def test_closed_session_fence_allows_fresh_automatic_correction_after_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "device.json"
    store = ClockCorrectionStore(state_path)
    previous = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 12))
    initial = store.observation_store.append(
        evidence_kind="native_trusted",
        session_id="before-restart",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1300,
        info_sequence_min=12,
        info_sequence_max=12,
        operation_id=previous.operation_id,
        observation_role="initial",
    )
    store = ClockCorrectionStore(state_path)

    async def info_after() -> RingInfo:
        return _info()

    def collect(epoch: int, session_id: str, *, readback: int | None = None) -> FakeOperationalSession:
        session = FakeOperationalSession(
            {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", epoch)},
            readback=pack("<I", readback) if readback is not None else None,
        )
        asyncio.run(
            collect_operational_telemetry(
                session,
                _status(),
                _info(),
                _event_emitter([]),
                clock=TelemetryClock(
                    lambda: 1000.0,
                    lambda: True,
                    0.5,
                    info_reader=info_after,
                    correction_sink=cast(ClockCorrectionSink, store),
                    observation_sink=store.observation_store,
                    session_id=session_id,
                ),
            )
        )
        return session

    first_session = collect(1320, "restart-session-one")
    assert first_session.writes == []
    assert next(item for item in store.records() if item.operation_id == previous.operation_id).state == "unresolved"

    store.note_transport_closed()
    next_session = collect(1320, "restart-session-two", readback=1000)

    corrections = store.records()
    previous = next(item for item in corrections if item.operation_id == previous.operation_id)
    current = next(item for item in corrections if item.operation_id != previous.operation_id)
    assert previous.state == "unknown"
    assert len(next_session.writes) == 1
    assert next_session.writes[0][1] == pack("<I", 1000)
    assert current.state == "resolved"
    current_initial = next(
        item
        for item in store.observation_store.records()
        if item.operation_id == current.operation_id and item.observation_role == "initial"
    )
    assert current_initial.parent_observation_id != initial.observation_id


def test_missing_time_write_is_not_reported_as_performed() -> None:
    class MissingWriter(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
            self.writes.append((uuid, value))
            return False

    session = MissingWriter({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    _run(session, events)

    assert len(session.writes) == 1
    clock_event = _clock_event(events)
    assert clock_event["action"] == "none"
    assert clock_event["outcome"] == "time_write_missing"
    assert "target_epoch" not in clock_event


def test_optional_characteristic_failures_are_nonblocking() -> None:
    class Failing(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            self.reads.append(uuid)
            if uuid != BATTERY_UUID:
                raise OSError("must not be emitted")
            return bytes((42,))

    events: list[dict[str, object]] = []
    _run(Failing({}), events)

    observation = _observation_event(events)
    assert observation["battery_percent"] == 42
    outcomes = observation["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["device_time"] == "read_failed"
    assert _clock_event(events)["outcome"] == "device_time_read_failed"


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

    observation = _observation_event(events)
    assert observation["battery_percent"] == 42
    outcomes = observation["optional_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["model"] == "timeout"
    assert outcomes["device_time"] == "timeout"


def test_hanging_host_clock_probe_uses_configured_host_timeout() -> None:
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
                operation_timeout=0.1,
                host_clock_probe_timeout=0.02,
            ),
        )
    )

    assert time.monotonic() - started < 0.1
    assert _clock_event(events)["outcome"] == "host_probe_timeout"


def _blocked_trust_probe_child(result: Connection) -> None:
    gate = threading.Event()
    events: list[dict[str, object]] = []
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    asyncio.run(
        operational_telemetry.collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: 1000.0,
                synchronized=gate.wait,
                host_clock_probe_timeout=0.02,
            ),
        )
    )
    result.send(_clock_event(events)["outcome"])
    result.close()


def test_timed_out_trust_probe_does_not_keep_child_process_alive() -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("trust probe process-exit check requires fork")
    context = multiprocessing.get_context("fork")
    receive_result, send_result = context.Pipe(duplex=False)
    process = context.Process(target=_blocked_trust_probe_child, args=(send_result,))
    started = False
    try:
        process.start()
        started = True
        send_result.close()
        assert receive_result.poll(1.0), "child did not report the bounded public probe outcome"
        assert receive_result.recv() == "host_probe_timeout"
        process.join(timeout=2.0)
        assert process.exitcode == 0, "timed-out trust probe kept its child process alive"
    finally:
        receive_result.close()
        send_result.close()
        if started and process.is_alive():
            process.terminate()
            process.join(timeout=1.0)
        if started and process.is_alive():
            process.kill()
            process.join(timeout=1.0)
        if started:
            process.close()


def test_slow_host_probe_and_large_ledger_finish_clock_stage_before_metadata(tmp_path: Path) -> None:
    correction_root = tmp_path / "clock-corrections"
    correction_root.mkdir()
    for index in range(100):
        correction = ClockCorrection(2, f"history-{index:03}", "not_written", 100, 100, 0.0, index)
        (correction_root / f"{correction.operation_id}.json").write_text(
            json.dumps(asdict(correction), sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
        )
    store = ClockCorrectionStore(tmp_path / "device.json")

    class SlowMetadata(FakeOperationalSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            if uuid == MODEL_UUID:
                trace.append("metadata")
                await asyncio.sleep(0.05)
            return await super().read_optional_characteristic(uuid)

    session = SlowMetadata(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 100)},
        readback=pack("<I", 1000),
    )
    events: list[dict[str, object]] = []
    trace: list[str] = []

    def delayed_host_probe() -> bool:
        time.sleep(0.65)
        return True

    async def later_info() -> RingInfo:
        await asyncio.sleep(0.05)
        return RingInfo(10, 13, 100, 2, RECORD_SIZE)

    def emit(event: Mapping[str, object]) -> None:
        if event.get("event") == "pendant_clock_sync":
            trace.append("clock_event")
        events.append(dict(event))

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            emit,
            clock=TelemetryClock(
                now=lambda: 1000.0,
                synchronized=delayed_host_probe,
                operation_timeout=0.5,
                host_clock_probe_timeout=1.0,
                info_reader=later_info,
                correction_sink=cast(ClockCorrectionSink, store),
                observation_sink=store.observation_store,
            ),
        )
    )

    correction = next(item for item in store.records() if item.boundary_sequence_min == 12)
    assert len(store.records()) == 101
    assert correction.state == "applied"
    assert _clock_event(events)["outcome"] == "verified"
    assert trace.index("clock_event") < trace.index("metadata")


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
                TransferTimeouts(
                    DEFAULT_CONFIG.transfer.info_timeout_seconds,
                    DEFAULT_CONFIG.transfer.sync_timeout_seconds,
                ),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=_event_emitter(events),
                host_clock_synchronized=lambda: False,
            ),
            runtime=OpportunisticRuntime(),
        )
    )

    assert session.writes == [b"\x10"]
    observation = _observation_event(events)
    assert observation["read_sequence"] == 10
    assert observation["write_sequence"] == 10
