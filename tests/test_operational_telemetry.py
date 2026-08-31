from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import rmtree
from struct import pack
from types import SimpleNamespace

import pytest

import omi_collector.capture.application.operational_telemetry as operational_telemetry
from fakes import ScriptedRingSession, WriteStep
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


def _event_emitter(events: list[dict[str, object]]) -> OperationalEmitter:
    def emit(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    return emit


def _info() -> RingInfo:
    return RingInfo(10, 12, 100, 2, RECORD_SIZE)


def _status() -> RingStatus:
    return RingStatus(123, 2, 456, 1)


def _run(
    session: FakeOperationalSession,
    events: list[dict[str, object]],
    *,
    times: tuple[float, float] = (1000.0, 1000.0),
    synchronized: bool = True,
    operation_timeout: float = 0.5,
) -> None:
    ticks = iter(times)
    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(lambda: next(ticks), lambda: synchronized, operation_timeout),
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


def test_drift_writes_then_reads_back_once_in_order() -> None:
    target = pack("<I", 1000)
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=target,
    )
    events: list[dict[str, object]] = []
    _run(session, events)

    assert session.writes == [(TIME_WRITE_UUID, target)]
    assert session.reads.index(TIME_READ_UUID) < len(session.reads)
    assert session.reads[-1] == TIME_READ_UUID
    assert events[-1]["outcome"] == "verified"
    assert events[-1]["target_epoch"] == 1000


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
        readback=pack("<I", 1001),
    )
    _run(verify_fail, verify_events)
    assert len(verify_fail.writes) == 1
    assert verify_events[-1]["outcome"] == "verification_failed"


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
            "omi",
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
