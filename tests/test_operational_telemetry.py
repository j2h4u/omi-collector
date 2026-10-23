from __future__ import annotations

import asyncio
import json
import multiprocessing
import sqlite3
import threading
import time
from collections.abc import Mapping
from contextlib import asynccontextmanager
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from shutil import rmtree
from struct import pack
from types import SimpleNamespace

import pytest

import omi_collector.capture.application.operational_telemetry as operational_telemetry
from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.clock_state_store import ClockStateStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
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
    collect_battery_observation,
    collect_operational_telemetry,
)
from omi_collector.capture.application.opportunistic_sync import run_opportunistic_collector
from omi_collector.capture.application.session_lifecycle import OpportunisticOptions, RetryPolicy
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.clock_state import ClockAnchor, ClockIntent, ClockRange, ClockState
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


class FakeClockStateStore:
    def __init__(self) -> None:
        self.current = ClockState()
        self.calls: list[str] = []
        self.fail_on: str | None = None

    def state(self) -> ClockState:
        return self.current

    def observe(
        self,
        info: RingInfo,
        *,
        offset_seconds: float,
        uncertainty_seconds: float,
        source_id: str,
        tolerance_seconds: float,
    ) -> ClockState:
        self.calls.append("observe")
        self._fail_if("observe")
        read = info.read_sequence
        write = info.write_sequence
        offset = offset_seconds
        uncertainty = uncertainty_seconds
        tolerance = tolerance_seconds
        anchor = self.current.anchor
        if anchor is not None and write > anchor.sequence_frontier:
            continuous = self.current.intent is None and abs(offset - anchor.offset_seconds) <= tolerance
            if write > anchor.sequence_frontier:
                self.current = self.current.add_range(
                    ClockRange(
                        anchor.sequence_frontier,
                        write,
                        anchor.offset_seconds if continuous else None,
                        anchor.uncertainty_seconds + abs(offset - anchor.offset_seconds) if continuous else None,
                        "approximate" if continuous else "unknown",
                        anchor.source_id,
                    )
                )
        elif anchor is None and read < write:
            self.current = self.current.add_range(ClockRange(read, write, None, None, "unknown", source_id))
        self.current = ClockState(
            self.current.ranges,
            self.current.intent,
            ClockAnchor(write, offset, uncertainty, "approximate", source_id),
        )
        return self.current

    def prepare(self, intent: ClockIntent) -> ClockState:
        self.calls.append("prepare")
        self._fail_if("prepare")
        self.current = self.current.begin_intent(intent)
        return self.current

    def mark_uncertain(self, operation_id: str, *, write_sequence_after: int | None = None) -> ClockState:
        self.calls.append("mark_uncertain")
        self._fail_if("mark_uncertain")
        self.current = self.current.mark_uncertain(operation_id, write_sequence_after=write_sequence_after)
        return self.current

    def cancel_prepared(self, operation_id: str) -> ClockState:
        self.calls.append("cancel_prepared")
        self._fail_if("cancel_prepared")
        self.current = self.current.cancel_prepared_intent(operation_id)
        return self.current

    def finish(self, operation_id: str, ranges: tuple[ClockRange, ...] = ()) -> ClockState:
        self.calls.append("finish")
        self._fail_if("finish")
        self.current = self.current.finish_intent(operation_id, ranges)
        return self.current

    def _fail_if(self, operation: str) -> None:
        if self.fail_on == operation:
            raise OSError(f"{operation} failed")


def _event_emitter(events: list[dict[str, object]]) -> OperationalEmitter:
    def emit(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    return emit


def _clock_store(path: Path) -> ClockStateStore:
    return ClockStateStore(StateDB(path))


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
        json.dumps(
            {
                "schema_version": 2,
                "start_sequence": start_sequence,
                "next_sequence": start_sequence + 1,
                "record_count": 1,
                "record_size": RECORD_SIZE,
                "raw_sha256": digest,
            }
        ),
        encoding="utf-8",
    )
    (bundle / "receipt.json").write_text(
        json.dumps({"attempt_id": "a" * 32, "raw_sha256": digest, "status": "sealed"}),
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
                lambda _timeout: synchronized,
                operation_timeout,
                state_store=FakeClockStateStore(),
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
    store = _clock_store(tmp_path / "collector-state.sqlite3")
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            None,
            clock=TelemetryClock(
                now=lambda: next(ticks),
                synchronized=lambda _timeout: True,
                state_store=store,
            ),
        )
    )

    state = store.state()
    assert state.anchor is not None
    assert state.anchor.sequence_frontier == _info().write_sequence
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
                synchronized=lambda _timeout: False,
                status_reader=read_status,
            ),
        )
    )

    assert status_reads == 0
    assert _observation_event(events)["used_bytes"] == 123


def test_synchronization_probe_error_skips_time_write_and_durable_evidence() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = FakeClockStateStore()
    ticks = iter((1000.0,) * 8)

    def fail_sync_check(_timeout: float) -> bool:
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
                state_store=store,
            ),
        )
    )

    clock_event = _clock_event(events)
    assert clock_event["outcome"] == "host_unsynchronized"
    assert clock_event["host_ntp_synchronized"] is False
    assert session.writes == []
    assert store.current == ClockState()
    assert store.calls == []
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
    sink = FakeClockStateStore()

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                now=lambda: host_time,
                synchronized=lambda _timeout: True,
                state_store=sink,
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
                synchronized=lambda _timeout: True,
                state_store=FakeClockStateStore(),
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
                synchronized=lambda _timeout: True,
                state_store=FakeClockStateStore(),
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
                synchronized=lambda _timeout: True,
                state_store=FakeClockStateStore(),
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


def test_battery_callback_receives_only_validated_sample() -> None:
    samples: list[int] = []

    async def read(payload: bytes) -> None:
        session = FakeOperationalSession({BATTERY_UUID: payload})
        await collect_battery_observation(
            session,
            _info(),
            _event_emitter([]),
            operation_timeout=0.1,
            observe_battery=samples.append,
        )

    asyncio.run(read(b"\x2a"))
    asyncio.run(read(b"\x65"))

    assert samples == [42]


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
                clock=TelemetryClock(now=lambda: 1000.0, synchronized=lambda _timeout: False),
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


def test_repeated_healthy_no_audio_visit_does_not_add_clock_history(tmp_path: Path) -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1000)})
    path = tmp_path / "collector-state.sqlite3"
    store = _clock_store(path)
    info = RingInfo(12, 12, 100, 0, RECORD_SIZE)

    def observe() -> None:
        ticks = iter((1000.0,) * 8)
        asyncio.run(
            collect_operational_telemetry(
                session,
                _status(),
                info,
                _event_emitter([]),
                clock=TelemetryClock(
                    lambda: next(ticks),
                    lambda _timeout: True,
                    0.5,
                    state_store=store,
                    session_id="empty-visit",
                ),
            )
        )

    observe()
    first_state = store.state()
    first_database = path.read_bytes()
    observe()

    assert first_state.anchor is not None
    assert store.state() == first_state
    assert path.read_bytes() == first_database
    assert len(store.db.list(StateKind.CLOCK)) == 1
    assert session.writes == []


def test_draft_clock_mapping_survives_later_correction_across_visits(tmp_path: Path) -> None:
    path = tmp_path / "collector-state.sqlite3"
    store = _clock_store(path)
    first_session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1005)})
    first_ticks = iter((1000.0,) * 8)
    asyncio.run(
        collect_operational_telemetry(
            first_session,
            _status(),
            RingInfo(0, 10, 100, 0, RECORD_SIZE),
            _event_emitter([]),
            clock=TelemetryClock(lambda: next(first_ticks), lambda _timeout: True, 0.5, state_store=store),
        )
    )
    assert store.segment_map().utc_for(5, 1005) is None

    later_session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1008)})
    later_ticks = iter((1003.0,) * 8)
    asyncio.run(
        collect_operational_telemetry(
            later_session,
            _status(),
            RingInfo(10, 12, 100, 0, RECORD_SIZE),
            _event_emitter([]),
            clock=TelemetryClock(lambda: next(later_ticks), lambda _timeout: True, 0.5, state_store=store),
        )
    )

    reopened = _clock_store(path)
    assert reopened.segment_map().utc_for(11, 1008) == pytest.approx(1003.0)
    assert reopened.segment_map().utc_for(5, 1005) is None
    assert later_session.writes == []

    correction_session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1003),
    )
    correction_ticks = iter((1003.0,) * 8)
    correction_events: list[dict[str, object]] = []

    async def info_after_correction() -> RingInfo:
        return RingInfo(14, 16, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            correction_session,
            _status(),
            RingInfo(12, 14, 100, 0, RECORD_SIZE),
            _event_emitter(correction_events),
            clock=TelemetryClock(
                lambda: next(correction_ticks),
                lambda _timeout: True,
                0.5,
                info_reader=info_after_correction,
                state_store=reopened,
                session_id="correction-visit",
            ),
        )
    )

    after_correction = _clock_store(path)
    assert after_correction.segment_map().utc_for(11, 1008) == pytest.approx(1003.0)
    assert after_correction.segment_map().utc_for(15, 1003) is None
    assert _clock_event(correction_events)["outcome"] == "verified"
    assert correction_session.writes == [(TIME_WRITE_UUID, pack("<I", 1003))]


def test_untrusted_host_does_not_write_clock_state_or_set_time() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1005)})
    events: list[dict[str, object]] = []
    store = FakeClockStateStore()
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(lambda: next(ticks), lambda _timeout: False, 0.5, state_store=store),
        )
    )

    assert store.calls == []
    assert session.writes == []
    assert _clock_event(events)["outcome"] == "host_unsynchronized"


def test_verified_set_time_persists_intent_before_write_and_keeps_exact_boundary(tmp_path: Path) -> None:
    store = _clock_store(tmp_path / "collector-state.sqlite3")
    store.observe(
        RingInfo(0, 12, 100, 0, RECORD_SIZE),
        offset_seconds=-5,
        uncertainty_seconds=1,
        source_id="prior-visit",
        tolerance_seconds=5,
    )
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)}, readback=pack("<I", 1000)
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)
    observed_intents: list[ClockIntent | None] = []

    class InspectingSession(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> object:
            observed_intents.append((await asyncio.to_thread(store.state)).intent)
            return await super().write_optional_characteristic(uuid, value)

    session = InspectingSession(session.values, readback=session.readback)

    async def info_after() -> RingInfo:
        return RingInfo(12, 12, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(0, 12, 100, 0, RECORD_SIZE),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda _timeout: True,
                0.5,
                info_reader=info_after,
                state_store=store,
                session_id="verified-visit",
            ),
        )
    )

    assert len(observed_intents) == 1
    assert observed_intents[0] is not None and observed_intents[0].phase == "prepared"
    assert store.state().intent is None
    assert store.state().ranges == (ClockRange(0, 12, None, None, "unknown", "prior-visit"),)
    assert _clock_event(events)["outcome"] == "verified"
    assert session.writes == [(TIME_WRITE_UUID, pack("<I", 1000))]


def test_verified_set_time_marks_only_unlocated_write_window_unknown(tmp_path: Path) -> None:
    store = _clock_store(tmp_path / "collector-state.sqlite3")
    store.observe(
        RingInfo(0, 12, 100, 0, RECORD_SIZE),
        offset_seconds=-5,
        uncertainty_seconds=1,
        source_id="prior-visit",
        tolerance_seconds=5,
    )
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)}, readback=pack("<I", 1000)
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        return RingInfo(12, 14, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(0, 12, 100, 0, RECORD_SIZE),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks), lambda _timeout: True, 0.5, info_reader=info_after, state_store=store
            ),
        )
    )

    assert store.state().intent is None
    assert store.state().ranges[0] == ClockRange(0, 12, None, None, "unknown", "prior-visit")
    assert store.state().ranges[1].start_sequence == 12
    assert store.state().ranges[1].next_sequence == 14
    assert store.state().ranges[1].confidence == "unknown"
    assert store.segment_map().utc_for(13, 1000) is None
    assert _clock_event(events)["boundary_ambiguous"] is True


def test_write_failure_retains_uncertain_intent_for_next_trusted_visit(tmp_path: Path) -> None:
    path = tmp_path / "collector-state.sqlite3"
    store = _clock_store(path)

    class FailingWriteSession(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> object:
            self.writes.append((uuid, value))
            raise OSError("write may have reached device")

    failed_session = FailingWriteSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    failure_events: list[dict[str, object]] = []
    failure_ticks = iter((1000.0,) * 8)

    async def info_after_write() -> RingInfo:
        return RingInfo(12, 14, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            failed_session,
            _status(),
            RingInfo(0, 12, 100, 0, RECORD_SIZE),
            _event_emitter(failure_events),
            clock=TelemetryClock(
                lambda: next(failure_ticks), lambda _timeout: True, 0.5, info_reader=info_after_write, state_store=store
            ),
        )
    )

    pending = store.state().intent
    assert pending is not None and pending.phase == "uncertain"
    assert _clock_event(failure_events)["outcome"] == "time_write_failed"

    next_session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1015)})
    next_events: list[dict[str, object]] = []
    next_ticks = iter((1015.0,) * 8)
    asyncio.run(
        collect_operational_telemetry(
            next_session,
            _status(),
            RingInfo(12, 16, 100, 0, RECORD_SIZE),
            _event_emitter(next_events),
            clock=TelemetryClock(lambda: next(next_ticks), lambda _timeout: True, 0.5, state_store=_clock_store(path)),
        )
    )

    recovered = _clock_store(path)
    assert recovered.state().intent is None
    assert recovered.segment_map().utc_for(13, 1015) is None
    assert _clock_event(next_events)["reconciled_operations"] == 1


def test_failed_intent_commit_prevents_ble_set_time() -> None:
    session = FakeOperationalSession({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = FakeClockStateStore()
    store.fail_on = "prepare"
    ticks = iter((1000.0,) * 8)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(lambda: next(ticks), lambda _timeout: True, 0.5, state_store=store),
        )
    )

    assert session.writes == []
    assert _clock_event(events)["outcome"] == "intent_persist_failed"


@pytest.mark.parametrize("failing_operation", ("state", "observe", "prepare"))
def test_corrupt_clock_evidence_stops_preflight_before_audio_or_time_write(failing_operation: str) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)

    class CorruptClockStore(FakeClockStateStore):
        def state(self) -> ClockState:
            if failing_operation == "state":
                raise fatal
            return super().state()

        def _fail_if(self, operation: str) -> None:
            if operation == failing_operation:
                raise fatal

    session = FakeOperationalSession({TIME_READ_UUID: pack("<I", 1010)})
    store = CorruptClockStore()
    ticks = iter((1000.0,) * 8)
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        asyncio.run(
            collect_operational_telemetry(
                session,
                _status(),
                _info(),
                _event_emitter([]),
                clock=TelemetryClock(lambda: next(ticks), lambda _timeout: True, 0.5, state_store=store),
            )
        )
    assert raised.value is fatal
    assert session.writes == []


def test_clock_fatal_survives_repeated_cancellation_during_owned_lease_exit(tmp_path: Path) -> None:
    state_db = StateDB(tmp_path / "corrupt-state.sqlite3")
    state_db.path.write_bytes(b"not a SQLite database")
    observed_fatals: list[FatalInternalAudioStorageError] = []
    original_causes: list[BaseException | None] = []
    exit_started = threading.Event()
    release_exit = threading.Event()
    exit_completed = threading.Event()

    class CorruptClockStore(ClockStateStore):
        def state(self) -> ClockState:
            try:
                return super().state()
            except FatalInternalAudioStorageError as error:
                observed_fatals.append(error)
                original_causes.append(error.__cause__)
                raise

    class HeldLease:
        def __enter__(self) -> HeldLease:
            return self

        def require_active(self) -> None:
            return None

        def __exit__(self, _type: object, value: object, _traceback: object) -> None:
            assert value is observed_fatals[0]
            exit_started.set()
            if not release_exit.wait(5):
                raise TimeoutError("held clock lease exit was not released")
            exit_completed.set()

    session = FakeOperationalSession({TIME_READ_UUID: pack("<I", 1010)})

    async def scenario() -> None:
        task = asyncio.create_task(
            collect_operational_telemetry(
                session,
                _status(),
                _info(),
                _event_emitter([]),
                clock=TelemetryClock(
                    now=lambda: 1000.0,
                    synchronized=lambda _timeout: True,
                    state_store=CorruptClockStore(state_db),
                    mutation_lease=HeldLease,
                ),
            )
        )
        try:
            assert await asyncio.to_thread(exit_started.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert not exit_completed.is_set()
            task.cancel()
            await asyncio.sleep(0)
            done, _pending = await asyncio.wait({task}, timeout=0.05)
            assert not done
            assert not exit_completed.is_set()
        finally:
            release_exit.set()
        with pytest.raises(FatalInternalAudioStorageError) as raised:
            await task
        assert raised.value is observed_fatals[0]
        assert raised.value.__cause__ is original_causes[0]
        assert isinstance(original_causes[0], sqlite3.DatabaseError)
        assert raised.value.sqlite_errorcode == sqlite3.SQLITE_NOTADB
        assert exit_completed.is_set()

    asyncio.run(scenario())
    assert session.writes == []


def test_clock_target_becomes_stale_while_monotonic_preparation_waits() -> None:
    monotonic_started = threading.Event()
    release_monotonic = threading.Event()
    wall_now = [1000.0]
    event: dict[str, object] = {}
    session = FakeOperationalSession({})

    def held_monotonic() -> float:
        monotonic_started.set()
        if not release_monotonic.wait(5):
            raise TimeoutError("held monotonic preparation was not released")
        return 1.0

    async def scenario() -> operational_telemetry._ClockReadback:
        task = asyncio.create_task(
            operational_telemetry._write_and_verify(
                operational_telemetry._ClockWrite(
                    session.read_optional_characteristic,
                    session.write_optional_characteristic,
                    1000,
                    event,
                    1.0,
                    lambda: wall_now[0],
                    held_monotonic,
                    None,
                )
            )
        )
        try:
            assert await asyncio.to_thread(monotonic_started.wait, 5)
            wall_now[0] = 1003.0
        finally:
            release_monotonic.set()
        return await task

    readback = asyncio.run(scenario())
    assert readback.epoch is None
    assert event["outcome"] == "target_stale"
    assert session.writes == []


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


def test_verification_failure_keeps_write_window_unknown(tmp_path: Path) -> None:
    store = _clock_store(tmp_path / "collector-state.sqlite3")
    session = FakeOperationalSession(
        {BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)},
        readback=pack("<I", 1005),
    )
    events: list[dict[str, object]] = []
    ticks = iter((1000.0,) * 8)

    async def info_after() -> RingInfo:
        return RingInfo(12, 14, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            RingInfo(0, 12, 100, 0, RECORD_SIZE),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda _timeout: True,
                0.5,
                info_reader=info_after,
                state_store=store,
            ),
        )
    )

    pending = store.state().intent
    assert pending is not None and pending.phase == "uncertain"
    assert pending.write_sequence_after == 14
    assert _clock_event(events)["outcome"] == "verification_failed"
    assert store.segment_map().utc_for(13, 1005) is None


def test_missing_time_write_cancels_prepared_intent_without_post_write_read() -> None:
    class MissingWriter(FakeOperationalSession):
        async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
            self.writes.append((uuid, value))
            return False

    session = MissingWriter({BATTERY_UUID: bytes((80,)), TIME_READ_UUID: pack("<I", 1010)})
    events: list[dict[str, object]] = []
    store = FakeClockStateStore()
    ticks = iter((1000.0,) * 8)
    info_reads = 0

    async def info_after() -> RingInfo:
        nonlocal info_reads
        info_reads += 1
        return RingInfo(12, 14, 100, 0, RECORD_SIZE)

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                lambda: next(ticks),
                lambda _timeout: True,
                0.5,
                info_reader=info_after,
                state_store=store,
            ),
        )
    )

    assert _clock_event(events)["outcome"] == "time_write_missing"
    assert info_reads == 0
    assert store.state().intent is None
    assert store.calls[-1] == "cancel_prepared"


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
    observed_timeouts: list[float] = []

    def timeout_aware_probe(timeout: float) -> bool:
        observed_timeouts.append(timeout)
        time.sleep(timeout)
        raise TimeoutError

    asyncio.run(
        collect_operational_telemetry(
            session,
            _status(),
            _info(),
            _event_emitter(events),
            clock=TelemetryClock(
                synchronized=timeout_aware_probe,
                operation_timeout=0.1,
                host_clock_probe_timeout=0.02,
            ),
        )
    )

    assert observed_timeouts == [0.02]
    assert _clock_event(events)["outcome"] == "host_probe_timeout"


def test_cancelled_host_clock_probe_joins_its_worker() -> None:
    async def run() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def bounded_probe(timeout: float) -> bool:
            started.set()
            if not release.wait(timeout):
                raise TimeoutError
            finished.set()
            return True

        task = asyncio.create_task(operational_telemetry._run_host_clock_probe(bounded_probe, 1.0))
        assert await asyncio.to_thread(started.wait, 1.0)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(run())


def _blocked_trust_probe_child(result: Connection) -> None:
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
                synchronized=lambda timeout: _raise_probe_timeout(timeout),
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


def _raise_probe_timeout(timeout: float) -> bool:
    time.sleep(timeout)
    raise TimeoutError


def test_slow_host_probe_finishes_clock_stage_before_metadata() -> None:
    trace: list[str] = []

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
    store = FakeClockStateStore()

    def delayed_host_probe(_timeout: float) -> bool:
        time.sleep(0.05)
        return True

    async def later_info() -> RingInfo:
        await asyncio.sleep(0.01)
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
                state_store=store,
            ),
        )
    )

    assert store.state().intent is None
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
            StagingStore(tmp_path, _capture_root(tmp_path), state_db=StateDB(tmp_path / "collector-state.sqlite3")),
            OpportunisticOptions(
                TransferTimeouts(
                    DEFAULT_CONFIG.transfer.info_timeout_seconds,
                    DEFAULT_CONFIG.transfer.sync_timeout_seconds,
                ),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=_event_emitter(events),
                host_clock_synchronized=lambda _timeout: False,
            ),
            runtime=OpportunisticRuntime(),
        )
    )

    assert session.writes == [b"\x10"]
    observation = _observation_event(events)
    assert observation["read_sequence"] == 10
    assert observation["write_sequence"] == 10
