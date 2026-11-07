"""Bounded runtime-effect contracts using isolated SQLite databases.

The generated clock model tracks only committed intent phases. A failure inside
an open transaction is a rollback, not a simulated power loss. Each example
owns a fresh database; the successful suffix witnesses conditional progress.
"""

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from unittest.mock import patch

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st

from omi_collector.capture.adapters.ble_scan_health_store import BleScanIncidentStore, ScanDiagnosticsFacade
from omi_collector.capture.adapters.clock_state_store import ClockStateStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateTransaction
from omi_collector.capture.adapters.public_time_facts import FrozenTimePredecessor, PublicTimeProjection
from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor, ScanIncidentContext
from omi_collector.capture.domain.clock_state import ClockIntent, ClockRange
from test_fsm_scan_model import Diagnostics


def _assert_clock_boundary(store: ClockStateStore, start: int, end: int) -> None:
    projection = PublicTimeProjection.from_state(store.state())
    for sequence in range(start, end + 1):
        previous = projection.freeze(sequence=sequence - 1, raw_timestamp=100)
        after = projection.freeze(
            sequence=sequence,
            raw_timestamp=100,
            predecessor=FrozenTimePredecessor(sequence - 1, previous),
        )
        assert after.comparable_to_previous is False


def _advance_clock(
    store: ClockStateStore,
    phase: str,
    end: int,
    fail: bool,
) -> tuple[str, str]:
    db = store.db
    transaction = db.transaction

    @contextmanager
    def interrupted() -> Iterator[StateTransaction]:
        with transaction() as tx:
            yield tx
            raise OSError("injected before transaction commit")

    def write() -> None:
        if phase == "empty":
            store.prepare(ClockIntent("write", "prepared", 0, 0, 0.0, 3))
        elif phase == "prepared":
            store.mark_uncertain("write", write_sequence_after=end)
        elif phase == "uncertain":
            ranges = (ClockRange(3, end, None, None, "unknown", "write"),) if end > 3 else ()
            store.finish("write", ranges)

    if phase == "completed":
        return phase, ""
    if fail:
        with patch.object(db, "transaction", interrupted), pytest.raises(OSError, match="injected"):
            write()
        return phase, f"{phase}:rollback"
    write()
    return {"empty": "prepared", "prepared": "uncertain", "uncertain": "completed"}[phase], f"{phase}:commit"


def _clock_scenario(actions: list[str], length: int) -> set[str]:
    with TemporaryDirectory(prefix="omi-effect-clock-") as temporary:
        path = Path(temporary) / "state.sqlite3"
        db = StateDB(path)
        store = ClockStateStore(db)
        phase = "empty"
        reached: set[str] = set()
        start, end = 3, 3 + length

        for action in [*actions, "commit", "commit", "commit"]:
            if action == "reload":
                store = ClockStateStore(StateDB(path))
                db = store.db
                reached.add(f"{phase}:reload")
            else:
                phase, transition = _advance_clock(store, phase, end, action == "rollback")
                if transition:
                    reached.add(transition)
            current = ClockStateStore(StateDB(path)).state()
            actual = (
                current.intent.phase if current.intent else ("completed" if current.causal_uncertainty else "empty")
            )
            assert actual == phase
            if phase == "completed":
                _assert_clock_boundary(ClockStateStore(StateDB(path)), start, end)
        assert phase == "completed"
        return reached


@settings(max_examples=20, deadline=None)
@given(actions=st.lists(st.sampled_from(("commit", "rollback", "reload")), max_size=6), length=st.integers(0, 3))
def test_generated_clock_transaction_recovery(actions: list[str], length: int) -> None:
    for transition in _clock_scenario(actions, length):
        event(transition)


def test_clock_effect_transition_witnesses() -> None:
    reached = _clock_scenario(["rollback", "commit", "rollback", "commit", "rollback", "reload"], 0)
    assert {
        "empty:rollback",
        "empty:commit",
        "prepared:rollback",
        "prepared:commit",
        "uncertain:rollback",
        "uncertain:commit",
        "uncertain:reload",
    } <= reached


def _scan_scenario(actions: list[str]) -> set[str]:
    import asyncio
    from typing import cast

    from omi_collector.capture.adapters.ble_scan_health_store import BleScanIncidentStore, ScanDiagnosticsFacade
    from omi_collector.capture.adapters.collector_state_db import StateKind
    from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor
    from test_fsm_scan_model import Diagnostics

    async def scenario(path: Path) -> set[str]:
        clock = [1.0]
        diagnostics = Diagnostics()
        sink = BleScanIncidentStore(await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics))
        monitor = ScanHealthMonitor(sink, diagnostics, 1000.0, clock=lambda: clock[0])
        await monitor.prepare(1)
        monitor.scan_started(1)
        clock[0] += 1000
        assert await monitor.check_deadline()
        active = True
        reached = {"closed:failure:open"}
        for action in [*actions, "later"]:
            before = active
            clock[0] += 10
            if action == "restart":
                sink = BleScanIncidentStore(
                    await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics)
                )
                monitor = ScanHealthMonitor(sink, diagnostics, 1000.0, clock=lambda: clock[0])
                await monitor.prepare(1)
                monitor.scan_started(1)
            elif action == "failure":
                await monitor.prepare(1)
                monitor.scan_started(1)
                clock[0] += 1000
                assert await monitor.check_deadline()
                active = True
            else:
                failure = clock[0] - 10
                monitor.radio_activity(2 if action == "foreign" else 1, clock[0] if action == "later" else failure)
                await monitor.flush_recovery()
                if action == "later":
                    active = False
            actual = await sink.active()
            assert actual == active
            reached.add(f"{before}:{action}:{actual}")
        assert not active
        history = await asyncio.to_thread(sink._state_db.get, StateKind.BLE_SCAN_INCIDENT, "history")
        assert history is not None
        return reached

    with TemporaryDirectory(prefix="omi-effect-scan-") as temporary:
        return asyncio.run(scenario(Path(temporary) / "state.sqlite3"))


@settings(max_examples=20, deadline=None)
@given(st.lists(st.sampled_from(("failure", "later", "foreign", "restart")), max_size=6))
def test_generated_scan_incident_recovery(actions: list[str]) -> None:
    for transition in _scan_scenario(actions):
        event(transition)


def test_scan_effect_transition_witnesses() -> None:
    reached = _scan_scenario(["foreign", "restart", "failure", "later", "failure"])
    assert {
        "closed:failure:open",
        "True:foreign:True",
        "True:restart:True",
        "True:later:False",
        "False:failure:True",
    } <= reached


def test_cancelled_incident_resolution_joins_committed_write() -> None:
    import asyncio
    import threading
    from typing import cast

    from omi_collector.capture.adapters.ble_scan_health_store import BleScanIncidentStore, ScanDiagnosticsFacade
    from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor
    from test_fsm_scan_model import Diagnostics

    async def scenario(path: Path) -> None:
        diagnostics = Diagnostics()
        sink = BleScanIncidentStore(await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics))
        clock = [1.0]
        monitor = ScanHealthMonitor(sink, diagnostics, 1000.0, clock=lambda: clock[0])
        await monitor.prepare(1)
        monitor.scan_started(1)
        clock[0] = 1001.0
        assert await monitor.check_deadline()
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        resolve = sink._resolve

        def blocked(generation: int, reason: str) -> bool:
            started.set()
            if not release.wait(30):
                raise TimeoutError("resolution was not released")
            result = resolve(generation, reason)
            finished.set()
            return result

        with patch.object(sink, "_resolve", blocked):
            monitor.radio_activity(1, 1002.0)
            task = asyncio.create_task(monitor.flush_recovery())
            try:
                assert await asyncio.to_thread(started.wait, 30)
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert finished.is_set()
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
        reopened = BleScanIncidentStore(
            await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics)
        )
        assert not await reopened.active()

    with TemporaryDirectory(prefix="omi-effect-cancel-") as temporary:
        asyncio.run(scenario(Path(temporary) / "state.sqlite3"))


def _publication_scenario(boundary: str, after: bool, restarts: int) -> set[str]:
    import json
    from typing import cast

    from omi_collector.capture.adapters.collector_state_db import StateKind
    from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
    from omi_collector.capture.adapters.ready_bundles import (
        ReadyBundleError,
        SqlitePublicationSource,
        finalize_public_audio_v2,
        retire_acknowledged_public_audio_v2,
        retire_one_covered_typed_source,
    )
    from test_public_audio_v2_recovery import (
        _checkpoint,
        _install_materialization_failure,
        _options,
        _ready_root,
        _set_acknowledged,
        _source,
    )

    with TemporaryDirectory(prefix="omi-effect-publication-") as temporary:
        root = Path(temporary)
        path = root / "state.sqlite3"
        db = StateDB(path)
        ready = _ready_root(root)
        checkpoint = _checkpoint(root)
        with InternalAudioStore(db) as store:
            source = _source(store, "a" * 32, 10)
            with pytest.MonkeyPatch.context() as patcher:
                fired = _install_materialization_failure(patcher, ready, boundary, after)
                with pytest.raises(RuntimeError, match="simulated non-durable"):
                    finalize_public_audio_v2(store, (source,), ready, _options())
                assert fired()
        for _ in range(restarts):
            with InternalAudioStore(StateDB(path)) as reopened:
                assert reopened.get_source("a" * 32) is not None
                assert len(reopened.read_record_page("a" * 32, -1, 2)) == 1
        with InternalAudioStore(StateDB(path)) as store:
            source = SqlitePublicationSource(store, "a" * 32)
            artifact = finalize_public_audio_v2(store, (source,), ready, _options()).published[0]
            manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
            digest = cast(str, manifest["audio_sha256"])
            _set_acknowledged(checkpoint, ((artifact.bundle_id, "0" * 64),))
            with pytest.raises(ReadyBundleError):
                retire_acknowledged_public_audio_v2(ready, db, checkpoint)
            assert artifact.path.is_dir()
            assert store.get_source("a" * 32) is not None
            _set_acknowledged(checkpoint, ((artifact.bundle_id, digest),))
            assert retire_acknowledged_public_audio_v2(ready, db, checkpoint)
            assert not artifact.path.exists()
            for _ in range(5):
                if store.get_source("a" * 32) is None:
                    break
                assert retire_one_covered_typed_source(store, 1)
            assert store.get_source("a" * 32) is None
            debt = StateDB(path).get(StateKind.COMPACTION, "current")
            assert debt is not None and debt["pending"] is True
        return {
            f"{boundary}:{'after' if after else 'before'}",
            "restart:published",
            "bad-ack:preserved",
            "ack:retired",
            "covered:reclaimed",
            "debt:pending",
        }


@settings(max_examples=20, deadline=None)
@given(
    boundary=st.sampled_from(("write", "file_sync", "manifest", "directory_sync", "rename")),
    after=st.booleans(),
    restarts=st.integers(0, 6),
)
def test_generated_publication_effect_recovery(boundary: str, after: bool, restarts: int) -> None:
    for transition in _publication_scenario(boundary, after, restarts):
        event(transition)


@pytest.mark.parametrize("boundary", ("write", "file_sync", "manifest", "directory_sync", "rename"))
@pytest.mark.parametrize("after", (False, True))
def test_publication_effect_transition_witnesses(boundary: str, after: bool) -> None:
    reached = _publication_scenario(boundary, after, 1)
    assert {
        f"{boundary}:{'after' if after else 'before'}",
        "restart:published",
        "bad-ack:preserved",
        "ack:retired",
        "covered:reclaimed",
        "debt:pending",
    } <= reached


@settings(max_examples=20, deadline=None)
@given(fail_open=st.booleans(), fail_resolve=st.booleans(), extra_reloads=st.integers(0, 4))
def test_generated_scan_transient_store_recovery(fail_open: bool, fail_resolve: bool, extra_reloads: int) -> None:
    async def scenario(path: Path) -> None:
        clock = [1.0]
        diagnostics = Diagnostics()
        sink = BleScanIncidentStore(await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics))
        monitor = ScanHealthMonitor(sink, diagnostics, 1000.0, clock=lambda: clock[0])
        open_write, resolve_write = sink._open, sink._resolve
        open_attempts = 0
        resolve_attempts = 0

        def open_once(context: ScanIncidentContext, snapshot: dict[str, object], detected_at: str) -> bool:
            nonlocal open_attempts
            open_attempts += 1
            if fail_open and open_attempts == 1:
                raise StateDBError("injected open write failure")
            return open_write(context, snapshot, detected_at)

        def resolve_once(generation: int, reason: str) -> bool:
            nonlocal resolve_attempts
            resolve_attempts += 1
            if fail_resolve and resolve_attempts == 1:
                raise StateDBError("injected resolve write failure")
            return resolve_write(generation, reason)

        with patch.object(sink, "_open", open_once), patch.object(sink, "_resolve", resolve_once):
            await monitor.prepare(1)
            monitor.scan_started(1)
            clock[0] = 1001.0
            assert await monitor.check_deadline()
            assert await sink.active() is not fail_open
            if fail_open:
                assert monitor.next_deadline == 1002.0
                assert not await monitor.check_deadline(1001.5)
                assert open_attempts == 1
                clock[0] = 1002.0
                assert await monitor.check_deadline()
            assert await sink.active()
            for _ in range(extra_reloads):
                reopened = BleScanIncidentStore(
                    await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics)
                )
                assert await reopened.active()
            clock[0] += 1.0
            monitor.radio_activity(1, clock[0])
            await monitor.flush_recovery()
            assert await sink.active() is fail_resolve
            if fail_resolve:
                assert monitor.next_deadline == clock[0] + 1.0
                assert not await monitor.check_deadline(clock[0] + 0.5)
                assert resolve_attempts == 1
                clock[0] += 1.0
                assert await monitor.check_deadline()
            reopened = BleScanIncidentStore(
                await asyncio.to_thread(StateDB, path), cast(ScanDiagnosticsFacade, diagnostics)
            )
            assert not await reopened.active()
            assert open_attempts == 1 + fail_open
            assert resolve_attempts == 1 + fail_resolve
            event(f"open-failure={fail_open},resolve-failure={fail_resolve}")

    with TemporaryDirectory(prefix="omi-effect-retry-") as temporary:
        asyncio.run(scenario(Path(temporary) / "state.sqlite3"))
