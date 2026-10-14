from __future__ import annotations

import asyncio
import os
import select
import signal
import subprocess
import sys
from pathlib import Path

from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.confirmed_loss import read_confirmed_losses
from omi_collector.capture.application.batch_reconciliation import BatchReconciler
from omi_collector.capture.application.collector import CollectionResult
from omi_collector.capture.application.session_lifecycle import SessionPhaseState
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo, RingStatus
from staging_test_factory import StagingStore, attempt_records, attempt_source
from test_batch_reconciliation import (
    _close_real_writers,
    _ignore_quarantine,
    _options,
    _record,
    _Runtime,
    _seed_partial,
    _wire_begin,
    _wire_done,
    _wire_record,
)


def _kill_during_uncommitted_drain(store: StagingStore, source_id: str, raw: bytes) -> None:
    ready_read, ready_write = os.pipe()
    gate_read, gate_write = os.pipe()
    code = """
import hashlib, os, sys
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import DrainCheckpoint, InternalAudioStore

db = StateDB(sys.argv[1])
raw = bytes.fromhex(sys.argv[2])
ready_fd = int(sys.argv[3])
gate_fd = int(sys.argv[4])
with InternalAudioStore(db) as store:
    original_commit = store._commit
    def held_commit() -> None:
        os.write(ready_fd, b"ready")
        os.read(gate_fd, 1)
        original_commit()
    store._commit = held_commit
    store.commit_drain(
        sys.argv[5],
        1,
        [raw],
        DrainCheckpoint(1, expected_sha256=hashlib.sha256(bytes.fromhex(sys.argv[6])).hexdigest()),
    )
"""
    first = _record(100)
    expected = first + raw
    process = subprocess.Popen(
        (
            sys.executable,
            "-c",
            code,
            str(store.state_db.path),
            raw.hex(),
            str(ready_write),
            str(gate_read),
            source_id,
            expected.hex(),
        ),
        pass_fds=(ready_write, gate_read),
    )
    os.close(ready_write)
    os.close(gate_read)
    try:
        ready, _, _ = select.select((ready_read,), (), (), 10)
        if not ready:
            raise AssertionError(f"force-exit child did not reach commit barrier: {process.poll()=}")
        assert os.read(ready_read, len(b"ready")) == b"ready"
        process.send_signal(signal.SIGKILL)
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        os.close(ready_read)
        os.close(gate_write)


def test_force_exit_before_commit_reconciles_retained_cursor_exactly_once(tmp_path: Path) -> None:
    store, _source, records = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    second = _record(101)
    _kill_during_uncommitted_drain(store, descriptor.attempt_id, second)
    assert attempt_records(store, descriptor.attempt_id) == records

    runtime = _Runtime()
    reconciler = BatchReconciler(store, _options(), runtime, _ignore_quarantine)
    reconciler.set_startup_state(descriptor, 101)
    current = RingInfo(101, 102, 100, 0, RECORD_SIZE)
    confirmed = RingInfo(102, 102, 100, 0, RECORD_SIZE)

    class CheckingSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            if payload.startswith(b"\x12"):
                assert await asyncio.to_thread(attempt_records, store, descriptor.attempt_id) == (
                    records[0],
                    second,
                )
                source = await asyncio.to_thread(attempt_source, store, descriptor.attempt_id)
                assert source is not None
                assert source.sequence_start + source.committed_count == 102
                assert source.committed_count == 2
            await super().write_control(payload)

    session = CheckingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(
                b"\x11" + (101).to_bytes(8, "big") + (1).to_bytes(4, "big"),
                (_wire_begin(101, 1), _wire_record(101), _wire_done(102)),
            ),
            WriteStep(b"\x12" + (102).to_bytes(8, "big"), (b"\x01\x00",)),
        ),
    )
    info_values = iter((current, confirmed))

    async def info(_session: object) -> RingInfo:
        return next(info_values)

    async def run() -> None:
        try:
            disposition, seen = await reconciler.connected_step(
                session, current, info, SessionPhaseState("read/reconcile")
            )
            assert disposition is None
            assert seen == confirmed
            assert await asyncio.to_thread(attempt_records, store, descriptor.attempt_id) == (records[0], second)
            assert sum(command[0] == 0x12 for command in session.writes) == 1
        finally:
            await _close_real_writers(runtime)
            await session.close()

    asyncio.run(run())


def test_force_exit_before_commit_records_cursor_ahead_loss_without_advance(tmp_path: Path) -> None:
    store, _source, records = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    _kill_during_uncommitted_drain(store, descriptor.attempt_id, _record(101))
    assert attempt_records(store, descriptor.attempt_id) == records
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    runtime = _Runtime()
    reconciler = BatchReconciler(store, _options(), runtime, _ignore_quarantine)
    reconciler.set_startup_state(descriptor, 101)
    current = RingInfo(103, 103, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1))

    async def info(_session: object) -> RingInfo:
        return current

    async def run() -> None:
        try:
            disposition, _ = await reconciler.connected_step(
                session, current, info, SessionPhaseState("read/reconcile")
            )
            assert disposition == "drained"
            assert all(command[0] != 0x12 for command in session.writes)
            source = attempt_source(store, descriptor.attempt_id)
            assert source is not None and source.committed_count == 1
            [loss] = await asyncio.to_thread(read_confirmed_losses, store.state_db)
            assert (loss.start_sequence, loss.end_sequence, loss.missing_record_count) == (101, 103, 2)
            result = reconciler.drained_result()
            assert isinstance(result, CollectionResult)
            assert result.next_sequence == 101
            assert result.advance_confirmed is False
        finally:
            await _close_real_writers(runtime)
            await session.close()

    asyncio.run(run())
