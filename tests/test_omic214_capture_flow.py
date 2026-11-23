from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import cast

from fakes import ScriptedRingSession, WriteStep
from framing_recovery_fixtures import SyntheticRead, framing_recovery_corpus
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.internal_audio_store import decode_uint64
from omi_collector.capture.application.session_lifecycle import SessionPhaseState
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo, RingStatus, encode_read_command
from staging_test_factory import attempt_records
from test_batch_reconciliation import _async_test, _close_real_writers, _make_reconciler, _options, _Runtime


async def _assert_recovery_alert(store: object, incident_id: str) -> None:
    from omi_collector.capture.adapters.staging_store import StagingStore

    assert isinstance(store, StagingStore)
    assert await asyncio.to_thread(store.state_db.list, StateKind.AUDIO_DAMAGE_ALERT) == (
        (incident_id, {"outcome": "recovered"}),
    )
    store.public_drain.path.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(store.public_drain.publish_snapshot)
    snapshot = cast(dict[str, object], json.loads(store.public_drain.path.read_text(encoding="utf-8")))
    assert snapshot["audio_damage_incidents"] == [{"incident_id": incident_id, "outcome": "recovered"}]


@_async_test
async def test_shifted_read_replay_recovers_numbered_audio_and_retains_evidence(tmp_path: Path) -> None:
    corpus = framing_recovery_corpus()
    record_count = corpus.shifted_read.packet_count
    verified_bytes = (corpus.verified * 3)[: record_count * RECORD_SIZE]
    command = encode_read_command(700, record_count)
    session = ScriptedRingSession(
        RingStatus(0, record_count, 0, 1),
        (
            WriteStep(command, corpus.shifted_read.notifications),
            WriteStep(command, SyntheticRead(700, record_count, 700 + record_count, (verified_bytes,)).notifications),
        ),
    )
    current = RingInfo(700, 700 + record_count, 100, 0, RECORD_SIZE)
    runtime = _Runtime()
    store, reconciler = _make_reconciler(
        tmp_path,
        runtime,
        replace(_options(advance=False), policy=replace(_options(advance=False).policy, batch_records=record_count)),
    )

    async def read_info(_session: object) -> RingInfo:
        return current

    try:
        assert await reconciler.connected_step(session, current, read_info, SessionPhaseState("read/reconcile")) == (
            None,
            current,
        )
        damaged_attempt = runtime.proxies[0].attempt_id
        assert await asyncio.to_thread(store.state_db.list, StateKind.AUDIO_DAMAGE_ALERT) == ()

        assert await reconciler.connected_step(session, current, read_info, SessionPhaseState("read/reconcile")) == (
            "collected",
            current,
        )
        recovered_attempt = runtime.proxies[1].attempt_id
        await _assert_recovery_alert(store, damaged_attempt)

        assert session.writes == [command, command]
        assert attempt_records(store, recovered_attempt) == tuple(
            verified_bytes[offset : offset + RECORD_SIZE] for offset in range(0, len(verified_bytes), RECORD_SIZE)
        )
        with closing(sqlite3.connect(store.state_db.path)) as connection:
            observation = cast(
                tuple[int, int, bytes, int] | None,
                connection.execute(
                    "SELECT fault_offset,received_bytes,tail,done FROM capture_observation WHERE source_id=?",
                    (damaged_attempt,),
                ).fetchone(),
            )
            observed_chunks = cast(
                list[tuple[int, bytes]],
                connection.execute(
                    "SELECT byte_offset,bytes FROM capture_observation_chunk WHERE source_id=? ORDER BY byte_offset",
                    (damaged_attempt,),
                ).fetchall(),
            )
            incident = cast(
                tuple[str, str, bytes] | None,
                connection.execute(
                    "SELECT status,settlement_reason,settled_head FROM capture_framing_incident WHERE incident_id=?",
                    (damaged_attempt,),
                ).fetchone(),
            )
            read_leg = cast(
                tuple[bytes, int, int, bytes, int] | None,
                connection.execute(
                    "SELECT read_start,read_count,received_bytes,done_next,verified_count FROM capture_read_leg WHERE source_id=?",
                    (recovered_attempt,),
                ).fetchone(),
            )

        assert observation == (0, len(corpus.shifted_read.raw_bytes), corpus.shifted_read.raw_bytes[-88:], 1)
        assert observed_chunks == [(0, corpus.shifted_read.raw_bytes[:-88])]
        assert incident is not None
        assert incident[:2] == ("settled", "independent_complete_read")
        assert decode_uint64(incident[2]) == current.write_sequence
        assert read_leg is not None
        assert (
            decode_uint64(read_leg[0]),
            read_leg[1],
            read_leg[2],
            decode_uint64(read_leg[3]),
            read_leg[4],
        ) == (700, record_count, len(verified_bytes), current.write_sequence, record_count)
    finally:
        await _close_real_writers(runtime)
        await session.close()
