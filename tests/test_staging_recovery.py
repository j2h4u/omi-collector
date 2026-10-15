"""Restart and ACK recovery contracts for SQL-backed ready publication."""

from __future__ import annotations

import json
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcomeState,
    _TypedPage,
    finalize_typed_sources,
    retire_acknowledged,
)
from omi_collector.config import ReadyConfig


class _Source:
    def __init__(self, store: InternalAudioStore, source_id: str) -> None:
        self.store = store
        self.source_id = source_id

    @property
    def source(self):
        source = self.store.get_source(self.source_id)
        assert source is not None
        return source

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> _TypedPage:
        return _TypedPage(
            self.source,
            after_ordinal,
            self.store.read_record_page(self.source_id, after_ordinal, limit),
        )


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _sealed_source(store: InternalAudioStore, source_id: str) -> tuple[_Source, bytes]:
    raw = _record(100, 7)
    store.create_source(source_id, "attempt", 100, 101)
    store.prepare_leg(source_id, 100, 1)
    store.record_read_begin(source_id, 100, 1)
    store.commit_drain(source_id, 0, (raw,), DrainCheckpoint(1))
    store.seal_source(
        source_id,
        leg_generation=1,
        expected_count=1,
        expected_sha256=sha256(raw).hexdigest(),
    )
    return _Source(store, source_id), raw


def _publish(store: InternalAudioStore, source: _Source, ready: Path):
    return finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=101,
        drained=True,
    )


def _checkpoint(path: Path, bundle_id: str, digest: str) -> None:
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [{"bundle_id": bundle_id, "records_sha256": digest}],
            }
        ),
        encoding="utf-8",
    )


def test_ack_retires_only_matching_ready_bundle_and_keeps_lineage(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source, _ = _sealed_source(store, "a" * 32)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    outcome = _publish(store, source, ready)
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    (bundle,) = outcome.published
    checkpoint = tmp_path / "work" / "omi-ready-checkpoint.json"
    _checkpoint(checkpoint, bundle.bundle_id, bundle.records_sha256)

    retired = retire_acknowledged(ready, database, checkpoint)

    assert tuple(item.bundle_id for item in retired) == (bundle.bundle_id,)
    assert not bundle.path.exists()
    row = database.get(StateKind.READY_PUBLICATION, bundle.bundle_id)
    assert row is not None and row["state"] == "retired"
    assert row["records_sha256"] == bundle.records_sha256
    assert row.get("sources")
    store.close()


def test_mismatched_ack_preserves_ready_bundle_and_ledger(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source, _ = _sealed_source(store, "b" * 32)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    outcome = _publish(store, source, ready)
    (bundle,) = outcome.published
    checkpoint = tmp_path / "work" / "omi-ready-checkpoint.json"
    _checkpoint(checkpoint, bundle.bundle_id, "0" * 64)

    with pytest.raises(ReadyBundleError):
        retire_acknowledged(ready, database, checkpoint)

    assert bundle.path.is_dir()
    row = database.get(StateKind.READY_PUBLICATION, bundle.bundle_id)
    assert row is not None and row["state"] == "ready"
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[str, int, int],
            connection.execute(
                "SELECT lifecycle, retirement_cursor, "
                "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                "FROM capture_source WHERE source_id=?",
                (source.source_id, source.source_id),
            ).fetchone(),
        )
        assert (row[0], row[1], row[2]) == ("retiring", 1, 0)
    store.close()
