"""Public ready-bundle publication contracts for SQL-backed audio sources."""

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

    def source_page(self, after_ordinal: int = -1, limit: int = 128):
        return _TypedPage(
            self.source,
            after_ordinal,
            self.store.read_record_page(self.source_id, after_ordinal, limit),
        )


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(store: InternalAudioStore, source_id: str, raw: bytes) -> _Source:
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
    return _Source(store, source_id)


def _publish(store: InternalAudioStore, sources: tuple[_Source, ...], ready: Path):
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=101,
        drained=True,
    )


def test_ready_publication_preserves_bundle_bytes_manifest_and_ledger(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    raw = _record(100, 7)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)

    outcome = _publish(store, (_source(store, "a" * 32, raw),), ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    (bundle,) = outcome.published
    records = (bundle.path / "records.bin").read_bytes()
    manifest_bytes = (bundle.path / "manifest.json").read_bytes()
    manifest = cast(dict[str, object], json.loads(manifest_bytes))
    assert records == raw
    assert manifest == {
        "bundle_id": bundle.bundle_id,
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": AUDIO_RECORD_SIZE,
        "records_sha256": sha256(raw).hexdigest(),
        "draft_raw_sha256": sha256(raw).hexdigest(),
        "time_ranges": [{"start_sequence": 100, "next_sequence": 101, "utc": None}],
    }
    assert {path.name for path in bundle.path.iterdir()} == {"records.bin", "manifest.json"}
    row = database.get(StateKind.READY_PUBLICATION, bundle.bundle_id)
    assert row is not None and row["state"] == "ready"
    assert row["records_sha256"] == manifest["records_sha256"]
    store.close()


def test_ready_publication_fails_closed_on_corrupted_existing_bundle(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    raw = _record(100, 7)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    outcome = _publish(store, (_source(store, "a" * 32, raw),), ready)
    (bundle,) = outcome.published
    path = bundle.path / "records.bin"
    path.write_bytes(bytes((raw[0] ^ 1,)) + raw[1:])

    with pytest.raises(ReadyBundleError):
        _publish(store, (), ready)

    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[str, int, int],
            connection.execute(
                "SELECT lifecycle, retirement_cursor, "
                "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                "FROM capture_source WHERE source_id=?",
                ("a" * 32, "a" * 32),
            ).fetchone(),
        )
        assert (row[0], row[1], row[2]) == ("retiring", 1, 0)
    store.close()


def test_ready_publication_rejects_symlinked_bundle_destination(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    outcome = _publish(store, (_source(store, "c" * 32, _record(100, 9)),), ready)
    (bundle,) = outcome.published
    outside = tmp_path / "outside"
    bundle.path.rename(outside)
    sentinel = outside / "sentinel"
    sentinel.write_bytes(b"preserve")
    bundle.path.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ReadyBundleError):
        _publish(store, (), ready)

    assert sentinel.read_bytes() == b"preserve"
    assert bundle.path.is_symlink()
    store.close()
