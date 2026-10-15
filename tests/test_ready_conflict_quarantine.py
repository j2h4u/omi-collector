from __future__ import annotations

import shutil
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

import omi_collector.capture.adapters.ready_bundles as ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcomeState,
    _TypedPage,
    finalize_typed_sources,
)
from omi_collector.config import ReadyConfig


class _Source:
    def __init__(self, store: InternalAudioStore, source_id: str) -> None:
        self._store = store
        self.source_id = source_id

    @property
    def source(self):
        source = self._store.get_source(self.source_id)
        assert source is not None
        return source

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> _TypedPage:
        return _TypedPage(
            self.source,
            after_ordinal,
            self._store.read_record_page(self.source_id, after_ordinal, limit),
        )


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(store: InternalAudioStore, source_id: str, start: int, records: tuple[bytes, ...]) -> _Source:
    store.create_source(source_id, "attempt", start, start + len(records))
    store.prepare_leg(source_id, start, len(records))
    store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(1))
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE capture_source SET lifecycle='sealed_waiting' WHERE source_id=?", (source_id,))
        connection.commit()
    return _Source(store, source_id)


def _ready_root(path: Path) -> Path:
    path.mkdir()
    path.chmod(0o2750)
    return path


def _publish(store: InternalAudioStore, sources: tuple[_Source, ...], ready: Path, frontier: int):
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
    )


def _pause_after_acceptance(
    monkeypatch: pytest.MonkeyPatch, store: InternalAudioStore, source: _Source, ready: Path, frontier: int
) -> None:
    original = ready_bundles._resume_typed_cohort

    def stop(*_args: object, **_kwargs: object) -> tuple[list[object], None]:
        raise ReadyBundleError("test pause after cohort acceptance")

    monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", stop)
    with pytest.raises(ReadyBundleError, match="pause after cohort"):
        _publish(store, (source,), ready, frontier)
    monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", original)


def _install_custody(tmp_path: Path, database: StateDB, ready: Path, records: tuple[bytes, ...]) -> None:
    seed_database = StateDB(tmp_path / "seed.sqlite3")
    seed_ready = _ready_root(tmp_path / "seed-ready")
    with InternalAudioStore(seed_database) as seed_store:
        seed_source = _source(seed_store, "f" * 32, 100, records)
        outcome = _publish(seed_store, (seed_source,), seed_ready, 100 + len(records))
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        destination = ready / outcome.published[0].bundle_id
        shutil.copytree(outcome.published[0].path, destination)
    for item_id, payload in seed_database.list(StateKind.READY_PUBLICATION):
        database.put(StateKind.READY_PUBLICATION, item_id, payload)


def _source_snapshot(database: StateDB, source_id: str) -> tuple[object, ...]:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...],
            connection.execute(
                "SELECT committed_count,committed_sha256,"
                "(SELECT group_concat(hex(device_timestamp)||hex(payload), '') FROM "
                "(SELECT device_timestamp,payload FROM capture_record WHERE source_id=? ORDER BY ordinal)) "
                "FROM capture_source WHERE source_id=?",
                (source_id, source_id),
            ).fetchone(),
        )
    return tuple(row)


def _conflict_state(database: StateDB, source_id: str) -> tuple[object, ...]:
    with closing(database.open_connection(read_only=True)) as connection:
        return tuple(
            cast(
                tuple[object, ...],
                connection.execute(
                    "SELECT source_type,lifecycle,block_reason,quarantine_disposition,quarantine_opaque,"
                    "terminal_reason,terminal_time,retention_deadline,certified_prefix_count,coverage_digest,"
                    "retirement_cursor FROM capture_source WHERE source_id=?",
                    (source_id,),
                ).fetchone(),
            )
        )


def _cohort_count(database: StateDB) -> int:
    with closing(database.open_connection(read_only=True)) as connection:
        return cast(tuple[int], connection.execute("SELECT count(*) FROM publication_cohort").fetchone())[0]


@pytest.mark.parametrize(
    ("ready_records", "incoming_records"),
    (
        ((_record(100, 1),), (_record(100, 2),)),
        ((_record(100, 1),), (_record(101, 1), _record(102, 3))),
        ((_record(100, 1),), (_record(100, 2), _record(101, 3))),
    ),
    ids=("full-payload-conflict", "partial-device-timestamp-conflict", "partial-payload-conflict"),
)
def test_ready_overlap_conflict_quarantines_without_changing_canonical_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ready_records: tuple[bytes, ...],
    incoming_records: tuple[bytes, ...],
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, incoming_records)
        _pause_after_acceptance(monkeypatch, store, source, ready, 100 + len(incoming_records))
        _install_custody(tmp_path, database, ready, ready_records)
        before = _source_snapshot(database, source.source_id)

        with pytest.raises(ReadyBundleError, match="conflict"):
            _publish(store, (source,), ready, 100 + len(incoming_records))

        assert _source_snapshot(database, source.source_id) == before
        assert before[:2] == (len(incoming_records), sha256(b"".join(incoming_records)).hexdigest())
        assert _conflict_state(database, source.source_id) == (
            "quarantine",
            "blocked",
            "ready_overlap_conflict",
            "unclassified",
            0,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        assert _cohort_count(database) == 0


def test_byte_identical_ready_overlap_retires_source_without_duplicate_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        raw = (_record(100, 1),)
        source = _source(store, "a" * 32, 100, raw)
        _pause_after_acceptance(monkeypatch, store, source, ready, 101)
        _install_custody(tmp_path, database, ready, raw)

        outcome = _publish(store, (source,), ready, 101)

        assert outcome.state is ReadyOutcomeState.WAITING
        assert outcome.reason == "already_published"
        assert outcome.published == ()
        assert len(tuple(path for path in ready.iterdir() if not path.name.startswith("."))) == 1
        assert _cohort_count(database) == 0
        assert _conflict_state(database, source.source_id)[1] == "retiring"
