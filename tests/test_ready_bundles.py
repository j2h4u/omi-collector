from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from hashlib import sha256
from json import dumps
from pathlib import Path
from struct import pack
from typing import NoReturn, cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureRecord,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcome,
    ReadyOutcomeState,
    PublicAudioV2Options,
    SqlitePublicationSource,
    finalize_public_audio_v2,
    _valid_publication_dates,
)
from omi_collector.config import ReadyConfig


def _record(timestamp: int, marker: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker)) + bytes(AUDIO_RECORD_SIZE - 7)


def _two_packet_record(timestamp: int, marker: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker, 2, 8, marker + 1)) + bytes(AUDIO_RECORD_SIZE - 10)


def _state_db(root: Path) -> StateDB:
    root.mkdir(parents=True, exist_ok=True)
    return StateDB(root / "collector-state.sqlite3")


@pytest.fixture
def audio_store(tmp_path: Path) -> Iterator[tuple[StateDB, InternalAudioStore]]:
    state_db = _state_db(tmp_path)
    store = InternalAudioStore(state_db)
    try:
        yield state_db, store
    finally:
        store.close()


def _ready_root(root: Path) -> Path:
    ready = root / "ready"
    ready.mkdir(parents=True, exist_ok=True, mode=0o2750)
    ready.chmod(0o2750)
    return ready


def _source(
    store: InternalAudioStore,
    source_id: str,
    start: int,
    records: tuple[bytes, ...],
) -> SqlitePublicationSource:
    store.create_source(source_id, "attempt", start, start + len(records))
    source = store.prepare_leg(source_id, start, len(records))
    source = store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _publish(
    store: InternalAudioStore,
    sources: tuple[SqlitePublicationSource, ...],
    ready: Path,
) -> ReadyOutcome:
    frontier = max(source.source.sequence_start + source.source.committed_count for source in sources)
    return finalize_public_audio_v2(
        store,
        sources,
        ready,
        PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), frontier, True),
    )


def _finalize(
    store: InternalAudioStore,
    sources: tuple[SqlitePublicationSource, ...],
    ready: Path,
    *,
    config: ReadyConfig | None = None,
    frontier: int | None = None,
    drained: bool = True,
) -> ReadyOutcome:
    end = max(source.source.sequence_start + source.source.committed_count for source in sources)
    return finalize_public_audio_v2(
        store,
        sources,
        ready,
        PublicAudioV2Options(
            config or ReadyConfig(target_audio_seconds=0.02),
            ClockState(),
            end if frontier is None else frontier,
            drained,
        ),
    )


def _entry(state_db: StateDB, bundle_id: str) -> dict[str, object]:
    value = state_db.get(StateKind.READY_PUBLICATION, bundle_id)
    assert value is not None
    return value


def test_threshold_requires_both_enough_audio_and_a_completed_drain(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100, 1),))

    waiting_for_drain = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=11,
        drained=False,
    )
    waiting_for_audio = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        frontier=11,
        drained=True,
    )
    published = _publish(store, (source,), ready)

    assert waiting_for_drain.reason == "waiting_for_drain"
    assert waiting_for_audio.reason == "waiting_for_threshold"
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.published[0].record_count == 1


def test_undrained_v2_publication_does_not_read_raw_pages(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "f" * 32, 10, (_record(100, 1),))
    reads: list[str] = []
    read_record_page = store.read_record_page

    def tracked(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        reads.append(source_id)
        return read_record_page(source_id, after_ordinal, limit)

    monkeypatch.setattr(store, "read_record_page", tracked)
    outcome = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=11,
        drained=False,
    )

    assert outcome.reason == "waiting_for_drain"
    assert outcome.stats is None
    assert reads == []


def test_disjoint_subthreshold_growth_reuses_durable_packet_summary(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    first = _source(store, "a" * 32, 10, (_two_packet_record(100, 1),))
    reads: list[str] = []
    read_record_page = store.read_record_page

    def tracked(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        reads.append(source_id)
        return read_record_page(source_id, after_ordinal, limit)

    monkeypatch.setattr(store, "read_record_page", tracked)
    config = ReadyConfig(target_audio_seconds=0.08)
    first_wait = _finalize(store, (first,), ready, config=config, frontier=11, drained=True)
    assert first_wait.reason == "waiting_for_threshold"
    assert first_wait.stats is not None and first_wait.stats.audio_packets == 2
    assert reads and set(reads) == {first.source_id}
    summaries = state_db.list(StateKind.PACKET_SUMMARY)
    assert len(summaries) == 1
    assert summaries[0][0] == first.source_id
    assert summaries[0][1]["audio_packets"] == 2
    assert set(summaries[0][1]) == {
        "schema_version",
        "sequence_start",
        "admitted_count",
        "admitted_sha256",
        "audio_packets",
        "source_fingerprint",
    }

    reads.clear()
    second = _source(store, "b" * 32, 20, (_record(101, 2),))
    second_wait = _finalize(
        store,
        (SqlitePublicationSource(store, first.source_id), second),
        ready,
        config=config,
        frontier=21,
        drained=True,
    )
    assert second_wait.reason == "waiting_for_threshold"
    assert reads and set(reads) == {second.source_id}
    assert second_wait.stats is not None
    assert (second_wait.stats.audio_packets, second_wait.stats.gap_count, second_wait.stats.gap_records) == (3, 1, 9)


def test_cached_old_sources_and_new_threshold_crossing_read_each_for_its_role(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = audio_store[1]
    ready = _ready_root(tmp_path)
    sources = (
        _source(store, "a" * 32, 10, (_two_packet_record(100, 1),)),
        _source(store, "b" * 32, 20, (_record(101, 2),)),
    )
    config = ReadyConfig(target_audio_seconds=0.08)
    assert _finalize(store, sources, ready, config=config, frontier=21, drained=True).reason == "waiting_for_threshold"

    reads: list[str] = []
    preflight_reads: list[str] = []
    normalization_reads: list[str] = []
    precomputed_sources: list[str] = []
    read_record_page = store.read_record_page
    normalize = ready_bundles._normalize_typed_admissions

    def tracked_page(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        reads.append(source_id)
        return read_record_page(source_id, after_ordinal, limit)

    def tracked_normalize(
        admissions: tuple[ready_bundles._TypedAdmission, ...],
        page_limit: int,
        custody: ready_bundles._TypedReadyCustody | None = None,
        precomputed: tuple[ready_bundles._TypedSlice, ...] = (),
    ) -> tuple[ready_bundles._TypedSlice, ...]:
        precomputed_sources.extend(item.admission.snapshot.source_id for item in precomputed)
        if not precomputed:
            return normalize(admissions, page_limit, custody, precomputed)
        preflight_reads.extend(reads)
        reads.clear()
        result = normalize(admissions, page_limit, custody, precomputed)
        normalization_reads.extend(reads)
        return result

    monkeypatch.setattr(store, "read_record_page", tracked_page)
    monkeypatch.setattr(ready_bundles, "_normalize_typed_admissions", tracked_normalize)
    third = _source(store, "d" * 32, 30, (_two_packet_record(102, 3),))
    published = _finalize(
        store,
        (*sources, third),
        ready,
        config=config,
        frontier=31,
        drained=True,
    )
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert precomputed_sources == [third.source_id]
    assert normalization_reads == [source.source_id for source in sources]
    assert preflight_reads == [third.source_id]


def test_packet_summary_rejects_certification_change_during_lookup(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "b" * 32, 10, (_record(100, 1),))
    options = {
        "config": ReadyConfig(target_audio_seconds=0.04),
        "frontier": 11,
        "drained": True,
    }
    assert _finalize(store, (source,), ready, **options).reason == "waiting_for_threshold"
    get_state = state_db.get
    changed = False

    def change_certification(kind: StateKind, item_id: str) -> dict[str, object] | None:
        nonlocal changed
        value = get_state(kind, item_id)
        if kind is StateKind.PACKET_SUMMARY and not changed:
            changed = True
            with store.publication_transaction() as (_state, connection):
                connection.execute(
                    "UPDATE capture_source SET coverage_digest=? WHERE source_id=?",
                    ("a" * 64, source.source_id),
                )
        return value

    monkeypatch.setattr(state_db, "get", change_certification)
    with pytest.raises(ReadyBundleError, match="packet summary certification changed"):
        _finalize(store, (SqlitePublicationSource(store, source.source_id),), ready, **options)


def test_threshold_crossing_reuses_preflight_slice_for_full_normalization(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "c" * 32, 10, (_two_packet_record(100, 1),))
    read_record_page = store.read_record_page
    page_reads: list[str] = []
    normalized_reads: list[str] = []
    precomputed_sources: list[str] = []

    def tracked_page(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        page_reads.append(source_id)
        return read_record_page(source_id, after_ordinal, limit)

    normalize = ready_bundles._normalize_typed_admissions

    def tracked_normalize(
        admissions: tuple[ready_bundles._TypedAdmission, ...],
        page_limit: int,
        custody: ready_bundles._TypedReadyCustody | None = None,
        precomputed: tuple[ready_bundles._TypedSlice, ...] = (),
    ) -> tuple[ready_bundles._TypedSlice, ...]:
        precomputed_sources.extend(item.admission.snapshot.source_id for item in precomputed)
        page_reads.clear()
        result = normalize(admissions, page_limit, custody, precomputed)
        normalized_reads.extend(page_reads)
        return result

    monkeypatch.setattr(store, "read_record_page", tracked_page)
    monkeypatch.setattr(ready_bundles, "_normalize_typed_admissions", tracked_normalize)
    outcome = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        frontier=11,
        drained=True,
    )

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert precomputed_sources == [source.source_id]
    assert normalized_reads == []


def test_busy_packet_summary_write_keeps_subthreshold_source_retryable(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "e" * 32, 10, (_record(100, 1),))
    generation = state_db.get_generation()
    start_write = store._start_write

    def busy_write(_connection: sqlite3.Connection) -> NoReturn:
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(store, "_start_write", busy_write)
    outcome = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        frontier=11,
        drained=True,
    )

    assert outcome.reason == "waiting_for_threshold"
    assert outcome.stats is not None and outcome.stats.audio_packets == 1
    assert state_db.list(StateKind.PACKET_SUMMARY) == ()
    assert state_db.get_generation() == generation
    assert store.get_source(source.source_id) == source.source
    monkeypatch.setattr(store, "_start_write", start_write)
    retry = _finalize(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        frontier=11,
        drained=True,
    )
    assert retry.reason == "waiting_for_threshold"
    assert len(state_db.list(StateKind.PACKET_SUMMARY)) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"formed_on_utc": "2026-2-03"},
        {"formed_on_utc": "2026-02-03", "recorded_on_utc_min": None},
        {
            "formed_on_utc": "2026-02-03",
            "recorded_on_utc_min": "2026-02-04",
            "recorded_on_utc_max": "2026-02-03",
        },
    ],
)
def test_ledger_rejects_malformed_or_partial_publication_dates(changes: dict[str, object]) -> None:
    assert not _valid_publication_dates(changes)
