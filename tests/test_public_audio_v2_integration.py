from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureRecord,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_public_audio_v2,
    publication_contract_state,
    retire_acknowledged_public_audio_v2,
)
from omi_collector.capture.domain.clock_state import ClockRange, ClockState
from omi_collector.capture.domain.ring_protocol import DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig
from staging_test_factory import StagingStore


@pytest.fixture
def audio_store(tmp_path: Path) -> Iterator[tuple[StateDB, InternalAudioStore]]:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    try:
        yield database, store
    finally:
        store.close()


def _ready_root(root: Path) -> Path:
    ready = root / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return ready


def _record(timestamp: int, marker: int = 1) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker)) + bytes(AUDIO_RECORD_SIZE - 7)


def _zero_record(timestamp: int) -> bytes:
    return pack(">I", timestamp) + bytes(AUDIO_RECORD_SIZE - 4)


def _record_with_sentinels(timestamp: int, first_size: int, overflow_size: int, marker: int) -> bytes:
    record = bytearray(AUDIO_RECORD_SIZE)
    record[:4] = timestamp.to_bytes(4, "big")
    record[4] = first_size
    record[5] = 8
    record[6 : 5 + first_size] = bytes((marker,)) * (first_size - 1)
    record[4 + 438] = overflow_size
    return bytes(record)


def _source(
    store: InternalAudioStore, source_id: str, start: int, records: tuple[bytes, ...]
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


def _checkpoint(root: Path, acknowledged: list[dict[str, str]] | None = None) -> Path:
    path = root / "work" / "omi-ready-checkpoint.json"
    path.parent.mkdir()
    path.write_text(
        json.dumps(
            {
                "audio_contract": "omi-audio-v2",
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": acknowledged or [],
                "consumer": {"retained": True},
            }
        ),
        encoding="utf-8",
    )
    return path


def _options(
    clock_state: ClockState, frontier: int, *, drained: bool = True, threshold: float = 0.02
) -> PublicAudioV2Options:
    return PublicAudioV2Options(
        ReadyConfig(target_audio_seconds=threshold),
        clock_state,
        frontier,
        drained,
        128,
    )


def test_v2_waiting_for_drain_does_not_read_raw_pages(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "f" * 32, 10, (_record(100),))
    reads: list[str] = []
    read_record_page = store.read_record_page

    def tracked(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        reads.append(source_id)
        return read_record_page(source_id, after_ordinal, limit)

    monkeypatch.setattr(store, "read_record_page", tracked)
    outcome = finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11, drained=False))

    assert outcome.reason == "waiting_for_drain"
    assert outcome.stats is None
    assert reads == []
    assert publication_contract_state(database).next_public_order == 0


def _seed_retired_legacy_lineage(
    store: InternalAudioStore, database: StateDB, start: int, records: tuple[bytes, ...]
) -> None:
    source = _source(store, "a" * 32, start, records)
    end = start + len(records)
    digest = source.source.committed_sha256
    bundle_id = sha256(f"{start}:{end}:{digest}".encode()).hexdigest()
    database.put(
        StateKind.READY_PUBLICATION,
        bundle_id,
        {
            "records_sha256": "c" * 64,
            "state": "retired",
            "start_sequence": start,
            "next_sequence": end,
            "draft_raw_sha256": digest,
            "sources": [{"start_sequence": start, "next_sequence": end, "raw_sha256": digest}],
        },
    )


def test_sql_helpers_normalize_plain_and_state_database_rows(tmp_path: Path) -> None:
    plain = sqlite3.connect(":memory:")
    try:
        assert ready_bundles._sql_one(plain.execute("SELECT 1")) == (1,)
        assert ready_bundles._sql_all(plain.execute("SELECT 1 UNION ALL SELECT 2")) == ((1,), (2,))
    finally:
        plain.close()

    database = StateDB(tmp_path / "row-factory.sqlite3")
    with closing(database.open_connection(read_only=True)) as connection:
        assert isinstance(connection.execute("SELECT 1").fetchone(), sqlite3.Row)
        assert type(ready_bundles._sql_one(connection.execute("SELECT 1"))) is tuple
        assert ready_bundles._sql_all(connection.execute("SELECT 1 UNION ALL SELECT 2")) == ((1,), (2,))


def test_v2_activation_publish_and_ack_gc_are_bound_to_audio_hash(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100),))
    clock = ClockState((ClockRange(10, 11, 1.0, 0.25, "confirmed", "test"),))

    outcome = finalize_public_audio_v2(store, (source,), ready, _options(clock, 11))

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    artifact = outcome.published[0]
    assert {item.name for item in artifact.path.iterdir()} == {"audio.ogg", "manifest.json"}
    manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_text(encoding="utf-8")))
    assert manifest["format"] == "omi-audio-v2"
    assert (manifest["id"], manifest["order"], manifest["packet_count"]) == (artifact.bundle_id, 0, 1)
    ledger_entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert ledger_entry is not None
    assert ledger_entry["audio_bytes"] == (artifact.path / "audio.ogg").stat().st_size
    assert ledger_entry["manifest_bytes"] == (artifact.path / "manifest.json").stat().st_size
    assert publication_contract_state(database).next_public_order == 1
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(
            connection.execute("SELECT lifecycle,format,public_order,audio_sha256,packet_count FROM ready_replay_proof")
        ) == ("active", "omi-audio-v2", (0).to_bytes(8, "big"), manifest["audio_sha256"], 1)

    checkpoint.write_text(
        json.dumps(
            {"consumer": {"retained": True}, "acknowledged": [{"id": artifact.bundle_id, "audio_sha256": "0" * 64}]}
        ),
        encoding="utf-8",
    )
    with pytest.raises(ReadyBundleError, match="not a published artifact"):
        retire_acknowledged_public_audio_v2(ready, database, checkpoint)
    assert artifact.path.is_dir()

    checkpoint.write_text(
        json.dumps(
            {
                "consumer": {"retained": True},
                "acknowledged": [{"id": artifact.bundle_id, "audio_sha256": manifest["audio_sha256"]}],
            }
        ),
        encoding="utf-8",
    )
    assert retire_acknowledged_public_audio_v2(ready, database, checkpoint)[0].bundle_id == artifact.bundle_id
    assert not artifact.path.exists()
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_replay_proof")) == (0,)


def test_v2_stage_reservation_restarts_with_same_nonce_order_and_id(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100),))
    original = ready_bundles._materialize_v2_stage_with_failure_latch

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("injected restart")

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", interrupt)
    with pytest.raises(ReadyBundleError, match="injected restart"):
        finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))
    with closing(database.open_connection(read_only=True)) as connection:
        reserved = ready_bundles._sql_one(
            connection.execute("SELECT stage_nonce,public_order,artifact_id,descriptor FROM ready_v2_output_stage")
        )
    assert reserved is not None
    assert publication_contract_state(database).next_public_order == 1
    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", original)
    outcome = finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))

    assert outcome.published[0].bundle_id == reserved[2]
    assert json.loads((outcome.published[0].path / "manifest.json").read_text())["order"] == 0
    assert publication_contract_state(database).next_public_order == 1


def test_v2_accepted_cohort_without_stage_resumes_after_interruption(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100),))
    resume = ready_bundles._resume_v2_cohort

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("simulated loss after cohort acceptance")

    monkeypatch.setattr(ready_bundles, "_resume_v2_cohort", interrupt)
    with pytest.raises(ReadyBundleError, match="simulated loss"):
        finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))

    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT lifecycle FROM publication_cohort")) == ("accepted",)
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_v2_output_stage")) == (0,)
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_output_stage")) == (0,)

    monkeypatch.setattr(ready_bundles, "_resume_v2_cohort", resume)
    outcome = finalize_public_audio_v2(store, (), ready, _options(ClockState(), 11))

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert {path.name for path in ready.iterdir()} == {outcome.published[0].bundle_id}
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_output_stage")) == (0,)


def test_v2_staging_cohort_without_files_resumes_after_interruption(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "c" * 32, 10, (_record(100),))

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("simulated loss after stage intent")

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", interrupt)
    with pytest.raises(ReadyBundleError, match="simulated loss"):
        finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))

    with closing(database.open_connection(read_only=True)) as connection:
        row = ready_bundles._sql_one(connection.execute("SELECT lifecycle FROM publication_cohort"))
        assert row == ("staging",)
        stage = ready_bundles._sql_one(connection.execute("SELECT owned_path FROM ready_v2_output_stage"))
        assert stage is not None and isinstance(stage[0], str)
        stage_path = Path(stage[0])
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_output_stage")) == (0,)
    assert not Path(stage_path).exists()

    monkeypatch.undo()
    outcome = finalize_public_audio_v2(store, (), ready, _options(ClockState(), 11))

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert {path.name for path in ready.iterdir()} == {outcome.published[0].bundle_id}
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_v2_output_stage")) == (0,)
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_output_stage")) == (0,)


def test_v2_resume_rejects_legacy_cohort_digest_before_mutation(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "b" * 32, 10, (_record(100),))
    resume = ready_bundles._resume_v2_cohort

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("simulated loss after cohort acceptance")

    monkeypatch.setattr(ready_bundles, "_resume_v2_cohort", interrupt)
    with pytest.raises(ReadyBundleError, match="simulated loss"):
        finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))
    monkeypatch.setattr(ready_bundles, "_resume_v2_cohort", resume)
    cohort = ready_bundles._load_active_typed_cohort(store)
    assert cohort is not None
    original_cohort_id = cohort.cohort_id
    wrong_digest = "a" * 64
    wrong_cohort_id = ready_bundles._typed_cohort_id(cohort.admissions, cohort.threshold, cohort.frontier, wrong_digest)
    with closing(database.open_connection()) as connection:
        member = cast(tuple[object, ...], connection.execute("SELECT * FROM publication_cohort_member").fetchone())
        connection.execute("DELETE FROM publication_cohort_member WHERE cohort_id=?", (original_cohort_id,))
        connection.execute(
            "UPDATE publication_cohort SET cohort_id=?,accepted_permit=?,config_digest=? WHERE cohort_id=?",
            (wrong_cohort_id, wrong_cohort_id, wrong_digest, original_cohort_id),
        )
        connection.execute(
            "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,slice_count,"
            "admitted_lifecycle,admitted_count,admitted_sha256,cursor) VALUES (?,?,?,?,?,?,?,?,?)",
            (wrong_cohort_id, *member[1:]),
        )
        connection.commit()
    cohort = ready_bundles._load_active_typed_cohort(store)
    assert cohort is not None
    before_entries = tuple(sorted(path.name for path in ready.iterdir()))
    with closing(database.open_connection(read_only=True)) as connection:
        before = (
            ready_bundles._sql_all(connection.execute("SELECT * FROM publication_cohort")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM publication_cohort_member")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM ready_v2_output_stage")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM ready_output_stage")),
        )
    generation = database.get_generation()

    with pytest.raises(ReadyBundleError, match="config does not match the active contract"):
        ready_bundles._resume_v2_cohort(
            store,
            cohort,
            ready_bundles._V2ResumeContext(ready, ClockState(), 128, None, None),
        )

    assert tuple(sorted(path.name for path in ready.iterdir())) == before_entries
    with closing(database.open_connection(read_only=True)) as connection:
        after = (
            ready_bundles._sql_all(connection.execute("SELECT * FROM publication_cohort")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM publication_cohort_member")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM ready_v2_output_stage")),
            ready_bundles._sql_all(connection.execute("SELECT * FROM ready_output_stage")),
        )
    assert after == before
    assert database.get_generation() == generation


def test_v2_holds_all_zero_audio_without_reserving_order(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_zero_record(100),))

    outcome = finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))

    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert publication_contract_state(database).next_public_order == 0
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_v2_output_stage")) == (0,)


def test_v2_waits_for_drain_and_threshold_without_reserving_order(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100),))

    waiting_for_drain = finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11, drained=False))
    waiting_for_threshold = finalize_public_audio_v2(
        store, (source,), ready, _options(ClockState(), 11, threshold=0.04)
    )

    assert waiting_for_drain.reason == "waiting_for_drain"
    assert waiting_for_threshold.reason == "waiting_for_threshold"
    assert publication_contract_state(database).next_public_order == 0


def test_v2_cross_cohort_sentinel_match_then_mismatch_freezes_boundary_at_zero(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _database, store = audio_store
    ready = _ready_root(tmp_path)
    clock = ClockState((ClockRange(10, 13, 0.0, 0.0, "confirmed", "test"),))
    first_record = bytearray(_record(100, 1))
    first_record[4 + 438] = 75
    first = _source(store, "a" * 32, 10, (bytes(first_record),))
    first_outcome = finalize_public_audio_v2(store, (first,), ready, _options(clock, 11))
    assert first_outcome.state is ReadyOutcomeState.PUBLISHED

    matched = _source(store, "b" * 32, 11, (_record_with_sentinels(101, 75, 97, 2),))
    matched_outcome = finalize_public_audio_v2(store, (matched,), ready, _options(clock, 12))
    matched_manifest = cast(
        dict[str, object], json.loads((matched_outcome.published[0].path / "manifest.json").read_text())
    )
    assert matched_manifest["boundaries"] == []

    mismatched = _source(store, "c" * 32, 12, (_record_with_sentinels(102, 80, 40, 3),))
    mismatch_outcome = finalize_public_audio_v2(store, (mismatched,), ready, _options(clock, 13))
    mismatch_manifest = cast(
        dict[str, object], json.loads((mismatch_outcome.published[0].path / "manifest.json").read_text())
    )
    assert mismatch_manifest["boundaries"] == [{"at_packet": 0, "facts": ["time_incomparable"]}]


def test_v2_same_cohort_sequence_gap_splits_artifacts_and_marks_second_boundary(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _database, store = audio_store
    ready = _ready_root(tmp_path)
    first = _source(store, "a" * 32, 10, (_record(100),))
    second = _source(store, "b" * 32, 12, (_record(102, 2),))

    outcome = finalize_public_audio_v2(
        store,
        (first, second),
        ready,
        _options(ClockState((ClockRange(10, 13, 0.0, 0.0, "confirmed", "test"),)), 13),
    )

    assert len(outcome.published) == 2
    second_manifest = cast(dict[str, object], json.loads((outcome.published[1].path / "manifest.json").read_bytes()))
    assert second_manifest["boundaries"] == [{"at_packet": 0, "facts": ["source_loss", "time_incomparable"]}]


def test_v2_zero_only_groups_join_packet_artifact_without_losing_custody(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    leading_zero = _source(store, "a" * 32, 10, (_zero_record(100),))
    audio = _source(store, "b" * 32, 12, (_record(102, 2),))
    trailing_zero = _source(store, "c" * 32, 13, (_zero_record(103),))

    outcome = finalize_public_audio_v2(
        store,
        (leading_zero, audio, trailing_zero),
        ready,
        _options(ClockState((ClockRange(10, 14, 0.0, 0.0, "confirmed", "test"),)), 14),
    )

    assert len(outcome.published) == 1
    manifest = cast(dict[str, object], json.loads((outcome.published[0].path / "manifest.json").read_bytes()))
    assert manifest["boundaries"] == [{"at_packet": 0, "facts": ["source_loss", "time_incomparable"]}]
    entry = database.get(StateKind.READY_PUBLICATION, outcome.published[0].bundle_id)
    assert entry is not None
    sources = cast(list[dict[str, object]], entry["sources"])
    assert [(item["start_sequence"], item["next_sequence"]) for item in sources] == [
        (10, 11),
        (12, 13),
        (13, 14),
    ]


def test_v2_same_cohort_sentinel_mismatch_splits_artifacts_and_marks_second_boundary(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _database, store = audio_store
    ready = _ready_root(tmp_path)
    first = bytearray(_record_with_sentinels(100, 2, 75, 1))
    second = _record_with_sentinels(101, 80, 40, 2)
    source = _source(store, "a" * 32, 10, (bytes(first), second))

    outcome = finalize_public_audio_v2(
        store,
        (source,),
        ready,
        _options(ClockState((ClockRange(10, 12, 0.0, 0.0, "confirmed", "test"),)), 12),
    )

    assert len(outcome.published) == 2
    second_manifest = cast(dict[str, object], json.loads((outcome.published[1].path / "manifest.json").read_bytes()))
    assert second_manifest["boundaries"] == [{"at_packet": 0, "facts": ["time_incomparable"]}]


def test_v2_exact_replay_of_legacy_retired_lineage_is_suppressed(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100), _record(101, 2))
    _seed_retired_legacy_lineage(store, database, 10, records)
    replay = _source(store, "b" * 32, 10, records)

    outcome = finalize_public_audio_v2(store, (replay,), ready, _options(ClockState(), 12))

    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert outcome.published == ()
    assert publication_contract_state(database).next_public_order == 0


def test_v2_partial_overlap_with_legacy_retired_lineage_fails_closed(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100), _record(101, 2))
    _seed_retired_legacy_lineage(store, database, 10, records)
    partial = _source(store, "b" * 32, 11, (records[1],))

    with pytest.raises(ReadyBundleError, match="overlaps retired custody"):
        finalize_public_audio_v2(store, (partial,), ready, _options(ClockState(), 12))
    assert publication_contract_state(database).next_public_order == 0


def test_v2_active_artifact_suppresses_exact_replay_and_rejects_conflicting_overlap(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100), _record(101, 2))
    published = finalize_public_audio_v2(
        store, (_source(store, "a" * 32, 10, records),), ready, _options(ClockState(), 12)
    )
    artifact = published.published[0]
    exact = finalize_public_audio_v2(store, (_source(store, "b" * 32, 10, records),), ready, _options(ClockState(), 12))
    before = database.list(StateKind.READY_PUBLICATION)
    conflicting = _source(store, "c" * 32, 11, (_record(101, 9),))

    assert exact.published == ()
    with pytest.raises(ReadyBundleError):
        finalize_public_audio_v2(store, (conflicting,), ready, _options(ClockState(), 12))
    assert artifact.path.is_dir()
    assert database.list(StateKind.READY_PUBLICATION) == before


def test_staging_store_preserves_subthreshold_source_under_v2_contract(tmp_path: Path) -> None:
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.04))
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
    ready = _ready_root(tmp_path)
    store = StagingStore.from_paths(
        bootstrap.paths,
        state_db=bootstrap.state_db,
        publication_root=ready,
        config=config,
    )
    attempt = store.prepare_streaming_attempt(10, 1)
    attempt.prepare_leg(10, 1)
    attempt.record_read_begin(ReadBeginNotification(10, 1))
    attempt.accept_chunk(10, _record(100))
    attempt.seal(DoneNotification(0, 11))
    source_id = attempt.attempt_id
    attempt.close()
    store.append_ready_closure(11, "drained")

    waiting = store.recover_and_publish()

    assert waiting.state is ReadyOutcomeState.WAITING, waiting.reason
    assert waiting.reason == "waiting_for_threshold"
    assert tuple(ready.iterdir()) == ()
    with InternalAudioStore(store.state_db, chunk_limit=config.writer.chunk_records) as audio:
        source = audio.get_source(source_id)
        assert source is not None and source.lifecycle == "sealed_waiting"


def test_staging_store_ack_preserves_multi_page_gc_progress_with_subthreshold_source(tmp_path: Path) -> None:
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
    ready = _ready_root(tmp_path)
    store = StagingStore.from_paths(
        bootstrap.paths,
        state_db=bootstrap.state_db,
        publication_root=ready,
        config=config,
    )
    checkpoint = _checkpoint(tmp_path)
    with InternalAudioStore(store.state_db) as audio_store:
        records = tuple(_record(100 + index, index % 254 + 1) for index in range(129))
        audio_store.create_source("a" * 32, "attempt", 10, 139)
        source = audio_store.prepare_leg("a" * 32, 10, len(records))
        source = audio_store.record_read_begin("a" * 32, 10, len(records))
        audio_store.commit_drain(
            "a" * 32,
            0,
            records[:128],
            DrainCheckpoint(source.current_leg_generation),
        )
        audio_store.commit_drain(
            "a" * 32,
            128,
            records[128:],
            DrainCheckpoint(source.current_leg_generation),
        )
        source = audio_store.get_source("a" * 32)
        assert source is not None
        audio_store.seal_source(
            "a" * 32,
            leg_generation=source.current_leg_generation,
            expected_count=source.committed_count,
            expected_sha256=source.committed_sha256,
        )
    store.append_ready_closure(139, "drained")

    published = store.recover_and_publish()
    assert published.published, (published.state, published.reason)
    artifact = published.published[0]
    manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        source_state = cast(
            tuple[str, int, int] | None,
            connection.execute(
                "SELECT lifecycle,retirement_cursor,(SELECT count(*) FROM capture_record WHERE source_id=capture_source.source_id) "
                "FROM capture_source WHERE source_id=?",
                ("a" * 32,),
            ).fetchone(),
        )
    assert source_state is not None and tuple(source_state) == ("retiring", 0, 129)
    with InternalAudioStore(store.state_db) as audio_store:
        _source(audio_store, "b" * 32, 200, (_zero_record(200),))
    store.append_ready_closure(201, "drained")
    checkpoint.write_text(
        json.dumps(
            {
                "acknowledged": [
                    {"id": artifact.bundle_id, "audio_sha256": manifest["audio_sha256"]},
                ]
            }
        ),
        encoding="utf-8",
    )

    with closing(store.state_db.open_connection(read_only=True)) as connection:
        member_count = cast(
            int,
            connection.execute(
                "SELECT count(*) FROM publication_cohort_member WHERE source_id=?", ("a" * 32,)
            ).fetchone()[0],
        )
    assert member_count == 0

    assert store.retire_acknowledged_ready(should_defer=lambda: False) == 1
    assert not artifact.path.exists()
    retirements = 0
    while store.retire_covered_raw_page(should_defer=lambda: False):
        retirements += 1
        assert retirements <= 3
    assert retirements == 3
    with InternalAudioStore(store.state_db) as audio_store:
        assert audio_store.get_source("a" * 32) is None
        preserved = audio_store.get_source("b" * 32)
        assert preserved is not None and preserved.lifecycle == "sealed_waiting"
