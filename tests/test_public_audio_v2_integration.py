from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    ReadyOutcomeState,
    SqlitePublicationSource,
    activate_public_audio_v2,
    begin_publication_cutover,
    finalize_public_audio_v2,
    finalize_typed_sources,
    publication_contract_state,
    retire_acknowledged,
    retire_acknowledged_public_audio_v2,
)
from omi_collector.capture.domain.clock_state import ClockRange, ClockState
from omi_collector.capture.domain.publication_contract import ContractMode
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
    path.write_text(json.dumps({"acknowledged": acknowledged or []}), encoding="utf-8")
    return path


def _activate(store: InternalAudioStore, ready: Path, checkpoint: Path) -> None:
    assert begin_publication_cutover(store.state_db).mode is ContractMode.QUIESCING
    assert activate_public_audio_v2(store, ready, checkpoint).mode is ContractMode.V2_OPEN


def _options(clock_state: ClockState, frontier: int) -> PublicAudioV2Options:
    return PublicAudioV2Options(
        ReadyConfig(target_audio_seconds=0.02),
        clock_state,
        frontier,
        True,
        128,
    )


def _publish_and_retire_legacy(
    store: InternalAudioStore,
    database: StateDB,
    ready: Path,
    source: SqlitePublicationSource,
    frontier: int,
) -> None:
    outcome = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
    )
    artifact = outcome.published[0]
    retire_acknowledged(
        ready,
        database,
        ready.parent / "unused-checkpoint.json",
        acknowledged=((artifact.bundle_id, artifact.records_sha256),),
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
    _activate(store, ready, checkpoint)
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
    checkpoint = _checkpoint(tmp_path)
    _activate(store, ready, checkpoint)
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


def test_v2_holds_all_zero_audio_without_reserving_order(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    _activate(store, ready, checkpoint)
    source = _source(store, "a" * 32, 10, (_zero_record(100),))

    outcome = finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))

    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert publication_contract_state(database).next_public_order == 0
    with closing(database.open_connection(read_only=True)) as connection:
        assert ready_bundles._sql_one(connection.execute("SELECT count(*) FROM ready_v2_output_stage")) == (0,)


def test_v2_cross_cohort_sentinel_match_then_mismatch_freezes_boundary_at_zero(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    _activate(store, ready, checkpoint)
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
    _activate(store, ready, _checkpoint(tmp_path))
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
    _activate(store, ready, _checkpoint(tmp_path))
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
    _activate(store, ready, _checkpoint(tmp_path))
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
    _publish_and_retire_legacy(store, database, ready, _source(store, "a" * 32, 10, records), 12)
    _activate(store, ready, _checkpoint(tmp_path))
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
    _publish_and_retire_legacy(store, database, ready, _source(store, "a" * 32, 10, records), 12)
    _activate(store, ready, _checkpoint(tmp_path))
    partial = _source(store, "b" * 32, 11, (records[1],))

    with pytest.raises(ReadyBundleError, match="overlaps retired custody"):
        finalize_public_audio_v2(store, (partial,), ready, _options(ClockState(), 12))
    assert publication_contract_state(database).next_public_order == 0


def test_staging_store_quiesces_legacy_then_routes_preserved_source_to_v2(tmp_path: Path) -> None:
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
    assert begin_publication_cutover(store.state_db).mode is ContractMode.QUIESCING
    attempt = store.prepare_streaming_attempt(10, 1)
    attempt.prepare_leg(10, 1)
    attempt.record_read_begin(ReadBeginNotification(10, 1))
    attempt.accept_chunk(10, _record(100))
    attempt.seal(DoneNotification(0, 11))
    source_id = attempt.attempt_id
    attempt.close()
    store.append_ready_closure(11, "drained")

    quiescing = store.recover_and_publish()

    assert quiescing.state is ReadyOutcomeState.WAITING, quiescing.reason
    assert tuple(ready.iterdir()) == ()
    checkpoint = _checkpoint(tmp_path)
    with InternalAudioStore(store.state_db, chunk_limit=config.writer.chunk_records) as audio:
        source = audio.get_source(source_id)
        assert source is not None and source.lifecycle == "sealed_waiting"
        assert activate_public_audio_v2(audio, ready, checkpoint).mode is ContractMode.V2_OPEN

    published = store.recover_and_publish()

    assert published.state is ReadyOutcomeState.PUBLISHED
    assert {item.name for item in published.published[0].path.iterdir()} == {"audio.ogg", "manifest.json"}


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
        _activate(audio_store, ready, checkpoint)
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
    assert published.retirement_pending
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

    retired = store.recover_and_publish()

    assert retired.state is ReadyOutcomeState.WAITING
    assert retired.reason == "ack_retired"
    assert retired.retirement_pending
    assert store.publication_followup_due()
    assert not artifact.path.exists()
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        source_state = ready_bundles._sql_one(
            connection.execute(
                "SELECT lifecycle,retirement_cursor FROM capture_source WHERE source_id=?",
                ("a" * 32,),
            )
        )
    assert source_state == ("retiring", 129)

    followups = 0
    while store.publication_followup_due():
        followups += 1
        assert followups <= 3
        store.recover_and_publish()
    assert followups == 2
    with InternalAudioStore(store.state_db) as audio_store:
        assert audio_store.get_source("a" * 32) is None
        preserved = audio_store.get_source("b" * 32)
        assert preserved is not None and preserved.lifecycle == "sealed_waiting"
