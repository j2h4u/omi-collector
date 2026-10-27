from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_public_audio_v2,
    publication_contract_state,
    retire_one_covered_typed_source,
)
from omi_collector.capture.application.storage_fault import (
    FatalInternalAudioStorageError,
    TransientInternalAudioStorageError,
)
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.config import ReadyConfig


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


def _source(
    store: InternalAudioStore, source_id: str = "a" * 32, start: int = 10, records: tuple[bytes, ...] | None = None
) -> SqlitePublicationSource:
    raw_records = records or (_record(100),)
    store.create_source(source_id, "attempt", start, start + len(raw_records))
    source = store.prepare_leg(source_id, start, len(raw_records))
    source = store.record_read_begin(source_id, start, len(raw_records))
    store.commit_drain(source_id, 0, raw_records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _options(frontier: int = 11) -> PublicAudioV2Options:
    return PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), frontier, True, 128)


def _counts(database: StateDB) -> tuple[int, int, int, int]:
    with closing(database.open_connection(read_only=True)) as connection:
        counts = (
            connection.execute("SELECT count(*) FROM publication_cohort").fetchone(),
            connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone(),
            connection.execute(
                "SELECT count(*) FROM ready_replay_proof WHERE lifecycle='active' AND format='omi-audio-v2'"
            ).fetchone(),
            connection.execute(
                "SELECT count(*) FROM state WHERE kind=? AND id!='frontier'",
                (StateKind.READY_PUBLICATION.value,),
            ).fetchone(),
        )
    typed_counts = cast(tuple[tuple[int], tuple[int], tuple[int], tuple[int]], counts)
    return tuple(row[0] for row in typed_counts)  # type: ignore[return-value]


def _assert_single_v2_artifact(database: StateDB, ready: Path) -> str:
    artifacts = tuple(path for path in ready.iterdir() if not path.name.startswith("."))
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert {item.name for item in artifact.iterdir()} == {"audio.ogg", "manifest.json"}
    manifest = cast(dict[str, object], json.loads((artifact / "manifest.json").read_text(encoding="utf-8")))
    audio = (artifact / "audio.ogg").read_bytes()
    assert manifest["format"] == "omi-audio-v2"
    assert manifest["id"] == artifact.name
    assert manifest["packet_count"] == 1
    assert manifest["audio_sha256"] == sha256(audio).hexdigest()
    assert _counts(database) == (0, 0, 1, 1)
    ledger = database.get(StateKind.READY_PUBLICATION, artifact.name)
    assert ledger is not None and ledger["state"] == "ready"
    assert publication_contract_state(database).next_public_order == 1
    return artifact.name


def _error(classified_busy: bool) -> sqlite3.DatabaseError | TransientInternalAudioStorageError:
    if classified_busy:
        return TransientInternalAudioStorageError("simulated classified busy", sqlite_errorcode=sqlite3.SQLITE_BUSY)
    return sqlite3.DatabaseError("simulated publication transaction failure")


def _inject_transaction_failure(
    store: InternalAudioStore,
    *,
    transaction_number: int,
    after_commit: bool,
    classified_busy: bool,
    expected_source_id: str | None = None,
) -> tuple[object, tuple[list[int], list[int]]]:
    database = store.state_db
    original = store.publication_transaction
    calls = [0]
    fired = [0]

    @contextmanager
    def injected():
        calls[0] += 1
        if calls[0] != transaction_number or fired[0]:
            with original() as transaction:
                yield transaction
            return
        fired[0] = 1
        if after_commit:
            with original() as transaction:
                yield transaction
            with closing(database.open_connection(read_only=True)) as connection:
                if transaction_number == 1:
                    assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 1
                else:
                    member = cast(
                        tuple[int] | None,
                        connection.execute(
                            "SELECT cursor FROM publication_cohort_member WHERE source_id=?",
                            (expected_source_id,),
                        ).fetchone(),
                    )
                    assert expected_source_id is not None and member is not None and member[0] == 1
            raise _error(classified_busy)
        with original() as transaction:
            yield transaction
            raise _error(classified_busy)

    store.publication_transaction = injected  # type: ignore[method-assign]
    return original, (calls, fired)


@pytest.mark.parametrize(
    "failure_case",
    ((False, False), (False, True), (True, False), (True, True)),
    ids=("rollback-ordinary", "rollback-classified", "committed-ordinary", "committed-classified"),
)
def test_v2_transaction_failures_reconcile_through_publication_api(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    failure_case: tuple[bool, bool],
) -> None:
    database, store = audio_store
    after_commit, classified_busy = failure_case
    ready = _ready_root(tmp_path)
    source = _source(store)
    original, (calls, fired) = _inject_transaction_failure(
        store,
        transaction_number=1,
        after_commit=after_commit,
        classified_busy=classified_busy,
    )

    outcome = finalize_public_audio_v2(store, (source,), ready, _options())
    store.publication_transaction = original  # type: ignore[method-assign]

    assert fired == [1]
    assert calls[0] >= 1
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    artifact_id = _assert_single_v2_artifact(database, ready)

    for _ in range(4):
        with closing(database.open_connection(read_only=True)) as connection:
            remaining = cast(
                int,
                connection.execute(
                    "SELECT count(*) FROM capture_source WHERE source_id=?", (source.source_id,)
                ).fetchone()[0],
            )
        if remaining == 0:
            break
        assert retire_one_covered_typed_source(store, 128)

    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            cast(
                int,
                connection.execute(
                    "SELECT count(*) FROM capture_source WHERE source_id=?", (source.source_id,)
                ).fetchone()[0],
            )
            == 0
        )
    assert _assert_single_v2_artifact(database, ready) == artifact_id


@pytest.mark.parametrize(
    "failure_case",
    ((False, False), (False, True), (True, False), (True, True)),
    ids=("rollback-ordinary", "rollback-classified", "committed-ordinary", "committed-classified"),
)
def test_v2_ready_custody_failure_reconciles_and_publishes_unique_suffix(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    failure_case: tuple[bool, bool],
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    first = _source(store, "a" * 32, 10, (_record(100),))
    assert finalize_public_audio_v2(store, (first,), ready, _options()).state is ReadyOutcomeState.PUBLISHED
    replay = _source(store, "b" * 32, 10, (_record(100), _record(101, 2)))
    after_commit, classified_busy = failure_case
    original, (calls, fired) = _inject_transaction_failure(
        store,
        transaction_number=2,
        after_commit=after_commit,
        classified_busy=classified_busy,
        expected_source_id=replay.source_id,
    )

    outcome = finalize_public_audio_v2(store, (replay,), ready, _options(12))
    store.publication_transaction = original  # type: ignore[method-assign]

    assert fired == [1]
    assert calls[0] >= 2
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    artifacts = tuple(path for path in ready.iterdir() if not path.name.startswith("."))
    assert len(artifacts) == 2
    manifests = tuple(
        cast(dict[str, object], json.loads((artifact / "manifest.json").read_text(encoding="utf-8")))
        for artifact in artifacts
    )
    packet_counts = tuple(cast(int, manifest["packet_count"]) for manifest in manifests)
    assert sorted(packet_counts) == [1, 1]
    assert _counts(database) == (0, 0, 2, 2)
    assert publication_contract_state(database).next_public_order == 2

    for _ in range(4):
        with closing(database.open_connection(read_only=True)) as connection:
            remaining = cast(
                int,
                connection.execute(
                    "SELECT count(*) FROM capture_source WHERE source_id=?", (replay.source_id,)
                ).fetchone()[0],
            )
        if remaining == 0:
            break
        assert retire_one_covered_typed_source(store, 128)
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            cast(
                int,
                connection.execute(
                    "SELECT count(*) FROM capture_source WHERE source_id=?", (replay.source_id,)
                ).fetchone()[0],
            )
            == 0
        )


def test_v2_committed_third_cohort_state_fails_closed(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    first = _source(store, "a" * 32, 10, (_record(100),))
    assert finalize_public_audio_v2(store, (first,), ready, _options()).state is ReadyOutcomeState.PUBLISHED
    source = _source(store, "b" * 32, 10, (_record(100), _record(101, 2)))
    original = store.publication_transaction
    calls = 0

    @contextmanager
    def mutate_after_cohort_commit():
        nonlocal calls
        calls += 1
        with original() as transaction:
            yield transaction
        if calls == 2:
            with closing(database.open_connection()) as connection:
                cursor = connection.execute(
                    "UPDATE publication_cohort_member SET cursor=2 WHERE source_id=?", (source.source_id,)
                )
                assert cursor.rowcount == 1
                connection.commit()
                persisted = cast(
                    tuple[int] | None,
                    connection.execute(
                        "SELECT cursor FROM publication_cohort_member WHERE source_id=?", (source.source_id,)
                    ).fetchone(),
                )
                assert persisted is not None and persisted[0] == 2
            raise sqlite3.DatabaseError("simulated ambiguous cohort state")

    store.publication_transaction = mutate_after_cohort_commit  # type: ignore[method-assign]

    with pytest.raises(FatalInternalAudioStorageError, match="third state"):
        finalize_public_audio_v2(store, (source,), ready, _options(12))

    assert calls == 2
    assert store._failed is True
    assert len(tuple(path for path in ready.iterdir() if not path.name.startswith("."))) == 1
