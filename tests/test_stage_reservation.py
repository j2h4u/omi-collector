from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore, InternalAudioStoreError
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    finalize_public_audio_v2,
    publication_contract_state,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from test_public_audio_v2_recovery import _options, _ready_root, _source


def test_scratch_failure_preserves_accepted_cohort_for_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._proof_page_scratch

    @contextmanager
    def fail_scratch(records: Iterable[bytes], *, record_count: int) -> Iterator[tuple[bytes, sqlite3.Connection]]:
        if record_count == 1:
            assert next(iter(records))
            raise RuntimeError("scratch failed")
        with original(records, record_count=record_count) as scratch:
            yield scratch

    try:
        monkeypatch.setattr(ready_bundles, "_proof_page_scratch", fail_scratch)
        with pytest.raises(RuntimeError, match="scratch failed"):
            finalize_public_audio_v2(store, (source,), ready, _options())
        with closing(database.open_connection(read_only=True)) as connection:
            assert ready_bundles._sql_one(connection.execute("SELECT lifecycle,cursor FROM publication_cohort")) == (
                "accepted",
                0,
            )
            assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()[0] == 0
        assert publication_contract_state(database).next_public_order == 0
        monkeypatch.setattr(ready_bundles, "_proof_page_scratch", original)
        assert len(finalize_public_audio_v2(store, (source,), ready, _options()).published) == 1
        assert publication_contract_state(database).next_public_order == 1
    finally:
        store.close()


@pytest.mark.parametrize(
    ("condition", "message"),
    (
        ("order", "publication order is exhausted"),
        ("stage", "another publication stage is active"),
        ("cursor", "cohort changed before stage intent"),
    ),
)
def test_reservation_rechecks_durable_guards_after_proof_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, condition: str, message: str
) -> None:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._proof_page_scratch

    @contextmanager
    def change_durable_state(
        records: Iterable[bytes], *, record_count: int
    ) -> Iterator[tuple[bytes, sqlite3.Connection]]:
        with original(records, record_count=record_count) as scratch:
            if condition == "order":
                database.put(
                    StateKind.PUBLICATION_CONTRACT,
                    "current",
                    {"version": 1, "mode": "v2_open", "next_public_order": 2**64 - 1},
                )
            else:
                with closing(database.open_connection()) as connection:
                    if condition == "cursor":
                        connection.execute("UPDATE publication_cohort SET cursor=1")
                    else:
                        connection.execute(
                            "INSERT INTO ready_output_stage(stage_id,group_identity,owned_path,"
                            "expected_bundle_digest,expected_record_count,expected_slice_count,owner_token) "
                            "VALUES ('competing','competing','/stage',?,1,1,'competing')",
                            ("a" * 64,),
                        )
                    connection.commit()
            yield scratch

    try:
        monkeypatch.setattr(ready_bundles, "_proof_page_scratch", change_durable_state)
        with pytest.raises(ReadyBundleError, match=message):
            finalize_public_audio_v2(store, (source,), ready, _options())
        with closing(database.open_connection(read_only=True)) as connection:
            assert ready_bundles._sql_one(connection.execute("SELECT lifecycle,cursor FROM publication_cohort")) == (
                "accepted",
                int(condition == "cursor"),
            )
            assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM ready_output_stage").fetchone()[0] == int(
                condition == "stage"
            )
        assert publication_contract_state(database).next_public_order == (2**64 - 1 if condition == "order" else 0)
        assert not tuple(ready.iterdir())
    finally:
        store.close()


def test_stage_rollback_with_changed_cohort_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, count=2)
    original = store.publication_transaction
    injected = False

    @contextmanager
    def interrupt_stage():
        nonlocal injected
        try:
            with original() as transaction:
                yield transaction
                _state, connection = transaction
                if connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 1:
                    injected = True
                    raise sqlite3.OperationalError("stage rollback")
        except InternalAudioStoreError:
            with closing(database.open_connection()) as connection:
                connection.execute("UPDATE publication_cohort SET cursor=1")
                connection.commit()
            raise

    try:
        monkeypatch.setattr(store, "publication_transaction", interrupt_stage)
        with pytest.raises(FatalInternalAudioStorageError, match="third state"):
            finalize_public_audio_v2(store, (source,), ready, _options(frontier=12))
        assert injected
        assert store._failed is True
        assert publication_contract_state(database).next_public_order == 0
        with closing(database.open_connection(read_only=True)) as connection:
            assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()[0] == 0
        assert not tuple(ready.iterdir())
    finally:
        store.close()


def test_reservation_streams_each_normalized_record_once_into_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    ready = _ready_root(tmp_path)
    sources = (_source(store, "a" * 32, 10, count=128), _source(store, "b" * 32, 138))
    original = ready_bundles._proof_page_scratch
    passes = 0
    records_seen = 0
    digest = sha256()

    @contextmanager
    def observe_scratch(records: Iterable[bytes], *, record_count: int) -> Iterator[tuple[bytes, sqlite3.Connection]]:
        def observe_records() -> Iterator[bytes]:
            nonlocal passes, records_seen
            passes += 1
            for record in records:
                records_seen += 1
                digest.update(record)
                yield record

        with original(observe_records(), record_count=record_count) as scratch:
            yield scratch

    try:
        monkeypatch.setattr(ready_bundles, "_proof_page_scratch", observe_scratch)
        assert len(finalize_public_audio_v2(store, sources, ready, _options(frontier=139)).published) == 1
        assert (passes, records_seen) == (1, 129)
        with closing(database.open_connection(read_only=True)) as connection:
            assert (
                connection.execute("SELECT records_sha256 FROM ready_replay_proof").fetchone()[0] == digest.hexdigest()
            )
            assert connection.execute("SELECT count(*) FROM ready_replay_proof_page").fetchone()[0] == 2
    finally:
        store.close()
