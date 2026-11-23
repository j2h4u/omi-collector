from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
)


def _source_bytes(connection: sqlite3.Connection, source_id: str) -> bytes:
    rows = cast(
        list[tuple[int, bytes]],
        connection.execute(
            "SELECT device_timestamp,payload FROM capture_record WHERE source_id=? ORDER BY ordinal", (source_id,)
        ).fetchall(),
    )
    return b"".join(timestamp.to_bytes(4, "big") + payload for timestamp, payload in rows)


def _observation_bytes(connection: sqlite3.Connection, source_id: str, leg_generation: int) -> bytes:
    row = cast(
        tuple[bytes, int, bytes] | None,
        connection.execute(
            "SELECT read_start,prefix_record_count,tail FROM capture_observation WHERE source_id=? AND leg_generation=?",
            (source_id, leg_generation),
        ).fetchone(),
    )
    assert row is not None
    source_start = cast(
        tuple[bytes],
        connection.execute("SELECT sequence_start FROM capture_source WHERE source_id=?", (source_id,)).fetchone(),
    )[0]
    first = int.from_bytes(row[0], "big") - int.from_bytes(source_start, "big")
    prefix = _source_bytes(connection, source_id)[first * 444 : (first + row[1]) * 444]
    chunks = cast(
        list[tuple[bytes]],
        connection.execute(
            "SELECT bytes FROM capture_observation_chunk WHERE source_id=? AND leg_generation=? ORDER BY byte_offset",
            (source_id, leg_generation),
        ).fetchall(),
    )
    return prefix + b"".join(item[0] for item in chunks) + row[2]


def test_corrupt_drain_retains_raw_bytes_and_freezes_numbered_frontier(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "a" * 32
    good = bytes(444)
    bad = bytes([255]) * 444
    tail = b"partial"
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 0, 5)
        store.prepare_leg(source_id, 0, 5)
        store.record_read_begin(source_id, 0, 5)
        assert store.commit_checked_drain(source_id, 0, (good, bad, good), DrainCheckpoint(1)) == 1
        checkpoint = DrainCheckpoint(1, CaptureTail(1, 4, 4 * 444 + len(tail), 4 * 444, tail))
        assert store.commit_checked_drain(source_id, 3, (good,), checkpoint) == 1
        source = store.get_source(source_id)
        assert source is not None and source.committed_count == 1
        assert tuple(row.raw_bytes for row in store.read_record_page(source_id, limit=8)) == (good,)
    with closing(sqlite3.connect(db.path)) as connection:
        observation = cast(
            tuple[object, object, object] | None,
            connection.execute("SELECT fault_offset, received_bytes, tail FROM capture_observation").fetchone(),
        )
        chunks = connection.execute(
            "SELECT byte_offset, bytes FROM capture_observation_chunk ORDER BY byte_offset"
        ).fetchall()
    assert observation == (444, 4 * 444 + len(tail), tail)
    assert chunks == [(444, bad + good), (3 * 444, good)]


def test_corrupt_transition_rolls_back_prefix_and_retains_previous_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "b" * 32
    good = bytes(444)
    bad = bytes([255]) * 444
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 0, 3)
        store.prepare_leg(source_id, 0, 3)
        store.record_read_begin(source_id, 0, 3)
        store.commit_tail(source_id, 0, DrainCheckpoint(1, CaptureTail(1, 0, 10, 0, good[:10])))
        original = store._insert_observation

        def fail(*_args: object) -> None:
            raise sqlite3.OperationalError("injected failure")

        monkeypatch.setattr(store, "_insert_observation", fail)
        with pytest.raises(InternalAudioStoreError, match="observation transaction was not confirmed durable"):
            store.commit_checked_drain(source_id, 0, (good, bad), DrainCheckpoint(1))
        source = store.get_source(source_id)
        assert source is not None and source.committed_count == 0
        monkeypatch.setattr(store, "_insert_observation", original)
        assert store.commit_checked_drain(source_id, 0, (good, bad), DrainCheckpoint(1)) == 1
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_tail").fetchone() == (0,)
        assert connection.execute("SELECT bytes FROM capture_observation_chunk").fetchone() == (bad,)


def test_short_done_demotes_provisional_records_and_keeps_exact_bytes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "c" * 32
    good = bytes(444)
    tail = b"part"
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        checkpoint = DrainCheckpoint(1, CaptureTail(1, 2, 2 * 444 + len(tail), 2 * 444, tail))
        assert store.commit_checked_drain(source_id, 0, (good, good), checkpoint) == 2
        assert store.finish_read(source_id, 103, 2 * 444 + len(tail)) is False
        source = store.get_source(source_id)
        assert source is not None and source.committed_count == 2
        assert store.confirmed_count(source_id) == 0
        assert tuple(row.raw_bytes for row in store.read_record_page(source_id, limit=8)) == (good, good)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT confirmed_damage FROM capture_framing_incident").fetchone() == (0,)
        assert connection.execute("SELECT tail,done FROM capture_observation").fetchone() == (tail, 1)
        assert _observation_bytes(connection, source_id, 1) == good + good + tail


def test_full_done_confirms_healthy_bytes_and_never_confirms_corrupt_bytes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    good = bytes(444)
    bad = bytes([255]) * 444
    with InternalAudioStore(db, chunk_limit=8) as store:
        healthy, damaged = "d" * 32, "e" * 32
        for source_id in (healthy, damaged):
            store.create_source(source_id, "attempt", 10, 12)
            store.prepare_leg(source_id, 10, 2)
            store.record_read_begin(source_id, 10, 2)
        store.commit_checked_drain(healthy, 0, (good, good), DrainCheckpoint(1))
        store.commit_checked_drain(damaged, 0, (good, bad), DrainCheckpoint(1))
        assert store.confirmed_count(healthy) == 0
        assert store.confirmed_count(damaged) == 0
        assert store.finish_read(healthy, 12, 888) is True
        assert store.finish_read(damaged, 12, 888) is False
        assert store.confirmed_count(healthy) == 2
        assert store.confirmed_count(damaged) == 0
        source = store.get_source(damaged)
        assert source is not None and source.committed_count == 1
    with closing(sqlite3.connect(db.path)) as connection:
        assert _observation_bytes(connection, damaged, 1) == good + bad


def test_device_head_settles_unknown_gap_without_deleting_evidence(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier, settle_unavailable

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "f" * 32
    bad = bytes([255]) * 444
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (bad,), DrainCheckpoint(1))
        assert store.finish_read(source_id, 102, 444) is False
    assert active_framing_frontier(db) == 100
    assert settle_unavailable(db, 100, 102) == ()
    assert active_framing_frontier(db) == 100
    assert settle_unavailable(db, 101, 102) == (source_id,)
    assert active_framing_frontier(db) is None
    assert settle_unavailable(db, 101, 102) == ()
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT bytes FROM capture_observation_chunk").fetchone() == (bad,)
        assert connection.execute("SELECT settlement_reason FROM capture_framing_incident").fetchone() == (
            "device_head_passed_unresolved_frontier",
        )


def test_unavailable_interrupted_read_preserves_provisional_bytes_without_numbering(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "1" * 32
    good = bytes(444)
    tail = b"partial"
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 104)
        store.prepare_leg(source_id, 100, 4)
        store.record_read_begin(source_id, 100, 4)
        store.commit_checked_drain(
            source_id, 0, (good, good), DrainCheckpoint(1, CaptureTail(1, 2, 888 + len(tail), 888, tail))
        )
        store.interrupt(source_id)
    with InternalAudioStore(db, chunk_limit=8) as store:
        assert store.quarantine_unconfirmed(source_id) == 0
        source = store.get_source(source_id)
        assert source is not None and source.source_type == "quarantine" and source.committed_count == 2
    with closing(sqlite3.connect(db.path)) as connection:
        assert _source_bytes(connection, source_id) == good + good
        assert connection.execute("SELECT bytes FROM capture_tail").fetchone() == (tail,)
        assert connection.execute("SELECT confirmed_damage FROM capture_framing_incident").fetchone() == (0,)


def test_only_full_independent_read_settles_recoverable_incident(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier, settle_recovered

    db = StateDB(tmp_path / "state.sqlite3")
    old, new = "2" * 32, "3" * 32
    good = bytes(444)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(old, "attempt", 100, 102)
        store.prepare_leg(old, 100, 2)
        store.record_read_begin(old, 100, 2)
        store.commit_checked_drain(old, 0, (good,), DrainCheckpoint(1))
        assert not store.finish_read(old, 102, 444)
        store.create_source(new, "attempt", 100, 102)
        store.prepare_leg(new, 100, 2)
        store.record_read_begin(new, 100, 2)
        store.commit_checked_drain(new, 0, (good, good), DrainCheckpoint(1))
        assert settle_recovered(db, new) == ()
        assert store.finish_read(new, 102, 888)
        assert settle_recovered(db, new) == ()
        source = store.get_source(new)
        assert source is not None
        store.seal_source(new, leg_generation=1, expected_count=2, expected_sha256=source.committed_sha256)
    assert settle_recovered(db, old) == ()
    from staging_test_factory import StagingStore

    staging = StagingStore(tmp_path / "spool", tmp_path / "capture", state_db=db)
    assert staging.reconcile_sealed_framing() == (old,)
    assert staging.reconcile_sealed_framing() == ()
    assert active_framing_frontier(db) is None
    with closing(sqlite3.connect(db.path)) as connection:
        assert _observation_bytes(connection, old, 1) == good


def test_a_read_completed_before_an_incident_cannot_be_recovery(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier, settle_recovered

    db = StateDB(tmp_path / "state.sqlite3")
    old_read, incident = "4" * 32, "5" * 32
    good = bytes(444)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(old_read, "attempt", 100, 102)
        store.prepare_leg(old_read, 100, 2)
        store.record_read_begin(old_read, 100, 2)
        store.commit_checked_drain(old_read, 0, (good, good), DrainCheckpoint(1))
        assert store.finish_read(old_read, 102, 888)
        source = store.get_source(old_read)
        assert source is not None
        store.seal_source(old_read, leg_generation=1, expected_count=2, expected_sha256=source.committed_sha256)
        store.create_source(incident, "attempt", 100, 102)
        store.prepare_leg(incident, 100, 2)
        store.record_read_begin(incident, 100, 2)
        store.commit_checked_drain(incident, 0, (good,), DrainCheckpoint(1))
        assert not store.finish_read(incident, 102, 444)
    assert settle_recovered(db, old_read) == ()
    assert active_framing_frontier(db) == 100


def test_actual_done_is_available_with_confirmed_prefix_after_reopen(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.attempts import _durable_prefix

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "6" * 32
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 101)
        store.prepare_leg(source_id, 100, 1)
        store.record_read_begin(source_id, 100, 1)
        store.commit_checked_drain(source_id, 0, (bytes(444),), DrainCheckpoint(1))
        source = store.get_source(source_id)
        assert source is not None
        provisional = _durable_prefix(source, db)
        assert provisional.done_next is None and provisional.has_unconfirmed
        assert store.finish_read(source_id, 101, 444)
    with InternalAudioStore(db, chunk_limit=8) as store:
        source = store.get_source(source_id)
        assert source is not None
        prefix = _durable_prefix(source, db)
        assert (prefix.next_sequence, prefix.done_next, prefix.has_unconfirmed) == (101, 101, False)


def test_divergent_replay_preserves_both_reads_and_accepts_remaining_chunks(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "7" * 32
    first = bytes(444)
    original = (1).to_bytes(4, "big") + bytes(440)
    different = (2).to_bytes(4, "big") + bytes(440)
    last = (3).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (first, original), DrainCheckpoint(1))
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (first, different), DrainCheckpoint(2))
        store.commit_checked_drain(source_id, 2, (last,), DrainCheckpoint(2))
        assert not store.finish_read(source_id, 103, 3 * 444)
    with closing(sqlite3.connect(db.path)) as connection:
        chunks = connection.execute(
            "SELECT byte_offset, bytes FROM capture_observation_chunk ORDER BY byte_offset"
        ).fetchall()
        assert chunks == [(444, different), (888, last)]
        assert _source_bytes(connection, source_id) == first + original
        assert _observation_bytes(connection, source_id, 2) == first + different + last
        assert connection.execute("SELECT confirmed_damage FROM capture_framing_incident").fetchone() == (0,)


def test_divergent_replay_tail_retains_both_partial_observations(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "8" * 32
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_tail(source_id, 0, DrainCheckpoint(1, CaptureTail(1, 0, 3, 0, b"old")))
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_tail(source_id, 0, DrainCheckpoint(2, CaptureTail(2, 0, 3, 0, b"new")))
        assert not store.finish_read(source_id, 102, 3)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT tail FROM capture_observation").fetchone() == (b"new",)
        assert connection.execute("SELECT bytes FROM capture_tail").fetchone() == (b"old",)


def test_interrupted_read_can_be_confirmed_by_full_matching_retry(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.attempts import _durable_prefix

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "9" * 32
    first = bytes(444)
    second = (1).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (first,), DrainCheckpoint(1))
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (first, second), DrainCheckpoint(2))
        assert store.finish_read(source_id, 102, 888)
        source = store.get_source(source_id)
        assert source is not None
        prefix = _durable_prefix(source, db)
        assert (prefix.next_sequence, prefix.done_next, prefix.has_unconfirmed) == (102, 102, False)


def test_repeated_visit_close_never_publishes_unconfirmed_prefix(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    writer.interrupt()
    source_id = writer.attempt_id
    writer.close()
    assert store.close_pending_prefix("absence") is None
    assert store.close_orphaned_drafts("absence") is None
    assert store.close_pending_prefix("absence") is None
    source = store.open_attempt(source_id)
    from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance

    assert (
        QuarantineMaintenance._durable_sql_frontier(source.descriptor, source.recover(), source.durable_prefix) == 100
    )
    assert source.durable_prefix.record_count == 0
    assert source.durable_prefix.has_unconfirmed
    assert source.recover().valid_records == 1


def test_explicit_prefix_close_preserves_unconfirmed_bytes_as_evidence(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    source_id = writer.attempt_id
    assert writer.publish_prefix() is None
    writer.close()
    with closing(sqlite3.connect(store.state_db.path)) as connection:
        assert connection.execute(
            "SELECT source_type,committed_count FROM capture_source WHERE source_id=?", (source_id,)
        ).fetchone() == ("quarantine", 1)
        assert _source_bytes(connection, source_id) == bytes(444)


def test_direct_seal_requires_durable_real_done(tmp_path: Path) -> None:
    import hashlib

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "a1" * 16
    raw = bytes(444)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 101)
        store.prepare_leg(source_id, 100, 1)
        store.record_read_begin(source_id, 100, 1)
        store.commit_checked_drain(source_id, 0, (raw,), DrainCheckpoint(1))
        with pytest.raises(InternalAudioStoreError, match="no matching verified READ completion"):
            store.seal_source(source_id, leg_generation=1, expected_count=1, expected_sha256=digest)
        assert store.finish_read(source_id, 101, 444)
        assert store.seal_source(source_id, leg_generation=1, expected_count=1, expected_sha256=digest).lifecycle == (
            "sealed_waiting"
        )


def test_direct_terminalization_refuses_unconfirmed_records(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_contract import AttemptStateError
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    writer.interrupt()
    source_id = writer.attempt_id
    writer.close()
    with pytest.raises(AttemptStateError, match="unconfirmed rows"):
        store.terminalize_prefix_attempt(source_id)
    assert store.open_attempt(source_id).durable_prefix.has_unconfirmed


def test_generic_quarantine_preserves_provisional_data_before_classification(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    writer.interrupt()
    source_id = writer.attempt_id
    writer.close()
    assert store.quarantine_pending("storage reconciliation") == (source_id,)
    with closing(sqlite3.connect(store.state_db.path)) as connection:
        assert connection.execute("SELECT source_type,committed_count FROM capture_source").fetchone() == (
            "quarantine",
            1,
        )
        assert _source_bytes(connection, source_id) == bytes(444)


def test_provisional_rows_do_not_authorize_a_mid_range_read(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "b1" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (bytes(444),), DrainCheckpoint(1))
        store.interrupt(source_id)
        with pytest.raises(InternalAudioStoreError, match="confirmed frontier"):
            store.prepare_leg(source_id, 101, 1)


def test_shorter_completed_retry_does_not_replace_full_read_evidence(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.attempts import _durable_prefix

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "c1" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (bytes(444), bytes(444)), DrainCheckpoint(1))
        assert store.finish_read(source_id, 102, 888)
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 1)
        store.record_read_begin(source_id, 100, 1)
        store.commit_checked_drain(source_id, 0, (bytes(444),), DrainCheckpoint(2))
        assert store.finish_read(source_id, 101, 444)
        source = store.get_source(source_id)
        assert source is not None
        prefix = _durable_prefix(source, db)
        assert (prefix.next_sequence, prefix.done_next) == (102, 102)


def test_raw_quarantine_cannot_certify_provisional_rows(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.quarantine import quarantine_pending
    from omi_collector.capture.adapters.staging_contract import AttemptStateError
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    writer.interrupt()
    source_id = writer.attempt_id
    writer.close()
    with pytest.raises(AttemptStateError, match="separate quarantine custody"):
        quarantine_pending(store._filesystem, "test")
    assert store.open_attempt(source_id).durable_prefix.has_unconfirmed


def test_later_head_advances_extend_one_unknown_gap_without_exact_loss(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import (
        extend_uncertain_framing,
        has_framing_uncertainty,
        settle_unavailable,
    )

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "d1" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (bytes([255]) * 444,), DrainCheckpoint(1))
        assert not store.finish_read(source_id, 103, 444)
    assert settle_unavailable(db, 101, 103) == (source_id,)
    assert has_framing_uncertainty(db, 101, 102)
    assert extend_uncertain_framing(db, 101, 102, 103) == (source_id,)
    assert extend_uncertain_framing(db, 101, 102, 103) == ()
    assert has_framing_uncertainty(db, 102, 103)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT count(*),hex(settled_head) FROM capture_framing_incident").fetchone() == (
            1,
            "0000000000000066",
        )


def test_two_complete_conflicting_reads_cannot_be_automatically_reconciled(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier, settle_recovered

    db = StateDB(tmp_path / "state.sqlite3")
    original, third = "e1" * 16, "e2" * 16
    first = bytes(444)
    different = (1).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(original, "attempt", 100, 102)
        store.prepare_leg(original, 100, 2)
        store.record_read_begin(original, 100, 2)
        store.commit_checked_drain(original, 0, (first, first), DrainCheckpoint(1))
        assert store.finish_read(original, 102, 888)
        store.interrupt(original)
        store.prepare_leg(original, 100, 2)
        store.record_read_begin(original, 100, 2)
        store.commit_checked_drain(original, 0, (different, first), DrainCheckpoint(2))
        assert not store.finish_read(original, 102, 888)
        store.create_source(third, "attempt", 100, 102)
        store.prepare_leg(third, 100, 2)
        store.record_read_begin(third, 100, 2)
        store.commit_checked_drain(third, 0, (first, first), DrainCheckpoint(1))
        assert store.finish_read(third, 102, 888)
        source = store.get_source(third)
        assert source is not None
        store.seal_source(third, leg_generation=1, expected_count=2, expected_sha256=source.committed_sha256)
    assert active_framing_frontier(db) == 100
    assert settle_recovered(db, third) == ()
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute(
            "SELECT verified_conflict,conflict_candidate FROM capture_framing_incident WHERE incident_id=?", (original,)
        ).fetchone() == (1, 1)
        assert connection.execute(
            "SELECT block_reason FROM capture_source WHERE source_id=?", (original,)
        ).fetchone() == ("ready_overlap_conflict",)
        assert connection.execute(
            "SELECT bytes FROM capture_observation_chunk WHERE source_id=?", (original,)
        ).fetchone() == (different + first,)


def test_verified_read_evidence_requires_a_matching_done_and_digest(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "f1" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 101)
        store.prepare_leg(source_id, 100, 1)
    with closing(sqlite3.connect(db.path)) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE capture_read_leg SET verified_count=1 WHERE source_id=?", (source_id,))


def test_multiple_conflicts_keep_earliest_verified_frontier(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "f2" * 16
    good = bytes(444)
    different = (1).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (good, good, good), DrainCheckpoint(1))
        assert store.finish_read(source_id, 103, 1332)
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (good, good, different), DrainCheckpoint(2))
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (different, good, good), DrainCheckpoint(3))
        assert not store.finish_read(source_id, 103, 1332)
    assert active_framing_frontier(db) == 100


def test_quarantine_rejects_empty_reason_before_moving_any_evidence(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_contract import AttemptStateError
    from omi_collector.capture.adapters.staging_writer import StagingWriter
    from omi_collector.capture.domain.ring_protocol import ReadBeginNotification
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    writer = StagingWriter(store, 100, 2)
    writer.prepare()
    writer.prepare_leg(100, 2)
    writer.read_begin(ReadBeginNotification(100, 2))
    writer.append_chunk(0, memoryview(bytes(444)))
    writer.interrupt()
    source_id = writer.attempt_id
    writer.close()
    with pytest.raises(AttemptStateError, match="reason"):
        store.quarantine_pending("")
    assert store.open_attempt(source_id).durable_prefix.has_unconfirmed


def test_short_conflicting_replay_is_not_a_second_verified_claim(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "f3" * 16
    good = bytes(444)
    different = (1).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (good, good), DrainCheckpoint(1))
        assert store.finish_read(source_id, 102, 888)
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_checked_drain(source_id, 0, (different,), DrainCheckpoint(2))
        assert not store.finish_read(source_id, 102, 444)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute(
            "SELECT conflict_candidate,verified_conflict FROM capture_framing_incident"
        ).fetchone() == (1, 0)
        assert connection.execute(
            "SELECT block_reason FROM capture_source WHERE source_id=?", (source_id,)
        ).fetchone() == ("framing_unresolved",)
        assert connection.execute("SELECT bytes FROM capture_observation_chunk").fetchone() == (different,)


def test_quarantine_certifies_verified_prefix_without_numbering_physical_suffix(tmp_path: Path) -> None:
    from staging_test_factory import StagingStore

    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    db = store.state_db
    source_id = "f4" * 16
    good, bad = bytes(4) + bytes((2, 8, 1)) + bytes(437), bytes([255]) * 444
    with InternalAudioStore(db, chunk_limit=8) as audio:
        audio.create_source(source_id, "attempt", 100, 104)
        audio.prepare_leg(source_id, 100, 4)
        audio.record_read_begin(source_id, 100, 4)
        audio.interrupt(source_id)
        audio.prepare_leg(source_id, 100, 1)
        audio.record_read_begin(source_id, 100, 1)
        audio.commit_checked_drain(source_id, 0, (good,), DrainCheckpoint(2))
        assert audio.finish_read(source_id, 101, 444)
        audio.interrupt(source_id)
        audio.prepare_leg(source_id, 101, 3)
        audio.record_read_begin(source_id, 101, 3)
        audio.commit_checked_drain(source_id, 1, (good, bad, good), DrainCheckpoint(3))
        assert not audio.finish_read(source_id, 104, 1332)
    admitted = store.admit_quarantined_prefix(source_id)
    assert (admitted.start_sequence, admitted.next_sequence, admitted.record_count) == (100, 101, 1)
    with closing(sqlite3.connect(db.path)) as connection:
        assert _source_bytes(connection, source_id) == good + good
        assert _observation_bytes(connection, source_id, 3) == good + bad + good
        assert connection.execute(
            "SELECT committed_count,certified_prefix_count FROM capture_source WHERE source_id=?", (source_id,)
        ).fetchone() == (2, 1)

    from omi_collector.capture.adapters.ready_bundles import (
        PublicAudioV2Options,
        ReadyOutcomeState,
        SqlitePublicationSource,
        finalize_public_audio_v2,
    )
    from omi_collector.capture.domain.clock_state import ClockState
    from omi_collector.config import ReadyConfig

    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    with InternalAudioStore(db, chunk_limit=8) as audio:
        outcome = finalize_public_audio_v2(
            audio,
            (SqlitePublicationSource(audio, source_id),),
            ready,
            PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), 101, True, 8),
        )
        assert outcome.state is ReadyOutcomeState.PUBLISHED
    with closing(sqlite3.connect(db.path)) as connection:
        assert _source_bytes(connection, source_id) == good + good
        assert _observation_bytes(connection, source_id, 3) == good + bad + good

    with closing(sqlite3.connect(db.path)) as connection:
        connection.execute("UPDATE capture_source SET retention_deadline='0' WHERE source_id=?", (source_id,))
        connection.commit()
    store.sweep_terminal_quarantine()
    with closing(sqlite3.connect(db.path)) as connection:
        assert _source_bytes(connection, source_id) == good + good
        assert _observation_bytes(connection, source_id, 3) == good + bad + good


def test_interrupted_malformed_read_keeps_exact_observation_after_restart(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "f5" * 16
    good, bad, tail = bytes(444), bytes([255]) * 444, b"tail"
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(
            source_id, 0, (good, bad), DrainCheckpoint(1, CaptureTail(1, 2, 888 + len(tail), 888, tail))
        )
        store.interrupt(source_id)
    with InternalAudioStore(db, chunk_limit=8) as store:
        assert store.quarantine_unconfirmed(source_id) == 0
    with closing(sqlite3.connect(db.path)) as connection:
        assert _observation_bytes(connection, source_id, 1) == good + bad + tail
        assert connection.execute("SELECT done FROM capture_observation").fetchone() == (0,)
        assert connection.execute("SELECT source_type,lifecycle FROM capture_source").fetchone() == (
            "quarantine",
            "blocked",
        )


def test_retirement_preserves_observation_overlapping_published_prefix(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.ready_bundles import (
        PublicAudioV2Options,
        SqlitePublicationSource,
        finalize_public_audio_v2,
        retire_one_covered_typed_source,
    )
    from omi_collector.capture.adapters.sqlite_compaction import pending_compaction_generation, vacuum_pending
    from omi_collector.capture.domain.clock_state import ClockState
    from omi_collector.config import ReadyConfig
    from staging_test_factory import StagingStore

    staging = StagingStore(tmp_path / "spool", tmp_path / "capture")
    db, source_id = staging.state_db, "f5" * 16
    good = bytes(4) + bytes((2, 8, 1)) + bytes(437)
    with InternalAudioStore(db, chunk_limit=8) as audio:
        audio.create_source(source_id, "attempt", 100, 102)
        audio.prepare_leg(source_id, 100, 2)
        audio.record_read_begin(source_id, 100, 2)
        audio.commit_checked_drain(source_id, 0, (good, good), DrainCheckpoint(1))
        assert audio.finish_read(source_id, 102, 888)
        audio.interrupt(source_id)
        audio.prepare_leg(source_id, 100, 2)
        audio.record_read_begin(source_id, 100, 2)
        audio.commit_checked_drain(source_id, 0, (good,), DrainCheckpoint(2))
        assert not audio.finish_read(source_id, 102, 444)
    staging.admit_quarantined_prefix(source_id)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    with InternalAudioStore(db, chunk_limit=8) as audio:
        outcome = finalize_public_audio_v2(
            audio,
            (SqlitePublicationSource(audio, source_id),),
            ready,
            PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), 102, True, 8),
        )
        assert outcome.published
        source = audio.get_source(source_id)
        assert source is not None and source.lifecycle == "retiring"
        assert not retire_one_covered_typed_source(audio, 8)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute(
            "SELECT prefix_record_count FROM capture_observation WHERE source_id=? AND leg_generation=2", (source_id,)
        ).fetchone() == (1,)
        assert _source_bytes(connection, source_id) == good + good
        assert _observation_bytes(connection, source_id, 2) == good
        connection.execute("CREATE TABLE compaction_fixture (payload BLOB)")
        connection.executemany("INSERT INTO compaction_fixture VALUES (?)", [(bytes(3500),)] * 100)
        connection.commit()
        connection.execute("DROP TABLE compaction_fixture")
        connection.commit()
    db.put(StateKind.COMPACTION, "current", {"generation": 5, "pending": True})
    assert pending_compaction_generation(db) == 5
    assert vacuum_pending(db, 5, threading.Event()) == "done"
    with closing(sqlite3.connect(db.path)) as connection:
        assert _source_bytes(connection, source_id) == good + good
        assert _observation_bytes(connection, source_id, 2) == good


def test_damaged_retry_retains_both_observations_under_one_incident(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import settle_recovered

    db = StateDB(tmp_path / "state.sqlite3")
    original, retry, recovered = ("a1" * 16, "a2" * 16, "a3" * 16)
    bad = bytes([255]) * 444
    with InternalAudioStore(db, chunk_limit=8) as store:
        for source_id in (original, retry):
            store.create_source(source_id, "attempt", 100, 102)
            store.prepare_leg(source_id, 100, 2)
            store.record_read_begin(source_id, 100, 2)
            store.commit_checked_drain(source_id, 0, (bad,), DrainCheckpoint(1))
            assert not store.finish_read(source_id, 102, 444)
        store.create_source(recovered, "attempt", 100, 102)
        store.prepare_leg(recovered, 100, 2)
        store.record_read_begin(recovered, 100, 2)
        store.commit_checked_drain(recovered, 0, (bytes(444), bytes(444)), DrainCheckpoint(1))
        assert store.finish_read(recovered, 102, 888)
        source = store.get_source(recovered)
        assert source is not None
        store.seal_source(recovered, leg_generation=1, expected_count=2, expected_sha256=source.committed_sha256)
    assert settle_recovered(db, recovered) == (original,)
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT incident_id,status FROM capture_framing_incident").fetchall() == [
            (original, "settled")
        ]
        assert connection.execute(
            "SELECT source_id,incident_id FROM capture_observation ORDER BY source_id"
        ).fetchall() == [(original, original), (retry, original)]
        assert _observation_bytes(connection, original, 1) == bad
        assert _observation_bytes(connection, retry, 1) == bad


def test_incomplete_conflict_does_not_move_verified_conflict_frontier(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.framing_incidents import active_framing_frontier

    db = StateDB(tmp_path / "state.sqlite3")
    source_id = "a4" * 16
    good = bytes(444)
    different = (1).to_bytes(4, "big") + bytes(440)
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 100, 103)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (good, good, good), DrainCheckpoint(1))
        assert store.finish_read(source_id, 103, 1332)
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (different,), DrainCheckpoint(2))
        store.interrupt(source_id)
        store.prepare_leg(source_id, 100, 3)
        store.record_read_begin(source_id, 100, 3)
        store.commit_checked_drain(source_id, 0, (good, good, different), DrainCheckpoint(3))
        assert not store.finish_read(source_id, 103, 1332)
    assert active_framing_frontier(db) == 102
