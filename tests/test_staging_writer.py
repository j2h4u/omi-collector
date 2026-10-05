from __future__ import annotations

import os
import threading
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.staging_writer import StagingWriter, StagingWriterStateError, ThreadAffinityError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, _capture_root(tmp_path))


def _record(value: int) -> bytes:
    return value.to_bytes(4, "big") + bytes(RECORD_SIZE - 4)


def _begin(writer: StagingWriter, start: int = 100, count: int = 2) -> None:
    writer.prepare()
    writer.read_begin(ReadBeginNotification(start, count))


def test_construction_does_not_touch_disk(tmp_path: Path) -> None:
    root = tmp_path / "staging"

    writer = StagingWriter(StagingStore(root, _capture_root(tmp_path)), 100, 2)

    assert not root.exists()
    writer.close()
    assert not root.exists()


def test_writer_maps_arena_offsets_and_owns_streaming_mutations(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 2)
    first = _record(1)
    second = _record(2)

    _begin(writer)
    writer.append_chunk(0, memoryview(first + second))
    prefix = writer.checkpoint()
    assert prefix.record_count == 2
    assert prefix.start_sequence == 100
    assert prefix.next_sequence == 102
    result = writer.seal(DoneNotification(0, 102))
    writer.close()

    assert result.bundle_path.joinpath("records.bin").read_bytes() == first + second
    assert result.deduplicated is False


def test_append_chunk_rejects_unaligned_or_out_of_range_data(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 2)
    _begin(writer)

    with pytest.raises(ValueError, match="offset"):
        writer.append_chunk(1, memoryview(_record(1)))
    with pytest.raises(ValueError, match="positive multiple"):
        writer.append_chunk(0, memoryview(b"short"))
    with pytest.raises(ValueError, match="exceeds"):
        writer.append_chunk(0, memoryview(_record(1) * 3))
    writer.close()


def test_prepare_resumes_partial_and_replays_from_checkpoint(tmp_path: Path) -> None:
    first = _record(1)
    second = _record(2)
    initial = StagingWriter(_store(tmp_path), 100, 2)
    _begin(initial)
    initial.append_chunk(0, memoryview(first))
    initial.checkpoint()
    initial.close()

    resumed = StagingWriter(_store(tmp_path), 100, 2)
    prefix = resumed.prepare_leg(100, 2)
    assert prefix.next_sequence == 101
    resumed.read_begin(ReadBeginNotification(100, 2))
    resumed.append_chunk(0, memoryview(first + second))
    resumed.checkpoint()
    result = resumed.seal(DoneNotification(0, 102))
    resumed.close()

    assert result.bundle_path.joinpath("records.bin").read_bytes() == first + second


def test_read_begin_rebinds_original_range_after_recovery_read_started(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 3)
    _begin(writer, 100, 3)
    writer.append_chunk(0, memoryview(_record(1) * 2))
    writer.checkpoint()

    writer.begin_recovery(102, 1)
    writer.read_begin(ReadBeginNotification(102, 1))
    writer.read_begin(ReadBeginNotification(100, 3))

    with pytest.raises(StagingWriterStateError, match="does not match the prepared leg"):
        writer.read_begin(ReadBeginNotification(100, 2))
    writer.close()


def test_prefix_is_published_by_the_same_target(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 2)
    _begin(writer)
    writer.append_chunk(0, memoryview(_record(1)))
    writer.checkpoint()

    result = writer.publish_prefix()
    writer.close()

    assert result is not None
    assert result.bundle_path.joinpath("manifest.json").is_file()
    assert not result.bundle_path.joinpath("gap.json").exists()


def test_target_rejects_direct_cross_thread_calls_after_first_call(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 1)
    writer.prepare()
    failures: list[BaseException] = []

    def call_from_other_thread() -> None:
        try:
            writer.checkpoint()
        except BaseException as error:  # noqa: BLE001 - assert the affinity boundary
            failures.append(error)

    thread = threading.Thread(target=call_from_other_thread)
    thread.start()
    thread.join()
    writer.close()

    assert len(failures) == 1
    assert isinstance(failures[0], ThreadAffinityError)


def test_prepare_is_idempotent_and_data_waits_for_each_read_begin(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 3)
    try:
        descriptor = writer.prepare()
        assert writer.prepare() == descriptor
        raw = tmp_path / "attempts" / descriptor.attempt_id / "records.bin"
        checkpoint = raw.with_name("checkpoint.json")
        before = (raw.read_bytes(), checkpoint.read_bytes())

        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.append_chunk(0, memoryview(_record(1)))
        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.checkpoint()
        assert (raw.read_bytes(), checkpoint.read_bytes()) == before

        prefix = writer.prepare_leg(100, 3)
        assert prefix.record_count == 0
        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.append_chunk(0, memoryview(_record(1)))
        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.checkpoint()

        writer.read_begin(ReadBeginNotification(100, 3))
        writer.append_chunk(0, memoryview(_record(1) * 2))
        durable = writer.checkpoint()
        recovery = writer.begin_recovery(102, 1)
        assert recovery == durable
        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.append_chunk(0, memoryview(_record(3)))
        with pytest.raises(StagingWriterStateError, match="READ_BEGIN"):
            writer.checkpoint()
        writer.read_begin(ReadBeginNotification(102, 1))
        writer.append_chunk(0, memoryview(_record(3)))
        assert writer.checkpoint().next_sequence == 103
    finally:
        writer.close()


def test_invalid_append_scalars_and_empty_data_leave_persisted_bytes_unchanged(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 2)
    try:
        descriptor = writer.prepare()
        writer.read_begin(ReadBeginNotification(100, 2))
        raw = tmp_path / "attempts" / descriptor.attempt_id / "records.bin"
        checkpoint = raw.with_name("checkpoint.json")
        before = (raw.read_bytes(), checkpoint.read_bytes())

        for offset, error in ((True, TypeError), ("0", TypeError), (-1, ValueError)):
            with pytest.raises(error):
                writer.append_chunk(cast(int, offset), memoryview(_record(1)))
        with pytest.raises(ValueError, match="positive multiple"):
            writer.append_chunk(0, memoryview(b""))
        assert (raw.read_bytes(), checkpoint.read_bytes()) == before
    finally:
        writer.close()


def test_prepare_leg_validates_bounds_and_accepts_zero_start(tmp_path: Path) -> None:
    writer = StagingWriter(_store(tmp_path), 0, 2)
    try:
        prefix = writer.prepare_leg(0, 2)
        assert (prefix.start_sequence, prefix.next_sequence, prefix.record_count) == (0, 0, 0)
        for start, count in ((True, 1), (-1, 1), (0, 0), (0, True)):
            with pytest.raises(ValueError):
                writer.prepare_leg(start, count)
    finally:
        writer.close()


@pytest.mark.parametrize("terminal", ["seal", "prefix"])
def test_terminal_publication_blocks_later_appends(tmp_path: Path, terminal: str) -> None:
    writer = StagingWriter(_store(tmp_path), 100, 1)
    try:
        _begin(writer, 100, 1)
        if terminal == "seal":
            writer.append_chunk(0, memoryview(_record(1)))
            writer.checkpoint()
            writer.seal(DoneNotification(0, 101))
        else:
            writer.append_chunk(0, memoryview(_record(1)))
            writer.checkpoint()
            writer.publish_prefix()
        with pytest.raises(StagingWriterStateError, match="sealed"):
            writer.append_chunk(0, memoryview(_record(2)))
    finally:
        writer.close()


def test_close_flushes_raw_before_releasing_lease_and_resume_adopts_raw_tail(tmp_path: Path) -> None:
    lock_checks: list[bool] = []
    store: StagingStore

    def fsync_and_check_lock(fd: int) -> None:
        os.fsync(fd)
        if Path(f"/proc/self/fd/{fd}").resolve().name == "records.bin" and os.fstat(fd).st_size:
            try:
                with store.device_lock(operation="close_order_probe"):
                    lock_checks.append(False)
            except DeviceAlreadyRunningError:
                lock_checks.append(True)

    store = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_and_check_lock)
    writer = StagingWriter(store, 100, 1)
    try:
        descriptor = writer.prepare()
        writer.read_begin(ReadBeginNotification(100, 1))
        writer.append_chunk(0, memoryview(_record(1)))
        checkpoint = tmp_path / "attempts" / descriptor.attempt_id / "checkpoint.json"
        checkpoint_before_close = checkpoint.read_bytes()
        writer.close()
    finally:
        writer.close()

    assert lock_checks == [True]
    assert checkpoint.read_bytes() == checkpoint_before_close
    with store.device_lock(operation="post_close_probe"):
        pass

    resumed = StagingWriter(store, 100, 1)
    try:
        resumed.prepare()
        prefix = resumed.prepare_leg(100, 1)
        assert (prefix.next_sequence, prefix.record_count) == (101, 1)
    finally:
        resumed.close()
