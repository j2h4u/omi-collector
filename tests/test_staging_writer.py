from __future__ import annotations

import threading
from pathlib import Path
from shutil import rmtree

import pytest

from omi_collector.capture.adapters.attempts import RecordDisposition
from omi_collector.capture.adapters.staging_writer import StagingWriter, StagingWriterStateError, ThreadAffinityError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _record(value: int) -> bytes:
    return value.to_bytes(4, "big") + bytes(RECORD_SIZE - 4)


def _begin(writer: StagingWriter, start: int = 100, count: int = 2) -> None:
    writer.prepare()
    writer.read_begin(ReadBeginNotification(start, count))


def test_construction_does_not_touch_disk(tmp_path: Path) -> None:
    root = tmp_path / "staging"

    writer = StagingWriter(root, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))

    assert not root.exists()
    writer.close()
    assert not root.exists()


def test_writer_maps_arena_offsets_and_owns_streaming_mutations(tmp_path: Path) -> None:
    writer = StagingWriter(tmp_path, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))
    first = _record(1)
    second = _record(2)

    _begin(writer)
    dispositions = writer.append_chunk(0, memoryview(first + second))

    assert dispositions == (RecordDisposition.APPENDED, RecordDisposition.APPENDED)
    prefix = writer.checkpoint()
    assert prefix.record_count == 2
    assert prefix.start_sequence == 100
    assert prefix.next_sequence == 102
    result = writer.seal(DoneNotification(0, 102))
    writer.close()

    assert result.bundle_path.joinpath("records.bin").read_bytes() == first + second
    assert result.deduplicated is False


def test_append_chunk_rejects_unaligned_or_out_of_range_data(tmp_path: Path) -> None:
    writer = StagingWriter(tmp_path, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))
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
    initial = StagingWriter(tmp_path, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))
    _begin(initial)
    initial.append_chunk(0, memoryview(first))
    initial.checkpoint()
    initial.close()

    resumed = StagingWriter(tmp_path, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))
    prefix = resumed.prepare_leg(100, 2)
    assert prefix.next_sequence == 101
    resumed.read_begin(ReadBeginNotification(100, 2))
    assert resumed.append_chunk(0, memoryview(first + second)) == (
        RecordDisposition.REPLAYED,
        RecordDisposition.APPENDED,
    )
    resumed.checkpoint()
    result = resumed.seal(DoneNotification(0, 102))
    resumed.close()

    assert result.bundle_path.joinpath("records.bin").read_bytes() == first + second


def test_read_begin_rebinds_original_range_after_recovery_read_started(tmp_path: Path) -> None:
    writer = StagingWriter(tmp_path, "omi_cv1", 100, 3, capture_root=_capture_root(tmp_path))
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
    writer = StagingWriter(tmp_path, "omi_cv1", 100, 2, capture_root=_capture_root(tmp_path))
    _begin(writer)
    writer.append_chunk(0, memoryview(_record(1)))
    writer.checkpoint()

    result = writer.publish_prefix()
    writer.close()

    assert result is not None
    assert result.bundle_path.joinpath("manifest.json").is_file()
    assert not result.bundle_path.joinpath("gap.json").exists()


def test_target_rejects_direct_cross_thread_calls_after_first_call(tmp_path: Path) -> None:
    writer = StagingWriter(tmp_path, "omi_cv1", 100, 1, capture_root=_capture_root(tmp_path))
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
