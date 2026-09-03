"""Focused attempts staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from hashlib import sha256
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from types import SimpleNamespace
from typing import BinaryIO, cast

import pytest

from omi_collector.capture.adapters import quarantine, staging_filesystem
from omi_collector.capture.adapters.attempts import (
    RecordDisposition,
    RecordGapError,
    RecordMismatchError,
)
from omi_collector.capture.adapters.staging_contract import AttemptStateError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, DurabilityConfig, StagingRetentionConfig

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


@pytest.fixture(autouse=True)
def _isolate_capture_root(tmp_path: Path) -> None:
    rmtree(_capture_root(tmp_path), ignore_errors=True)


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _started_attempt(tmp_path: Path, *, count: int = 2):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt("omi_cv1", 100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _started_streaming_attempt(tmp_path: Path, *, count: int = 2, fsync_fn: Callable[[int], None] = fsync):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_fn).prepare_streaming_attempt(
        "omi_cv1", 100, count
    )
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _rewrite_checkpoint(path: Path, field: str, value: object) -> None:
    checkpoint = cast(dict[str, object], loads(path.read_text(encoding="utf-8")))
    checkpoint[field] = value
    path.write_text(dumps(checkpoint), encoding="utf-8")


def _replace_with_symlink(path: Path, target: Path, payload: bytes | str) -> None:
    path.unlink()
    path.symlink_to(target)
    if isinstance(payload, bytes):
        target.write_bytes(payload)
    else:
        target.write_text(payload, encoding="utf-8")


def _guard_rename_to_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    real_rename = staging_filesystem.os.rename

    def guarded_rename(
        source: str | bytes | PathLike[str] | PathLike[bytes],
        destination: str | bytes | PathLike[str] | PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        if src_dir_fd is None or dst_dir_fd is None or src_dir_fd != dst_dir_fd:
            raise OSError(EXDEV, "simulated cross-mount rename")
        real_rename(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(staging_filesystem.os, "rename", guarded_rename)


class _RecordingStream:
    def __init__(self, wrapped: object) -> None:
        self.wrapped = wrapped
        self.writes: list[bytes] = []

    def write(self, payload: bytes) -> int:
        self.writes.append(payload)
        return cast(int, self.wrapped.write(payload))  # type: ignore[attr-defined]

    def flush(self) -> None:
        self.wrapped.flush()  # type: ignore[attr-defined]

    def fileno(self) -> int:
        return cast(int, self.wrapped.fileno())  # type: ignore[attr-defined]

    def close(self) -> None:
        self.wrapped.close()  # type: ignore[attr-defined]


class _ReadCountingStream:
    def __init__(self, wrapped: BinaryIO, reads: list[int]) -> None:
        self._wrapped = wrapped
        self._reads = reads

    def read(self, size: int = -1) -> bytes:
        self._reads[0] += 1
        return self._wrapped.read(size)

    def fileno(self) -> int:
        return self._wrapped.fileno()

    def close(self) -> None:
        self._wrapped.close()

    def __enter__(self) -> _ReadCountingStream:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def test_prepare_persists_descriptor_before_returning(tmp_path: Path) -> None:
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt("omi_cv1", 100, 2)

    assert attempt.path.parent == tmp_path / "attempts"
    assert attempt.path.name == attempt.attempt_id
    assert (attempt.path / "attempt.json").is_file()
    assert (
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id).descriptor.start_sequence
        == 100
    )


def test_append_requires_read_begin_and_exact_order_and_size(tmp_path: Path) -> None:
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt("omi_cv1", 100, 2)
    with pytest.raises(AttemptStateError, match="READ_BEGIN"):
        attempt.append_record(0, 100, _record(1))

    attempt.record_read_begin(ReadBeginNotification(100, 2))
    with pytest.raises(AttemptStateError, match="exactly"):
        attempt.append_record(0, 100, b"short")
    with pytest.raises(AttemptStateError, match="next expected"):
        attempt.append_record(1, 101, _record(1))
    attempt.append_record(0, 100, _record(1))
    attempt.append_record(1, 101, _record(2))

    assert (attempt.path / "records.bin").read_bytes() == _record(1) + _record(2)


def test_streaming_accept_chunk_replays_overlap_and_appends_one_suffix(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    first, second, third = _record(1), _record(2), _record(3)
    attempt.accept_chunk(100, first)
    attempt.checkpoint()
    attempt.begin_recovery(100, 3)

    dispositions = attempt.accept_chunk(100, first + second + third)

    assert dispositions == (
        RecordDisposition.REPLAYED,
        RecordDisposition.APPENDED,
        RecordDisposition.APPENDED,
    )
    assert (attempt.path / "records.bin").read_bytes() == first + second + third


def test_streaming_accept_chunk_writes_the_appended_suffix_once(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    stream = _RecordingStream(cast(object, attempt._stream_raw))
    attempt._stream_raw = cast("BinaryIO", stream)
    attempt.accept_chunk(100, _record(1) + _record(2) + _record(3))

    assert stream.writes == [_record(1) + _record(2) + _record(3)]


def test_streaming_accept_chunk_matches_per_record_hash_and_checkpoint(tmp_path: Path) -> None:
    records = b"".join(_record(index % 256) for index in range(1025))
    per_record = _started_streaming_attempt(tmp_path / "per-record", count=1025)
    chunked = _started_streaming_attempt(tmp_path / "chunked", count=1025)
    for index in range(1025):
        per_record.append_record(index, 100 + index, records[index * RECORD_SIZE : (index + 1) * RECORD_SIZE])
    chunked.accept_chunk(100, records)

    assert (per_record.path / "records.bin").read_bytes() == (chunked.path / "records.bin").read_bytes()
    per_checkpoint = cast(dict[str, object], loads((per_record.path / "checkpoint.json").read_text()))
    chunk_checkpoint = cast(dict[str, object], loads((chunked.path / "checkpoint.json").read_text()))
    assert per_checkpoint["record_count"] == chunk_checkpoint["record_count"] == 1024
    assert per_checkpoint["raw_sha256"] == chunk_checkpoint["raw_sha256"]
    assert per_record.durable_prefix == chunked.durable_prefix


@pytest.mark.parametrize("records", [b"", b"short", _record(1) + b"torn"])
def test_streaming_accept_chunk_rejects_malformed_payload(tmp_path: Path, records: bytes) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)

    with pytest.raises(AttemptStateError, match="positive multiple"):
        attempt.accept_chunk(100, records)

    assert (attempt.path / "records.bin").read_bytes() == b""


def test_streaming_accept_chunk_rejects_gap_and_mutated_replay(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    first, second = _record(1), _record(2)
    attempt.accept_chunk(100, first + second)
    attempt.checkpoint()
    attempt.begin_recovery(100, 3)

    with pytest.raises(RecordGapError):
        attempt.accept_chunk(102, _record(3) + _record(4))
    with pytest.raises(RecordMismatchError):
        attempt.accept_chunk(100, _record(9))

    assert (attempt.path / "records.bin").read_bytes() == first + second


def test_streaming_checkpoint_is_bounded_and_flushes_an_explicit_tail(tmp_path: Path) -> None:
    sync_calls = 0

    def track_sync(fd: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        fsync(fd)

    attempt = _started_streaming_attempt(tmp_path, count=1025, fsync_fn=track_sync)
    before_records = sync_calls
    for index in range(1023):
        attempt.append_record(index, 100 + index, _record((index + 1) % 256))
    assert sync_calls == before_records

    attempt.append_record(1023, 1123, _record(1024 % 256))
    # A streaming checkpoint now durably publishes both the raw fsync and its
    # atomic checkpoint file (temporary-file fsync plus attempt-directory fsync).
    assert sync_calls == before_records + 3
    attempt.append_record(1024, 1124, _record(1025 % 256))
    assert sync_calls == before_records + 3

    attempt.checkpoint()
    assert sync_calls == before_records + 6
    assert (attempt.path / "records.bin").stat().st_size == 1025 * RECORD_SIZE
    attempt.close()


def test_streaming_checkpoints_never_rehash_the_growing_raw_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1024)
    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _path: (_ for _ in ()).throw(AssertionError("streaming staging must not use read_bytes")),
    )
    for index in range(1024):
        attempt.append_record(index, 100 + index, _record(index % 256))
    attempt.checkpoint()

    attempt.seal(DoneNotification(0, 1124))


def test_durability_config_projects_headroom_overhead_and_checkpoint_batching(tmp_path: Path) -> None:
    durability = DurabilityConfig(
        staging_headroom_bytes=10,
        staging_overhead_fraction=0.5,
        checkpoint_records=2,
        io_chunk_bytes=7,
    )
    config = CollectorConfig(durability=durability)
    expected_required = 3 * RECORD_SIZE + max(10, 3 * RECORD_SIZE // 2)

    def statvfs(_: str | Path) -> object:
        return SimpleNamespace(f_bavail=expected_required, f_frsize=1)

    store = StagingStore(tmp_path, _capture_root(tmp_path), statvfs_fn=statvfs, config=config)
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    attempt.append_record(0, 100, _record(1))
    attempt.append_record(1, 101, _record(2))

    checkpoint = cast(dict[str, object], loads((attempt.path / "checkpoint.json").read_text()))
    assert expected_required == 1998
    assert checkpoint["record_count"] == 2


def test_writer_close_failure_preserves_recoverable_prefix_without_terminal_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    writer = StagingWriter(store, "omi_cv1", 100, 1)
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    assert writer.publish_prefix() is None
    attempt = writer._attempt
    assert attempt is not None
    original_close = attempt.close

    def fail_close(*, durable: bool = False, _durable: bool | None = None) -> None:
        del durable, _durable
        raise OSError("simulated writer close failure")

    monkeypatch.setattr(attempt, "close", fail_close)
    with pytest.raises(OSError, match="writer close failure"):
        writer.close()
    monkeypatch.setattr(attempt, "close", original_close)
    attempt.close(durable=True)

    assert (attempt.path / "prefix-publication.json").is_file()
    assert not (attempt.path / "terminal-retired.json").exists()
    assert store.sweep_terminal_retired("omi_cv1") == ()
    assert attempt.path.exists()


def test_terminal_marker_write_failure_leaves_only_recoverable_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    monkeypatch.setattr(quarantine, "_wall_clock_ns", wall_clock_ns)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    write_json_atomic = store._filesystem._write_json_atomic

    def fail_after_terminal_marker_write(path: Path, value: object) -> None:
        write_json_atomic(path, value)
        if path.name == "terminal-retired.json":
            raise OSError("simulated terminal marker fsync failure")

    monkeypatch.setattr(store._filesystem, "_write_json_atomic", fail_after_terminal_marker_write)
    with pytest.raises(OSError, match="terminal marker fsync failure"):
        store.terminalize_prefix_attempt("omi_cv1", attempt.attempt_id)

    now += 1_000_000_000
    assert (attempt.path / "prefix-publication.json").is_file()
    assert not (attempt.path / "terminal-retired.json").exists()
    assert store.sweep_terminal_retired("omi_cv1") == ()
    assert attempt.path.exists()


def test_streaming_rejects_malformed_sequence_and_count(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    with pytest.raises(AttemptStateError, match="next expected"):
        attempt.append_record(1, 101, _record(1))
    with pytest.raises(AttemptStateError, match="next expected"):
        attempt.append_record(0, 101, _record(1))
    attempt.append_record(0, 100, _record(1))
    with pytest.raises(AttemptStateError, match="exceeds READ_BEGIN"):
        attempt.append_record(1, 101, _record(2))


def test_streaming_reopen_does_not_rehash_or_trust_a_partial_prefix(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    first = _record(1)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    attempt.close()
    before = (attempt.path / "records.bin").read_bytes()

    reopened = StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)

    with pytest.raises(AttemptStateError, match="no trusted live durable prefix"):
        _ = reopened.durable_prefix
    assert (attempt.path / "records.bin").read_bytes() == before
    reopened.close()


def test_streaming_resume_promotes_only_an_aligned_post_checkpoint_tail(tmp_path: Path) -> None:
    first_store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = first_store.prepare_streaming_attempt("omi_cv1", 100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    first, second = _record(1), _record(2)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    attempt.append_record(1, 101, second)
    attempt.close(durable=True)
    checkpoint_before = cast(dict[str, object], loads((attempt.path / "checkpoint.json").read_text(encoding="utf-8")))

    restarted = StagingStore(tmp_path, _capture_root(tmp_path))
    with restarted.device_lock("omi_cv1") as lease:
        resumed = restarted.resume_streaming_attempt("omi_cv1", lease)
        assert resumed is not None
        assert resumed.durable_prefix.record_count == 2
        assert resumed.durable_prefix.raw_sha256 == sha256(first + second).hexdigest()
        assert loads((resumed.path / "checkpoint.json").read_text(encoding="utf-8"))["record_count"] == 2
        resumed.close()

    assert checkpoint_before["record_count"] == 1
    assert (attempt.path / "records.bin").read_bytes() == first + second
    assert attempt.path.exists()


def test_large_resume_hashes_raw_evidence_in_one_streaming_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 4097)
    attempt.record_read_begin(ReadBeginNotification(100, 4097))
    for index in range(4097):
        attempt.append_record(index, 100 + index, _record(index % 256))
    attempt.close(durable=True)

    reads = [0]
    original_open = Path.open

    def count_raw_reads(path: Path, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = cast(Callable[..., object], original_open)(path, mode, *args, **kwargs)
        if path.name == "records.bin" and mode == "rb":
            return _ReadCountingStream(cast(BinaryIO, stream), reads)
        return stream

    monkeypatch.setattr(Path, "open", count_raw_reads)
    with store.device_lock("omi_cv1") as lease:
        resumed = store.resume_streaming_attempt("omi_cv1", lease)
        assert resumed is not None
        assert resumed.durable_prefix.record_count == 4097
        resumed.close()

    # The 4097-record stream is 2 chunks at the configured 1 MiB size plus
    # one EOF read. Promotion performs only a metadata fsync open, never a
    # second payload read or read_bytes allocation.
    assert reads[0] == 3


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "malformed",
        "version",
        "identity",
        "hash",
        "short",
        "unaligned",
        "oversized",
        "symlink",
        "nonregular",
        "raw_symlink",
        "raw_nonregular",
    ],
)
def test_streaming_resume_rejects_damaged_checkpoint_or_raw_evidence(tmp_path: Path, damage: str) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    attempt.close(durable=True)
    checkpoint = attempt.path / "checkpoint.json"
    raw = attempt.path / "records.bin"

    actions: dict[str, Callable[[], object]] = {
        "missing": checkpoint.unlink,
        "malformed": lambda: checkpoint.write_text("{", encoding="utf-8"),
        "version": lambda: _rewrite_checkpoint(checkpoint, "version", 2),
        "identity": lambda: _rewrite_checkpoint(checkpoint, "attempt_id", "f" * 32),
        "hash": lambda: _rewrite_checkpoint(checkpoint, "raw_sha256", "0" * 64),
        "short": lambda: raw.write_bytes(b""),
        "unaligned": lambda: raw.write_bytes(raw.read_bytes() + b"torn"),
        "oversized": lambda: raw.write_bytes(raw.read_bytes() + _record(2) + _record(3)),
        "symlink": lambda: _replace_with_symlink(checkpoint, tmp_path / "outside-checkpoint", "{}"),
        "nonregular": lambda: (checkpoint.unlink(), checkpoint.mkdir()),
        "raw_symlink": lambda: _replace_with_symlink(raw, tmp_path / "outside-raw", _record(1)),
        "raw_nonregular": lambda: (raw.unlink(), raw.mkdir()),
    }
    actions[damage]()

    with pytest.raises(AttemptStateError):
        store.open_attempt(attempt.attempt_id)
    assert attempt.path.exists()
    assert (attempt.path / "attempt.json").exists()


def test_reopen_rejects_unaligned_or_out_of_bounds_streaming_bytes(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.close()
    raw = attempt.path / "records.bin"
    raw.write_bytes(b"torn")
    with pytest.raises(AttemptStateError, match="record aligned"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)

    raw.write_bytes(_record(1) + _record(2) + _record(3))
    with pytest.raises(AttemptStateError, match="exceeds"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)


@pytest.mark.parametrize(
    ("field", "value"),
    [("attempt_id", 7), ("start_sequence", "100"), ("read_begin_start", True)],
)
def test_open_rejects_malformed_descriptor_fields(tmp_path: Path, field: str, value: object) -> None:
    attempt_id = "a" * 32
    attempt_path = tmp_path / "attempts" / attempt_id
    attempt_path.mkdir(parents=True)
    payload: dict[str, object] = {
        "attempt_id": attempt_id,
        "device_slug": "omi_cv1",
        "start_sequence": 100,
        "packet_count": 2,
        "record_size": RECORD_SIZE,
        "read_begin_start": None,
        "read_begin_count": None,
    }
    payload[field] = value
    (attempt_path / "attempt.json").write_text(dumps(payload), encoding="utf-8")

    with pytest.raises(AttemptStateError, match="malformed"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)


def test_open_rejects_non_object_and_invalid_descriptor_identity(tmp_path: Path) -> None:
    attempt_id = "a" * 32
    attempt_path = tmp_path / "attempts" / attempt_id
    attempt_path.mkdir(parents=True)
    descriptor = {
        "attempt_id": attempt_id,
        "device_slug": "omi_cv1",
        "start_sequence": 100,
        "packet_count": 2,
        "record_size": RECORD_SIZE,
        "read_begin_start": None,
        "read_begin_count": None,
    }
    (attempt_path / "attempt.json").write_text("[]", encoding="utf-8")
    with pytest.raises(AttemptStateError, match="must contain a JSON object"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)

    descriptor["record_size"] = RECORD_SIZE + 1
    (attempt_path / "attempt.json").write_text(dumps(descriptor), encoding="utf-8")
    with pytest.raises(AttemptStateError, match="identity or record size"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)

    descriptor["record_size"] = RECORD_SIZE
    descriptor["attempt_id"] = "b" * 32
    (attempt_path / "attempt.json").write_text(dumps(descriptor), encoding="utf-8")
    with pytest.raises(AttemptStateError, match="identity or record size"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)


@pytest.mark.parametrize(
    ("read_begin_start", "read_begin_count", "message"),
    [(100, None, "incomplete READ_BEGIN"), (100, 3, "does not match requested range")],
)
def test_open_rejects_invalid_persisted_read_begin(
    tmp_path: Path, read_begin_start: int, read_begin_count: int | None, message: str
) -> None:
    attempt_id = "a" * 32
    attempt_path = tmp_path / "attempts" / attempt_id
    attempt_path.mkdir(parents=True)
    descriptor = {
        "attempt_id": attempt_id,
        "device_slug": "omi_cv1",
        "start_sequence": 100,
        "packet_count": 2,
        "record_size": RECORD_SIZE,
        "read_begin_start": read_begin_start,
        "read_begin_count": read_begin_count,
    }
    (attempt_path / "attempt.json").write_text(dumps(descriptor), encoding="utf-8")

    with pytest.raises(AttemptStateError, match=message):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)


def test_read_begin_is_idempotent_and_rejects_mismatch(tmp_path: Path) -> None:
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt("omi_cv1", 100, 1)
    with pytest.raises(AttemptStateError, match="does not match"):
        attempt.record_read_begin(ReadBeginNotification(101, 1))
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    with pytest.raises(AttemptStateError, match="already persisted"):
        attempt.record_read_begin(ReadBeginNotification(100, 2))


@pytest.mark.parametrize(
    ("device_slug", "start_sequence", "packet_count"),
    [("omi/cv1", 1, 1), ("", 1, 1), ("omi_cv1", -1, 1), ("omi_cv1", 1, 0), ("omi_cv1", True, 1)],
)
def test_prepare_validates_slug_sequences_and_counts(
    tmp_path: Path, device_slug: str, start_sequence: int, packet_count: int
) -> None:
    with pytest.raises(AttemptStateError):
        StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt(
            device_slug, start_sequence, packet_count
        )


def test_append_validates_integer_arguments_and_count_overflow(tmp_path: Path) -> None:
    attempt = _started_attempt(tmp_path, count=1)
    with pytest.raises(AttemptStateError, match="index"):
        attempt.append_record(-1, 100, _record(1))
    with pytest.raises(AttemptStateError, match="sequence"):
        attempt.append_record(0, -1, _record(1))
    attempt.append_record(0, 100, _record(1))
    with pytest.raises(AttemptStateError, match="exceeds READ_BEGIN"):
        attempt.append_record(1, 101, _record(2))
