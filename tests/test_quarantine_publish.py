from __future__ import annotations

from collections.abc import Callable
from errno import EEXIST, EIO
from hashlib import sha256
from json import dumps, loads
from multiprocessing import get_context
from multiprocessing.queues import Queue as QueueType
from os import PathLike, fsdecode, fsync, mkfifo
from pathlib import Path
from shutil import copytree, rmtree
from stat import S_IMODE
from typing import BinaryIO, cast

import pytest

import omi_collector.capture.adapters.quarantine_publish as quarantine_publish
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.quarantine_publish import (
    QuarantineOutputCollisionError,
    QuarantinePublishError,
    QuarantineSalvageDeferredError,
)
from omi_collector.capture.adapters.staging_contract import StagingError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from staging_test_factory import StagingStore

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _case_path(tmp_path: Path, name: str) -> Path:
    case = tmp_path / name
    case.mkdir()
    return case


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _quarantined_attempt(spool: Path) -> tuple[Path, bytes, bytes]:
    attempt = StagingStore(spool, spool.parent / "captures").prepare_streaming_attempt(100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    prefix = _record(1)
    tail = _record(2)
    attempt.accept_chunk(100, prefix)
    attempt.checkpoint()
    attempt.accept_chunk(101, tail)
    attempt.close(durable=True)
    source = StagingStore(spool, spool.parent / "captures").quarantine_attempt_source(attempt.attempt_id)
    return source, prefix, tail


def _single_quarantined_attempt(spool: Path, *, start: int = 100, count: int = 1) -> tuple[Path, bytes]:
    store = StagingStore(spool, _capture_root(spool))
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    payload = _record(1)
    attempt.accept_chunk(start, payload)
    attempt.checkpoint()
    attempt.close(durable=True)
    return store.quarantine_attempt_source(attempt.attempt_id), payload


def _publish_fifo_child(source: str, spool: str, capture_root: str, output: QueueType[object]) -> None:
    output.put(str(Path(cast(str, quarantine_publish.__file__)).resolve()))
    try:
        child_store = StagingStore(Path(spool), Path(capture_root))
        child_store.publish_quarantined_prefix(Path(source), should_defer=lambda: False)
    except QuarantinePublishError as error:
        output.put(("raised", type(error).__name__, str(error)))
    else:
        output.put(("returned", "unexpectedly published FIFO source"))


def _snapshot(source: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}


def _attempt_state(store: StagingStore, source: Path) -> tuple[str, dict[str, object]]:
    quarantine_row = store.state_db.get(StateKind.QUARANTINE, source.name)
    assert quarantine_row is not None
    attempt_id = cast(str, quarantine_row["attempt_id"])
    row = store.state_db.get(StateKind.ATTEMPT, attempt_id)
    assert row is not None
    return attempt_id, row


def _rewrite_attempt_field(store: StagingStore, source: Path, section: str, key: str, value: object) -> None:
    attempt_id, row = _attempt_state(store, source)
    document = cast(dict[str, object], row[section])
    if key.startswith("delete:"):
        del document[key.removeprefix("delete:")]
    else:
        document[key] = value
    store.state_db.put(StateKind.ATTEMPT, attempt_id, row)


def _rename_quarantine_entry(store: StagingStore, source: Path, destination: Path) -> None:
    quarantine_row = store.state_db.get(StateKind.QUARANTINE, source.name)
    assert quarantine_row is not None
    source.rename(destination)
    quarantine_row["physical_name"] = destination.name
    with store.state_db.transaction() as transaction:
        transaction.delete(StateKind.QUARANTINE, source.name)
        transaction.put(StateKind.QUARANTINE, destination.name, quarantine_row)


class _ShortWritingStream:
    def __init__(
        self,
        path: Path,
        wrapped: BinaryIO,
        written: dict[str, int],
        short_write_size: int | None,
    ) -> None:
        self.path = path
        self.wrapped = wrapped
        self.written = written
        self.short_write_size = short_write_size

    def write(self, payload: bytes) -> int:
        if not payload:
            raise AssertionError(f"publisher attempted an empty write to {self.path.name}")
        write_payload = payload if self.short_write_size is None else payload[: self.short_write_size]
        count = self.wrapped.write(write_payload)
        if count <= 0:
            raise AssertionError(f"publisher made no write progress for {self.path.name}")
        self.written[self.path.name] = self.written.get(self.path.name, 0) + count
        return count

    def flush(self) -> None:
        self.wrapped.flush()

    def fileno(self) -> int:
        return self.wrapped.fileno()

    def close(self) -> None:
        self.wrapped.close()

    def __enter__(self) -> _ShortWritingStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.wrapped.close()


def _patch_short_writes(
    monkeypatch: pytest.MonkeyPatch,
    written: dict[str, int],
    *,
    short_write_size: int | None = None,
) -> None:
    real_open = cast(Callable[..., BinaryIO], Path.open)

    def observed_open(path: Path, mode: str = "r", *args: object, **kwargs: object) -> object:
        wrapped = real_open(path, mode, *args, **kwargs)
        if mode == "xb" and path.parent.name.startswith(".") and path.parent.name.endswith(".tmp"):
            return _ShortWritingStream(path, wrapped, written, short_write_size)
        return wrapped

    monkeypatch.setattr(Path, "open", observed_open)


@pytest.fixture(autouse=True)
def _guard_publication_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_short_writes(monkeypatch, {})


def test_publishes_only_authenticated_prefix_and_leaves_source_unchanged(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    source_raw = source.joinpath("records.bin").read_bytes()

    result = StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(
        source, should_defer=lambda: False
    )

    assert not result.deduplicated
    assert result.record_count == 1
    assert result.bundle_path == _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert source.joinpath("records.bin").read_bytes() == source_raw == prefix + tail
    manifest = cast(dict[str, object], loads(result.bundle_path.joinpath("manifest.json").read_text(encoding="utf-8")))
    assert manifest == {
        "schema_version": 2,
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": RECORD_SIZE,
        "raw_sha256": sha256(prefix).hexdigest(),
    }
    assert set(result.bundle_path.iterdir()) == {
        result.bundle_path / "records.bin",
        result.bundle_path / "manifest.json",
    }
    assert not tuple((_capture_root(tmp_path)).glob(".*.tmp"))
    duplicate = StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(
        source, should_defer=lambda: False
    )
    assert duplicate.deduplicated


def test_quarantine_publication_uses_shared_bundle_directory_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    requested_modes: list[int] = []
    real_mkdir = quarantine_publish.os.mkdir

    def observed_mkdir(
        path: str | bytes | PathLike[str] | PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if dir_fd is not None:
            requested_modes.append(mode)
        real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(quarantine_publish.os, "mkdir", observed_mkdir)
    result = StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(
        source, should_defer=lambda: False
    )

    assert requested_modes == [0o770]
    mode = S_IMODE(result.bundle_path.stat().st_mode)
    assert mode & 0o700 == 0o700
    assert mode & 0o007 == 0


def test_rejects_symlinked_source_raw(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    authority = source / "records.bin"
    target = tmp_path / "records.bin.target"
    authority.rename(target)
    authority.symlink_to(target)

    with pytest.raises(QuarantinePublishError, match=r"missing or unreadable|regular file"):
        StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(source, should_defer=lambda: False)

    assert not tuple((_capture_root(tmp_path)).glob("100-*"))


def test_rejects_nonidentical_existing_bundle_collision(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)
    (destination / "records.bin").write_bytes(_record(9))
    (destination / "manifest.json").write_text("{}", encoding="utf-8")

    with pytest.raises(QuarantineOutputCollisionError, match="ordinary bundle collision"):
        StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(source, should_defer=lambda: False)


def test_noncanonical_existing_bundle_is_retryable_not_a_conflict(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)

    with pytest.raises(OSError, match="not yet canonical"):
        StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(source, should_defer=lambda: False)


def test_rejects_unknown_attempt_field(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    _rewrite_attempt_field(store, source, "descriptor", "unexpected", True)

    with pytest.raises(QuarantinePublishError, match="schema is not exact"):
        store.publish_quarantined_prefix(source)


@pytest.mark.parametrize("layout", ["alias", "nested"])
def test_rejects_alias_or_nested_publication_roots(tmp_path: Path, layout: str) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    if layout == "alias":
        capture_root = tmp_path / "capture-alias"
        capture_root.symlink_to(_capture_root(tmp_path), target_is_directory=True)
    else:
        capture_root = tmp_path / "nested-captures"
        capture_root.mkdir()
        (tmp_path / "nested-captures" / "inside").mkdir()
        capture_root = tmp_path / "nested-captures" / "inside"

    with pytest.raises(
        (OSError, QuarantinePublishError, StagingError),
        match=r"temporarily unavailable|real directory|regular directory|distinct, non-nested",
    ):
        store = StagingStore(tmp_path, capture_root)
        store.publish_quarantined_prefix(source, should_defer=lambda: False)


def test_rejects_capture_root_symlink_swap_before_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    real_publish = quarantine_publish._publish_atomic

    def swap_before_atomic(
        destination: Path,
        manifest: dict[str, object],
        raw_source: Path,
        prefix_size: int,
        *,
        should_defer: Callable[[], bool],
    ) -> None:
        backup = tmp_path / "capture-backup"
        capture_root.rename(backup)
        capture_root.symlink_to(outside, target_is_directory=True)
        real_publish(destination, manifest, raw_source, prefix_size, should_defer=should_defer)

    monkeypatch.setattr(quarantine_publish, "_publish_atomic", swap_before_atomic)
    with pytest.raises(OSError, match="temporarily unavailable"):
        StagingStore(tmp_path, capture_root).publish_quarantined_prefix(source, should_defer=lambda: False)
    assert not tuple(outside.iterdir())


def test_manual_publish_reuses_active_device_lock(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    store = StagingStore(tmp_path, capture_root)

    with store.device_lock() as lease:
        result = store.publish_quarantined_prefix(source)
        lease.require_active()

    assert not tuple((capture_root).glob(".*.tmp"))
    assert (
        result.bundle_path.joinpath("records.bin").read_bytes()
        == source.joinpath("records.bin").read_bytes()[:RECORD_SIZE]
    )


def test_hash_deferral_preserves_source_and_retries_byte_identically(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    count = quarantine_publish.DEFAULT_CONFIG.durability.io_chunk_bytes // RECORD_SIZE + 2
    payload = bytes(index % 251 for index in range(count * RECORD_SIZE))
    attempt = store.prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    attempt.accept_chunk(100, memoryview(payload))
    attempt.checkpoint()
    attempt_id = attempt.attempt_id
    attempt.close(durable=True)
    source = store.quarantine_attempt_source(attempt_id)
    before = {path.name: path.read_bytes() for path in source.iterdir()}
    defer_checks = 0

    def defer_after_first_chunk() -> bool:
        nonlocal defer_checks
        defer_checks += 1
        return defer_checks >= 3

    with pytest.raises(QuarantineSalvageDeferredError):
        store.publish_quarantined_prefix(source, should_defer=defer_after_first_chunk)

    assert {path.name: path.read_bytes() for path in source.iterdir()} == before
    assert not tuple((_capture_root(tmp_path)).glob(".*.tmp"))
    result = store.publish_quarantined_prefix(source)
    assert result.bundle_path.joinpath("records.bin").read_bytes() == payload


def test_defer_requested_after_atomic_rename_finishes_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    renamed = False
    real_rename = quarantine_publish.os.rename

    def observed_rename(*args: object, **kwargs: object) -> None:
        nonlocal renamed
        real_rename(*args, **kwargs)  # type: ignore[arg-type]
        renamed = True

    monkeypatch.setattr(quarantine_publish.os, "rename", observed_rename)
    result = store.publish_quarantined_prefix(source, should_defer=lambda: renamed)

    assert renamed
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempt_id", "f" * 32),
        ("schema_version", 3),
        ("start_sequence", -1),
        ("packet_count", 0),
        ("packet_count", True),
        ("record_size", RECORD_SIZE + 1),
        ("read_begin_start", "100"),
        ("read_begin_start", 99),
        ("read_begin_count", False),
        ("read_begin_count", 2),
        ("unexpected", "field"),
    ],
)
def test_invalid_attempt_descriptor_is_preserved_and_valid_retry_publishes(
    tmp_path: Path, field: str, value: object
) -> None:
    case = _case_path(tmp_path, f"attempt-{field}-{value}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    original = _snapshot(source)
    attempt_id, original_state = _attempt_state(store, source)
    _rewrite_attempt_field(store, source, "descriptor", field, value)
    invalid_evidence = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == invalid_evidence
    assert not tuple(_capture_root(case).iterdir())
    store.state_db.put(StateKind.ATTEMPT, attempt_id, original_state)
    result = store.publish_quarantined_prefix(source)
    assert result.record_count == 1
    assert _snapshot(source) == original


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("unexpected", "field"),
        ("version", 2),
        ("attempt_id", "f" * 32),
        ("record_count", -1),
        ("record_count", True),
        ("raw_sha256", "abc"),
        ("raw_sha256", "g" * 64),
        ("raw_sha256", "0" * 64),
        ("record_count", 4),
        ("record_count", 3),
        ("delete:raw_sha256", None),
    ],
)
def test_invalid_checkpoint_is_preserved_and_valid_retry_publishes(tmp_path: Path, field: str, value: object) -> None:
    case = _case_path(tmp_path, f"checkpoint-{field}-{value}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    original = _snapshot(source)
    attempt_id, original_state = _attempt_state(store, source)
    _rewrite_attempt_field(store, source, "checkpoint", field, value)
    invalid_evidence = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == invalid_evidence
    assert not tuple(_capture_root(case).iterdir())
    store.state_db.put(StateKind.ATTEMPT, attempt_id, original_state)
    result = store.publish_quarantined_prefix(source)
    assert result.record_count == 1
    assert _snapshot(source) == original


@pytest.mark.parametrize("conflict", ["records.bin", "manifest.json"])
def test_existing_bundle_conflicts_are_typed_and_preserve_both_sides(tmp_path: Path, conflict: str) -> None:
    case = _case_path(tmp_path, f"conflict-{conflict}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    result = store.publish_quarantined_prefix(source)
    source_before = _snapshot(source)
    destination = result.bundle_path
    conflict_file = destination / conflict

    if conflict == "records.bin":
        conflict_file.write_bytes(_record(9))
    else:
        document = cast(dict[str, object], loads(conflict_file.read_text(encoding="utf-8")))
        document["start_sequence"] = 200
        document["next_sequence"] = 201
        conflict_file.write_text(dumps(document), encoding="utf-8")
    destination_after_conflict = _snapshot(destination)

    with pytest.raises(QuarantineOutputCollisionError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == source_before
    assert _snapshot(destination) == destination_after_conflict


@pytest.mark.parametrize("missing", ["records.bin", "manifest.json"])
def test_incomplete_existing_destination_is_retryable_and_preserved(tmp_path: Path, missing: str) -> None:
    case = _case_path(tmp_path, f"incomplete-{missing}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    result = store.publish_quarantined_prefix(source)
    source_before = _snapshot(source)
    destination_before = _snapshot(result.bundle_path)
    (result.bundle_path / missing).unlink()
    destination_after_removal = _snapshot(result.bundle_path)

    with pytest.raises(OSError, match="not yet canonical") as error:
        store.publish_quarantined_prefix(source)

    assert not isinstance(error.value, QuarantineOutputCollisionError)
    assert _snapshot(source) == source_before
    assert _snapshot(result.bundle_path) == destination_after_removal
    (result.bundle_path / missing).write_bytes(destination_before[missing])
    assert store.publish_quarantined_prefix(source).deduplicated
    assert _snapshot(source) == source_before


def test_repeat_publication_ignores_opaque_source_tail_and_preserves_both_sides(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    first = store.publish_quarantined_prefix(source)
    source_before = _snapshot(source)
    destination_before = _snapshot(first.bundle_path)

    duplicate = store.publish_quarantined_prefix(source)

    assert duplicate.deduplicated
    assert _snapshot(source) == source_before
    assert _snapshot(first.bundle_path) == destination_before
    assert first.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert source.joinpath("records.bin").read_bytes() == prefix + tail


def test_publication_completes_short_positive_writes_without_empty_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    source_before = _snapshot(source)
    written: dict[str, int] = {}
    _patch_short_writes(monkeypatch, written, short_write_size=7)
    result = store.publish_quarantined_prefix(source)

    digest = sha256(prefix).hexdigest()
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert loads(result.bundle_path.joinpath("manifest.json").read_text(encoding="utf-8")) == {
        "schema_version": 2,
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": RECORD_SIZE,
        "raw_sha256": digest,
    }
    assert written == {
        "records.bin": len(prefix),
        "manifest.json": len((result.bundle_path / "manifest.json").read_bytes()),
    }
    assert _snapshot(source) == source_before
    assert source.joinpath("records.bin").read_bytes() == prefix + tail


def test_public_publication_result_serializes_exact_raw_byte_count_and_replay_status(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))

    first = store.publish_quarantined_prefix(source, should_defer=lambda: False)
    first_payload = first.as_dict()
    assert first_payload == {
        "bundle_path": str(first.bundle_path),
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "raw_bytes": RECORD_SIZE,
        "raw_sha256": sha256(prefix).hexdigest(),
        "deduplicated": False,
    }
    assert type(first_payload["raw_bytes"]) is int

    replay = store.publish_quarantined_prefix(source, should_defer=lambda: False)
    replay_payload = replay.as_dict()
    assert replay_payload["raw_bytes"] == RECORD_SIZE
    assert type(replay_payload["raw_bytes"]) is int
    assert replay_payload["deduplicated"] is True
    assert replay_payload["bundle_path"] == str(first.bundle_path)


def test_publication_leaves_unrelated_authenticated_capture_temporary_for_device_lock(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    prior = store.prepare_streaming_attempt(200, 1)
    prior.record_read_begin(ReadBeginNotification(200, 1))
    prior.accept_chunk(200, _record(9))
    published = prior.seal(DoneNotification(0, 201))
    temporary = published.bundle_path.with_name(f".{published.bundle_path.name}.{'a' * 32}.tmp")
    published.bundle_path.rename(temporary)
    before = _snapshot(temporary)
    source_before = _snapshot(source)

    result = store.publish_quarantined_prefix(source)

    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert source.joinpath("records.bin").read_bytes() == prefix + tail
    assert _snapshot(source) == source_before
    assert temporary.is_dir()
    assert _snapshot(temporary) == before
    with store.device_lock():
        pass
    assert published.bundle_path.is_dir()
    assert not temporary.exists()
    assert (published.bundle_path / "records.bin").read_bytes() == _record(9)


@pytest.mark.parametrize("record_count", [0, 4])
def test_invalid_checkpoint_bounds_preserve_quarantine_source_and_publish_nothing(
    tmp_path: Path, record_count: int
) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    raw = source / "records.bin"
    if record_count == 0:
        _rewrite_attempt_field(store, source, "checkpoint", "record_count", 0)
        _rewrite_attempt_field(store, source, "checkpoint", "raw_sha256", sha256(b"").hexdigest())
    else:
        payload = raw.read_bytes() + _record(3) + _record(4)
        raw.write_bytes(payload)
        _rewrite_attempt_field(store, source, "checkpoint", "record_count", record_count)
        _rewrite_attempt_field(store, source, "checkpoint", "raw_sha256", sha256(payload).hexdigest())
    original = _snapshot(source)
    capture = _capture_root(tmp_path)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == original
    assert tuple(capture.iterdir()) == ()


@pytest.mark.parametrize("invalid_id", ["g" * 32, "a" * 31])
def test_publication_rejects_noncanonical_directory_identity_without_mutation(tmp_path: Path, invalid_id: str) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    old_id, _ = _attempt_state(store, source)
    _rewrite_attempt_field(store, source, "descriptor", "attempt_id", invalid_id)
    _rewrite_attempt_field(store, source, "checkpoint", "attempt_id", invalid_id)
    invalid_source = source.with_name(invalid_id)
    _rename_quarantine_entry(store, source, invalid_source)
    original = _snapshot(invalid_source)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(invalid_source)

    assert _snapshot(invalid_source) == original
    assert tuple(_capture_root(tmp_path).iterdir()) == ()
    assert old_id != invalid_id


def test_publication_succeeds_when_quarantine_source_basename_is_exact_attempt_id(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt_id, _ = _attempt_state(store, source)
    exact = source.with_name(attempt_id)
    _rename_quarantine_entry(store, source, exact)
    source_before = _snapshot(exact)

    result = store.publish_quarantined_prefix(exact)

    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert _snapshot(exact) == source_before
    assert (exact / "records.bin").read_bytes() == prefix + tail


def test_one_record_quarantine_publication_at_packet_count_boundary(tmp_path: Path) -> None:
    source, raw = _single_quarantined_attempt(tmp_path, count=1)
    original = _snapshot(source)

    result = StagingStore(tmp_path, _capture_root(tmp_path)).publish_quarantined_prefix(
        source, should_defer=lambda: False
    )

    assert result.start_sequence == 100
    assert result.next_sequence == 101
    assert result.record_count == 1
    assert result.raw_bytes == RECORD_SIZE
    assert result.raw_sha256 == sha256(raw).hexdigest()
    assert result.bundle_path.joinpath("records.bin").read_bytes() == raw
    assert _snapshot(source) == original


@pytest.mark.parametrize(("field", "value"), [("read_begin_start", False), ("read_begin_count", True)])
def test_publication_rejects_boolean_read_begin_fields_matching_numeric_zero_and_one(
    tmp_path: Path, field: str, value: bool
) -> None:
    source, _ = _single_quarantined_attempt(tmp_path, start=0, count=1)
    capture_before = set(_capture_root(tmp_path).iterdir())
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    _rewrite_attempt_field(store, source, "descriptor", field, value)
    original = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == original
    assert set(_capture_root(tmp_path).iterdir()) == capture_before


@pytest.mark.parametrize(
    ("field", "value", "read_start"),
    [
        ("start_sequence", -1, -1),
        ("start_sequence", False, 0),
    ],
)
def test_publication_rejects_negative_or_boolean_start_sequence_without_mutation(
    tmp_path: Path, field: str, value: int | bool, read_start: int
) -> None:
    source, _ = _single_quarantined_attempt(tmp_path, start=0, count=1)
    capture_before = set(_capture_root(tmp_path).iterdir())
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    _rewrite_attempt_field(store, source, "descriptor", field, value)
    _rewrite_attempt_field(store, source, "descriptor", "read_begin_start", read_start)
    original = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        store.publish_quarantined_prefix(source)

    assert _snapshot(source) == original
    assert set(_capture_root(tmp_path).iterdir()) == capture_before


def test_publication_detects_symlinked_canonical_output_on_replay(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    result = store.publish_quarantined_prefix(source)
    raw = result.bundle_path / "records.bin"
    external = tmp_path / "external-published-raw"
    raw.rename(external)
    raw.symlink_to(external)
    source_before = _snapshot(source)
    manifest_before = (result.bundle_path / "manifest.json").read_bytes()

    with pytest.raises(QuarantineOutputCollisionError):
        store.publish_quarantined_prefix(source)

    assert raw.is_symlink() and raw.readlink() == external
    assert external.read_bytes() == prefix
    assert (result.bundle_path / "manifest.json").read_bytes() == manifest_before
    assert _snapshot(source) == source_before


def test_publication_rejects_oversized_canonical_raw_file_on_replay(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    result = store.publish_quarantined_prefix(source)
    raw = result.bundle_path / "records.bin"
    raw.write_bytes(prefix + b"x")
    oversized = raw.read_bytes()
    source_before = _snapshot(source)

    with pytest.raises(QuarantineOutputCollisionError):
        store.publish_quarantined_prefix(source)

    assert raw.read_bytes() == oversized
    assert raw.read_bytes().startswith(prefix)
    assert (source / "records.bin").read_bytes() == prefix + tail
    assert _snapshot(source) == source_before


def test_publication_resolves_identical_rename_collision_from_public_filesystem_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    source_before = _snapshot(source)
    real_rename = quarantine_publish.os.rename
    injected = False

    def install_identical_destination_and_report_collision(
        src: str | bytes | PathLike[str] | PathLike[bytes],
        dst: str | bytes | PathLike[str] | PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal injected
        source_path = (
            Path(f"/proc/self/fd/{src_dir_fd}") / Path(fsdecode(src)).name
            if src_dir_fd is not None
            else Path(fsdecode(src))
        )
        destination_path = (
            Path(f"/proc/self/fd/{dst_dir_fd}") / Path(fsdecode(dst)).name
            if dst_dir_fd is not None
            else Path(fsdecode(dst))
        )
        if not injected and source_path.name.startswith(".") and source_path.name.endswith(".tmp"):
            copytree(source_path, destination_path)
            injected = True
            raise FileExistsError(EEXIST, "simulated identical rename collision")
        real_rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(quarantine_publish.os, "rename", install_identical_destination_and_report_collision)
    result = store.publish_quarantined_prefix(source)

    assert injected
    assert not result.deduplicated
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert _snapshot(source) == source_before
    assert (source / "records.bin").read_bytes() == prefix + tail


def test_failed_publication_mkdir_preserves_source_and_retry_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    source_before = _snapshot(source)
    capture = _capture_root(tmp_path)
    real_mkdir = quarantine_publish.os.mkdir
    sentinel = OSError(EIO, "injected publication temporary mkdir failure")

    def fail_capture_temporary_mkdir(
        path: str | bytes | PathLike[str] | PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if dir_fd is not None:
            raise sentinel
        real_mkdir(path, mode, dir_fd=dir_fd)

    with monkeypatch.context() as scoped:
        scoped.setattr(quarantine_publish.os, "mkdir", fail_capture_temporary_mkdir)
        with pytest.raises(OSError) as error:
            store.publish_quarantined_prefix(source)
        assert error.value is sentinel

    assert _snapshot(source) == source_before
    assert tuple(capture.iterdir()) == ()
    assert not tuple(capture.glob(".*.tmp"))
    result = store.publish_quarantined_prefix(source)
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert (source / "records.bin").read_bytes() == prefix + tail


def test_publication_failure_preserves_source_and_retry_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    source_before = _snapshot(source)
    capture = _capture_root(tmp_path)
    real_fsync = fsync
    sentinel = OSError(EIO, "injected pre-rename records sync failure")

    def fail_raw_output_sync(fd: int) -> None:
        try:
            path = Path(f"/proc/self/fd/{fd}").resolve()
        except OSError:
            path = Path()
        if path.name == "records.bin" and path.parent.parent == capture:
            raise sentinel
        real_fsync(fd)

    with monkeypatch.context() as scoped:
        scoped.setattr(quarantine_publish.os, "fsync", fail_raw_output_sync)
        with pytest.raises(OSError) as error:
            store.publish_quarantined_prefix(source)
        assert error.value is sentinel
    assert _snapshot(source) == source_before
    assert tuple(capture.iterdir()) == ()
    assert not tuple(capture.glob(".*.tmp"))
    result = store.publish_quarantined_prefix(source)

    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert _snapshot(source) == source_before
    assert (source / "records.bin").read_bytes() == prefix + tail


@pytest.mark.parametrize("start", [0, -1])
def test_publication_range_boundaries_preserve_exact_start_and_reject_negative_start(
    tmp_path: Path, start: int
) -> None:
    source, raw = _single_quarantined_attempt(tmp_path, start=0, count=1)
    capture_before = set(_capture_root(tmp_path).iterdir())
    if start == -1:
        store = StagingStore(tmp_path, _capture_root(tmp_path))
        _rewrite_attempt_field(store, source, "descriptor", "start_sequence", -1)
        _rewrite_attempt_field(store, source, "descriptor", "read_begin_start", -1)
    original = _snapshot(source)
    capture = _capture_root(tmp_path)

    if start < 0:
        with pytest.raises(QuarantinePublishError):
            StagingStore(tmp_path, capture).publish_quarantined_prefix(source)
        assert _snapshot(source) == original
        assert set(capture.iterdir()) == capture_before
        return

    result = StagingStore(tmp_path, capture).publish_quarantined_prefix(source)
    assert result.start_sequence == 0
    assert result.next_sequence == 1
    assert result.bundle_path.joinpath("records.bin").read_bytes() == raw
    assert _snapshot(source) == original


def test_capture_output_fifo_is_rejected_in_bounded_fork_with_candidate_provenance(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    raw = source / "records.bin"
    raw.unlink()
    raw_target = tmp_path / "records.bin.regular-evidence"
    raw_target.write_bytes(_record(1) + _record(2))
    mkfifo(raw)
    context = get_context("fork")
    output = context.Queue()
    expected_module_path = Path(cast(str, quarantine_publish.__file__)).resolve()
    process = context.Process(
        target=_publish_fifo_child,
        args=(str(source), str(tmp_path), str(_capture_root(tmp_path)), output),
    )

    try:
        process.start()
        process.join(2)
        assert not process.is_alive(), "FIFO publication blocked in the candidate child"
        module_path = Path(cast(str, output.get(timeout=1))).resolve()
        result = cast(tuple[str, ...], output.get(timeout=1))
        assert module_path == expected_module_path
        assert result[0] == "raised"
        assert result[1] == "QuarantinePublishError"
        assert source.is_dir()
        assert raw.is_fifo()
        assert raw_target.read_bytes() == _record(1) + _record(2)
        assert not tuple(_capture_root(tmp_path).iterdir())
    finally:
        if process.is_alive():
            process.terminate()
            process.join(1)
        if process.is_alive():
            process.kill()
            process.join(1)
        assert not process.is_alive()
        output.close()
        output.join_thread()
