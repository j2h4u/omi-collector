from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from json import dumps, loads
from os import PathLike
from pathlib import Path
from shutil import rmtree
from stat import S_IMODE
from typing import BinaryIO, cast

import pytest

import omi_collector.capture.adapters.quarantine_publish as quarantine_publish
from omi_collector.capture.adapters.quarantine_publish import (
    QuarantineOutputCollisionError,
    QuarantinePublishError,
    QuarantineSalvageDeferredError,
    publish_quarantined_prefix,
)
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError, StagingError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification

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


def _snapshot(source: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}


def _rewrite_object(path: Path, key: str, value: object) -> None:
    document = cast(dict[str, object], loads(path.read_text(encoding="utf-8")))
    if key.startswith("delete:"):
        del document[key.removeprefix("delete:")]
    else:
        document[key] = value
    path.write_text(dumps(document), encoding="utf-8")


class _ShortWritingStream:
    def __init__(self, path: Path, wrapped: BinaryIO, written: dict[str, int]) -> None:
        self.path = path
        self.wrapped = wrapped
        self.written = written

    def write(self, payload: bytes) -> int:
        if not payload:
            raise AssertionError(f"publisher attempted an empty write to {self.path.name}")
        short_payload = payload[:7]
        count = self.wrapped.write(short_payload)
        if count <= 0:
            raise AssertionError(f"publisher made no write progress for {self.path.name}")
        self.written[self.path.name] = self.written.get(self.path.name, 0) + count
        return count

    def flush(self) -> None:
        self.wrapped.flush()

    def fileno(self) -> int:
        return self.wrapped.fileno()

    def __enter__(self) -> _ShortWritingStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.wrapped.close()


def _patch_short_writes(monkeypatch: pytest.MonkeyPatch, written: dict[str, int]) -> None:
    real_open = cast(Callable[..., BinaryIO], Path.open)

    def observed_open(path: Path, mode: str = "r", *args: object, **kwargs: object) -> object:
        wrapped = real_open(path, mode, *args, **kwargs)
        if mode == "xb":
            return _ShortWritingStream(path, wrapped, written)
        return wrapped

    monkeypatch.setattr(Path, "open", observed_open)


def test_publishes_only_authenticated_prefix_and_leaves_source_unchanged(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    source_raw = source.joinpath("records.bin").read_bytes()

    result = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)

    assert not result.deduplicated
    assert result.record_count == 1
    assert result.bundle_path == _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert source.joinpath("records.bin").read_bytes() == source_raw == prefix + tail
    manifest = cast(dict[str, object], loads(result.bundle_path.joinpath("manifest.json").read_text(encoding="utf-8")))
    receipt = cast(dict[str, object], loads(result.bundle_path.joinpath("receipt.json").read_text(encoding="utf-8")))
    assert manifest == {
        "schema_version": 2,
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": RECORD_SIZE,
        "raw_sha256": sha256(prefix).hexdigest(),
    }
    attempt_id = cast(dict[str, object], loads(source.joinpath("attempt.json").read_text(encoding="utf-8")))[
        "attempt_id"
    ]
    assert receipt == {"attempt_id": attempt_id, "raw_sha256": sha256(prefix).hexdigest(), "status": "sealed"}
    assert set(result.bundle_path.iterdir()) == {
        result.bundle_path / "records.bin",
        result.bundle_path / "manifest.json",
        result.bundle_path / "receipt.json",
    }
    assert not tuple((_capture_root(tmp_path)).glob(".*.tmp"))
    duplicate = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)
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
    result = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)

    assert requested_modes == [0o770]
    mode = S_IMODE(result.bundle_path.stat().st_mode)
    assert mode & 0o700 == 0o700
    assert mode & 0o007 == 0


@pytest.mark.parametrize("name", ["attempt.json", "checkpoint.json", "records.bin"])
def test_rejects_symlinked_source_authority(tmp_path: Path, name: str) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    authority = source / name
    target = tmp_path / f"{name}.target"
    authority.rename(target)
    authority.symlink_to(target)

    with pytest.raises(QuarantinePublishError, match=r"missing or unreadable|regular file"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)

    assert not tuple((_capture_root(tmp_path)).glob("100-*"))


def test_rejects_nonidentical_existing_bundle_collision(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)
    (destination / "records.bin").write_bytes(_record(9))
    (destination / "manifest.json").write_text("{}", encoding="utf-8")
    (destination / "receipt.json").write_text("{}", encoding="utf-8")

    with pytest.raises(QuarantineOutputCollisionError, match="ordinary bundle collision"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)


def test_noncanonical_existing_bundle_is_retryable_not_a_conflict(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)

    with pytest.raises(OSError, match="not yet canonical"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)


def test_rejects_unknown_attempt_field(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    attempt = cast(dict[str, object], loads(source.joinpath("attempt.json").read_text(encoding="utf-8")))
    attempt["unexpected"] = True
    source.joinpath("attempt.json").write_text(
        dumps(attempt),
        encoding="utf-8",
    )

    with pytest.raises(QuarantinePublishError, match="schema is not exact"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths)


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
        paths = StagingStore(tmp_path, capture_root).paths
        publish_quarantined_prefix(source, paths)


def test_rejects_capture_root_symlink_swap_before_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    real_publish = quarantine_publish._publish_atomic

    def swap_before_atomic(  # noqa: PLR0913
        destination: Path,
        manifest: dict[str, object],
        receipt: dict[str, object],
        raw_source: Path,
        prefix_size: int,
        *,
        should_defer: Callable[[], bool],
    ) -> None:
        backup = tmp_path / "capture-backup"
        capture_root.rename(backup)
        capture_root.symlink_to(outside, target_is_directory=True)
        real_publish(destination, manifest, receipt, raw_source, prefix_size, should_defer=should_defer)

    monkeypatch.setattr(quarantine_publish, "_publish_atomic", swap_before_atomic)
    with pytest.raises(OSError, match="temporarily unavailable"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, capture_root).paths)
    assert not tuple(outside.iterdir())


def test_manual_publish_respects_collector_device_lock(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    store = StagingStore(tmp_path, capture_root)

    with store.device_lock(), pytest.raises(DeviceAlreadyRunningError):
        publish_quarantined_prefix(source, StagingStore(tmp_path, capture_root).paths)

    assert not tuple((capture_root).glob(".*.tmp"))


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
        publish_quarantined_prefix(source, store.paths, should_defer=defer_after_first_chunk)

    assert {path.name: path.read_bytes() for path in source.iterdir()} == before
    assert not tuple((_capture_root(tmp_path)).glob(".*.tmp"))
    result = publish_quarantined_prefix(source, store.paths)
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
    result = publish_quarantined_prefix(source, store.paths, should_defer=lambda: renamed)

    assert renamed
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix


@pytest.mark.parametrize(
    ("filename", "field", "value"),
    [
        ("attempt.json", "attempt_id", "f" * 32),
        ("attempt.json", "schema_version", 3),
        ("attempt.json", "start_sequence", -1),
        ("attempt.json", "packet_count", 0),
        ("attempt.json", "packet_count", True),
        ("attempt.json", "record_size", RECORD_SIZE + 1),
        ("attempt.json", "read_begin_start", "100"),
        ("attempt.json", "read_begin_start", 99),
        ("attempt.json", "read_begin_count", False),
        ("attempt.json", "read_begin_count", 2),
        ("attempt.json", "unexpected", "field"),
    ],
)
def test_invalid_attempt_descriptor_is_preserved_and_valid_retry_publishes(
    tmp_path: Path, filename: str, field: str, value: object
) -> None:
    case = _case_path(tmp_path, f"attempt-{filename}-{field}-{value}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    original = _snapshot(source)
    changed = source / filename
    _rewrite_object(changed, field, value)
    invalid_evidence = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        publish_quarantined_prefix(source, store.paths)

    assert _snapshot(source) == invalid_evidence
    assert not tuple(_capture_root(case).iterdir())
    changed.write_bytes(original[filename])
    result = publish_quarantined_prefix(source, store.paths)
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
    checkpoint = source / "checkpoint.json"
    _rewrite_object(checkpoint, field, value)
    invalid_evidence = _snapshot(source)

    with pytest.raises(QuarantinePublishError):
        publish_quarantined_prefix(source, store.paths)

    assert _snapshot(source) == invalid_evidence
    assert not tuple(_capture_root(case).iterdir())
    checkpoint.write_bytes(original["checkpoint.json"])
    result = publish_quarantined_prefix(source, store.paths)
    assert result.record_count == 1
    assert _snapshot(source) == original


@pytest.mark.parametrize("conflict", ["records.bin", "manifest.json", "receipt.json"])
def test_existing_bundle_conflicts_are_typed_and_preserve_both_sides(tmp_path: Path, conflict: str) -> None:
    case = _case_path(tmp_path, f"conflict-{conflict}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    result = publish_quarantined_prefix(source, store.paths)
    source_before = _snapshot(source)
    destination = result.bundle_path
    conflict_file = destination / conflict

    if conflict == "records.bin":
        conflict_file.write_bytes(_record(9))
    else:
        document = cast(dict[str, object], loads(conflict_file.read_text(encoding="utf-8")))
        if conflict == "manifest.json":
            document["start_sequence"] = 200
            document["next_sequence"] = 201
        else:
            original_id = cast(str, document["attempt_id"])
            document["attempt_id"] = "0" * 32 if original_id != "0" * 32 else "1" * 32
        conflict_file.write_text(dumps(document), encoding="utf-8")
    destination_after_conflict = _snapshot(destination)

    with pytest.raises(QuarantineOutputCollisionError):
        publish_quarantined_prefix(source, store.paths)

    assert _snapshot(source) == source_before
    assert _snapshot(destination) == destination_after_conflict


@pytest.mark.parametrize("missing", ["records.bin", "manifest.json", "receipt.json"])
def test_incomplete_existing_destination_is_retryable_and_preserved(tmp_path: Path, missing: str) -> None:
    case = _case_path(tmp_path, f"incomplete-{missing}")
    source, _, _ = _quarantined_attempt(case)
    store = StagingStore(case, _capture_root(case))
    result = publish_quarantined_prefix(source, store.paths)
    source_before = _snapshot(source)
    destination_before = _snapshot(result.bundle_path)
    (result.bundle_path / missing).unlink()
    destination_after_removal = _snapshot(result.bundle_path)

    with pytest.raises(OSError, match="not yet canonical") as error:
        publish_quarantined_prefix(source, store.paths)

    assert not isinstance(error.value, QuarantineOutputCollisionError)
    assert _snapshot(source) == source_before
    assert _snapshot(result.bundle_path) == destination_after_removal
    (result.bundle_path / missing).write_bytes(destination_before[missing])
    assert publish_quarantined_prefix(source, store.paths).deduplicated
    assert _snapshot(source) == source_before


def test_repeat_publication_ignores_opaque_source_tail_and_preserves_both_sides(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    first = publish_quarantined_prefix(source, store.paths)
    source_before = _snapshot(source)
    destination_before = _snapshot(first.bundle_path)

    duplicate = publish_quarantined_prefix(source, store.paths)

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
    _patch_short_writes(monkeypatch, written)
    result = publish_quarantined_prefix(source, store.paths)

    digest = sha256(prefix).hexdigest()
    attempt_id = cast(str, loads(source_before["attempt.json"])["attempt_id"])
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert loads(result.bundle_path.joinpath("manifest.json").read_text(encoding="utf-8")) == {
        "schema_version": 2,
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": RECORD_SIZE,
        "raw_sha256": digest,
    }
    assert loads(result.bundle_path.joinpath("receipt.json").read_text(encoding="utf-8")) == {
        "attempt_id": attempt_id,
        "raw_sha256": digest,
        "status": "sealed",
    }
    assert written == {
        "records.bin": len(prefix),
        "manifest.json": len((result.bundle_path / "manifest.json").read_bytes()),
        "receipt.json": len((result.bundle_path / "receipt.json").read_bytes()),
    }
    assert _snapshot(source) == source_before
    assert source.joinpath("records.bin").read_bytes() == prefix + tail
