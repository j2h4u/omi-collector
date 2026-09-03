from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from json import dumps, loads
from os import PathLike
from pathlib import Path
from shutil import rmtree
from stat import S_IMODE
from typing import cast

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


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _quarantined_attempt(spool: Path) -> tuple[Path, bytes, bytes]:
    attempt = StagingStore(spool, spool.parent / "captures").prepare_streaming_attempt("omi_cv1", 100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    prefix = _record(1)
    tail = _record(2)
    attempt.append_record(0, 100, prefix)
    attempt.checkpoint()
    attempt.append_record(1, 101, tail)
    attempt.close(durable=True)
    source = StagingStore(spool, spool.parent / "captures").quarantine_attempt_source("omi_cv1", attempt.attempt_id)
    return source, prefix, tail


def test_publishes_only_authenticated_prefix_and_leaves_source_unchanged(tmp_path: Path) -> None:
    source, prefix, tail = _quarantined_attempt(tmp_path)
    source_raw = source.joinpath("records.bin").read_bytes()

    result = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")

    assert not result.deduplicated
    assert result.record_count == 1
    assert result.bundle_path == _capture_root(tmp_path) / "omi_cv1" / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
    assert source.joinpath("records.bin").read_bytes() == source_raw == prefix + tail
    manifest = cast(dict[str, object], loads(result.bundle_path.joinpath("manifest.json").read_text(encoding="utf-8")))
    receipt = cast(dict[str, object], loads(result.bundle_path.joinpath("receipt.json").read_text(encoding="utf-8")))
    assert manifest == {
        "device_slug": "omi_cv1",
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
    assert not tuple((_capture_root(tmp_path) / "omi_cv1").glob(".*.tmp"))
    duplicate = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")
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
    result = publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")

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
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")

    assert not tuple((_capture_root(tmp_path) / "omi_cv1").glob("100-*"))


def test_rejects_nonidentical_existing_bundle_collision(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / "omi_cv1" / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)
    (destination / "records.bin").write_bytes(_record(9))
    (destination / "manifest.json").write_text("{}", encoding="utf-8")
    (destination / "receipt.json").write_text("{}", encoding="utf-8")

    with pytest.raises(QuarantineOutputCollisionError, match="ordinary bundle collision"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")


def test_noncanonical_existing_bundle_is_retryable_not_a_conflict(tmp_path: Path) -> None:
    source, prefix, _ = _quarantined_attempt(tmp_path)
    destination = _capture_root(tmp_path) / "omi_cv1" / f"100-101-{sha256(prefix).hexdigest()[:16]}"
    destination.mkdir(parents=True)

    with pytest.raises(OSError, match="not yet canonical"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")


def test_rejects_unknown_attempt_field(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    attempt = cast(dict[str, object], loads(source.joinpath("attempt.json").read_text(encoding="utf-8")))
    attempt["unexpected"] = True
    source.joinpath("attempt.json").write_text(
        dumps(attempt),
        encoding="utf-8",
    )

    with pytest.raises(QuarantinePublishError, match="schema is not exact"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, _capture_root(tmp_path)).paths, "omi_cv1")


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
        publish_quarantined_prefix(source, paths, "omi_cv1")


def test_rejects_capture_root_symlink_swap_before_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    real_publish = quarantine_publish._publish_atomic

    def swap_before_atomic(
        destination: Path,
        manifest: dict[str, object],
        receipt: dict[str, object],
        raw: bytes,
        *,
        should_defer: Callable[[], bool],
    ) -> None:
        backup = tmp_path / "capture-backup"
        capture_root.rename(backup)
        capture_root.symlink_to(outside, target_is_directory=True)
        real_publish(destination, manifest, receipt, raw, should_defer=should_defer)

    monkeypatch.setattr(quarantine_publish, "_publish_atomic", swap_before_atomic)
    with pytest.raises(OSError, match="temporarily unavailable"):
        publish_quarantined_prefix(source, StagingStore(tmp_path, capture_root).paths, "omi_cv1")
    assert not (outside / "omi_cv1").exists()


def test_manual_publish_respects_collector_device_lock(tmp_path: Path) -> None:
    source, _, _ = _quarantined_attempt(tmp_path)
    capture_root = _capture_root(tmp_path)
    store = StagingStore(tmp_path, capture_root)

    with store.device_lock("omi_cv1"), pytest.raises(DeviceAlreadyRunningError):
        publish_quarantined_prefix(source, StagingStore(tmp_path, capture_root).paths, "omi_cv1")

    assert not tuple((capture_root / "omi_cv1").glob(".*.tmp"))


def test_hash_deferral_preserves_source_and_retries_byte_identically(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    count = quarantine_publish.DEFAULT_CONFIG.durability.io_chunk_bytes // RECORD_SIZE + 2
    payload = bytes(index % 251 for index in range(count * RECORD_SIZE))
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    attempt.accept_chunk(100, memoryview(payload))
    attempt.checkpoint()
    attempt_id = attempt.attempt_id
    attempt.close(durable=True)
    source = store.quarantine_attempt_source("omi_cv1", attempt_id)
    before = {path.name: path.read_bytes() for path in source.iterdir()}
    defer_checks = 0

    def defer_after_first_chunk() -> bool:
        nonlocal defer_checks
        defer_checks += 1
        return defer_checks >= 3

    with pytest.raises(QuarantineSalvageDeferredError):
        publish_quarantined_prefix(source, store.paths, "omi_cv1", should_defer=defer_after_first_chunk)

    assert {path.name: path.read_bytes() for path in source.iterdir()} == before
    assert not tuple((_capture_root(tmp_path) / "omi_cv1").glob(".*.tmp"))
    result = publish_quarantined_prefix(source, store.paths, "omi_cv1")
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
    result = publish_quarantined_prefix(source, store.paths, "omi_cv1", should_defer=lambda: renamed)

    assert renamed
    assert result.bundle_path.joinpath("records.bin").read_bytes() == prefix
