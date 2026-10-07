"""Focused quarantine staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from hashlib import sha256
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters import staging_filesystem
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    DeviceAlreadyRunningError,
    PendingAttemptError,
    StagingError,
)
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.quarantine_machine import QuarantineState
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
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _started_streaming_attempt(tmp_path: Path, *, count: int = 2, fsync_fn: Callable[[int], None] = fsync):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_fn).prepare_streaming_attempt(100, count)
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


def test_device_lock_quarantines_hard_crash_publication_leftover(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    capture_root.mkdir(parents=True)
    leftover = capture_root / ".100-101-deadbeef.tmp"
    leftover.mkdir()
    payload = _record(1)
    (leftover / "records.bin").write_bytes(payload)

    with StagingStore(spool, capture_root).device_lock():
        pass

    assert not leftover.exists()
    quarantined = tuple(path for path in (spool / "quarantine").glob("capture-temporary-*") if path.is_dir())
    assert len(quarantined) == 1
    assert (quarantined[0] / "records.bin").read_bytes() == payload
    assert (quarantined[0] / "unprocessable.json").is_file()


def test_device_lock_finalizes_complete_capture_local_publication_temporary(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.accept_chunk(100, _record(1))
    result = attempt.seal(DoneNotification(0, 101))
    temporary = result.bundle_path.with_name(f".{result.bundle_path.name}.{'a' * 32}.tmp")
    result.bundle_path.replace(temporary)

    with StagingStore(spool, capture_root).device_lock():
        pass

    assert result.bundle_path.is_dir()
    assert not temporary.exists()
    assert (result.bundle_path / "records.bin").read_bytes() == _record(1)


def test_terminal_quarantine_expires_at_exact_retention_and_preserves_live_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    config = CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0))
    store = StagingStore(tmp_path, _capture_root(tmp_path), config=config)
    root = tmp_path / "quarantine"
    published = root / "published-source"
    unprocessable = root / "unprocessable-source"
    published.mkdir(parents=True)
    unprocessable.mkdir()
    (published / "records.bin").write_bytes(b"published evidence")
    (unprocessable / "records.bin").write_bytes(b"unprocessable evidence")
    store.mark_quarantine_published(published)
    store.mark_quarantine_unprocessable(unprocessable, "strict proof failed")
    live = root / "live-source"
    live.mkdir()
    (live / "records.bin").write_bytes(b"live evidence")
    malformed = root / "malformed-source"
    malformed.mkdir()
    (malformed / "unprocessable.json").write_bytes(b"{")

    def contents() -> dict[Path, bytes]:
        return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}

    original_contents = contents()
    now += 72_000_000_000 - 1
    assert store.sweep_terminal_quarantine() == ()
    assert contents() == original_contents

    now += 1

    assert set(store.sweep_terminal_quarantine()) == {published, unprocessable}
    assert not published.exists()
    assert not unprocessable.exists()
    assert contents() == {
        path: payload
        for path, payload in original_contents.items()
        if path.parts[0] in {"live-source", "malformed-source"}
    }


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("version", True),
        ("version", 2),
        ("state", "published"),
        ("classified_at_unix_ns", True),
        ("classified_at_unix_ns", "1000000000"),
        ("classified_at_unix_ns", -1),
        ("reason", ""),
        ("reason", None),
        ("original_name", ""),
        ("original_name", 7),
    ],
)
def test_terminal_quarantine_rejects_malformed_marker_fields_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, invalid_value: object
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    attempt = store.prepare_streaming_attempt(100, 2)
    try:
        attempt.record_read_begin(ReadBeginNotification(100, 2))
        attempt.accept_chunk(100, _record(1))
        attempt.checkpoint()
    finally:
        attempt.close(durable=True)
    entry = store.quarantine_attempt_source(attempt.attempt_id)
    store.mark_quarantine_unprocessable(entry, "authenticated fixture")
    marker_path = entry / "unprocessable.json"
    valid_marker = cast(dict[str, object], loads(marker_path.read_text(encoding="utf-8")))
    marker = {**valid_marker, field: invalid_value}
    marker_bytes = dumps(marker).encode()
    marker_path.write_bytes(marker_bytes)
    entry_contents = {path.relative_to(entry): path.read_bytes() for path in entry.rglob("*") if path.is_file()}
    now += 72_000_000_000

    assert store.sweep_terminal_quarantine() == ()
    assert set(store.quarantined_attempts()) == {entry}
    assert entry.exists()
    assert marker_path.read_bytes() == marker_bytes
    assert {path.relative_to(entry): path.read_bytes() for path in entry.rglob("*") if path.is_file()} == entry_contents


def test_quarantine_sidecar_requires_exact_or_uuid_suffixed_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    for name in (
        "valid-exact",
        "valid-uuid",
        "wrong-prefix",
        "short-suffix",
        "nonhex-suffix",
        "empty-original",
        "missing-original",
        "nonstring-original",
    ):
        (attempts / name).write_bytes(f"evidence:{name}".encode())
    by_original_name = {
        cast(dict[str, object], loads(Path(f"{entry}.json").read_text(encoding="utf-8")))["original_name"]: entry
        for entry in store.quarantine_pending("opaque fixture")
    }
    valid_exact = by_original_name["valid-exact"]
    Path(f"{valid_exact}.json").write_text(
        dumps(
            {
                **cast(dict[str, object], loads(Path(f"{valid_exact}.json").read_text(encoding="utf-8"))),
                "original_name": valid_exact.name,
            }
        ),
        encoding="utf-8",
    )

    by_original_name["short-suffix"].rename(by_original_name["short-suffix"].with_name(f"short-suffix-{'a' * 31}"))
    Path(f"{by_original_name['short-suffix']}.json").rename(
        Path(f"{by_original_name['short-suffix'].with_name(f'short-suffix-{"a" * 31}')}.json")
    )
    by_original_name["nonhex-suffix"].rename(by_original_name["nonhex-suffix"].with_name(f"nonhex-suffix-{'g' * 32}"))
    Path(f"{by_original_name['nonhex-suffix']}.json").rename(
        Path(f"{by_original_name['nonhex-suffix'].with_name(f'nonhex-suffix-{"g" * 32}')}.json")
    )
    for original_name, marker_value in {
        "wrong-prefix": "different-prefix",
        "empty-original": "",
        "nonstring-original": 7,
    }.items():
        marker_path = Path(f"{by_original_name[original_name]}.json")
        marker_path.write_text(
            dumps(
                {
                    **cast(dict[str, object], loads(marker_path.read_text(encoding="utf-8"))),
                    "original_name": marker_value,
                }
            ),
            encoding="utf-8",
        )
    Path(f"{by_original_name['missing-original']}.json").write_text(
        dumps(
            {
                key: value
                for key, value in cast(
                    dict[str, object],
                    loads(Path(f"{by_original_name['missing-original']}.json").read_text(encoding="utf-8")),
                ).items()
                if key != "original_name"
            }
        ),
        encoding="utf-8",
    )
    invalid_entries = {
        original_name: (
            by_original_name[original_name].with_name(f"short-suffix-{'a' * 31}")
            if original_name == "short-suffix"
            else by_original_name[original_name].with_name(f"nonhex-suffix-{'g' * 32}")
            if original_name == "nonhex-suffix"
            else by_original_name[original_name]
        )
        for original_name in (
            "wrong-prefix",
            "short-suffix",
            "nonhex-suffix",
            "empty-original",
            "missing-original",
            "nonstring-original",
        )
    }
    invalid_bytes = {
        entry: (entry.read_bytes(), Path(f"{entry}.json").read_bytes()) for entry in invalid_entries.values()
    }
    now += 72_000_000_000

    assert set(store.sweep_terminal_quarantine()) == {valid_exact, by_original_name["valid-uuid"]}
    assert not valid_exact.exists()
    assert not Path(f"{valid_exact}.json").exists()
    assert not by_original_name["valid-uuid"].exists()
    assert {
        entry: (entry.read_bytes(), Path(f"{entry}.json").read_bytes()) for entry in invalid_entries.values()
    } == invalid_bytes


def test_contradictory_quarantine_markers_remain_live_at_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    entry = tmp_path / "quarantine" / "contradictory"
    entry.mkdir(parents=True)
    evidence = entry / "records.bin"
    evidence.write_bytes(b"contradictory evidence")
    published = entry / "published.json"
    published.write_text(dumps({"version": 1, "state": "published", "published_at_unix_ns": now}), encoding="utf-8")
    unprocessable = entry / "unprocessable.json"
    unprocessable.write_text(
        dumps({"version": 1, "state": "unprocessable", "classified_at_unix_ns": now, "reason": "unverified"}),
        encoding="utf-8",
    )
    original = {path: path.read_bytes() for path in (evidence, published, unprocessable)}
    now += 72_000_000_000

    assert store.sweep_terminal_quarantine() == ()
    assert set(store.quarantined_attempts()) == {entry}
    assert {path: path.read_bytes() for path in (evidence, published, unprocessable)} == original


def test_device_lock_rejects_symlink_publishing_root(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    outside = tmp_path / "outside-publishing"
    outside.mkdir()
    capture_root.symlink_to(outside, target_is_directory=True)

    with (
        pytest.raises(StagingError, match="capture root"),
        StagingStore(spool, capture_root).device_lock(),
    ):
        pass


def test_terminal_retired_marker_ignores_missing_destination_then_expires_only_its_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    config = CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0))
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", wall_clock_ns)
    store = StagingStore(tmp_path, _capture_root(tmp_path), config=config)
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    result = attempt.publish_prefix()
    assert result is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)

    marker = cast(dict[str, object], loads((attempt.path / "terminal-retired.json").read_text(encoding="utf-8")))
    assert marker == {
        "version": 1,
        "state": "terminal-retired",
        "terminalized_at_unix_ns": now,
    }
    rmtree(result.bundle_path)
    active = store.prepare_streaming_attempt(200, 1)
    quarantine = tmp_path / "quarantine"
    quarantine.mkdir(parents=True)
    preserved = quarantine / "preserved"
    preserved.write_text("evidence", encoding="utf-8")

    assert store.pending_attempts() == (active.descriptor,)
    assert store.sweep_terminal_retired() == ()
    assert attempt.path.exists()
    now += 72_000_000_000
    assert store.sweep_terminal_retired() == (attempt.path,)
    assert not attempt.path.exists()
    assert active.path.exists()
    assert preserved.read_text(encoding="utf-8") == "evidence"


def test_terminal_retired_sweep_does_not_rehash_records_before_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(
            durability=DurabilityConfig(io_chunk_bytes=1024),
            staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        ),
    )
    attempt = store.prepare_streaming_attempt(100, 5)
    attempt.record_read_begin(ReadBeginNotification(100, 5))
    attempt.accept_chunk(100, memoryview(b"x" * (5 * RECORD_SIZE)))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)
    now += 1_000_000_000
    monkeypatch.setattr(
        quarantine_module,
        "_published_prefix",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retired records were rehashed")),
    )
    assert store.sweep_terminal_retired() == (attempt.path,)
    assert not attempt.path.exists()


def test_quarantine_attempt_source_preserves_only_existing_attempt_files(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    attempt_id = attempt.attempt_id
    expected_files = {path.name for path in attempt.path.iterdir()}
    attempt.close(durable=True)

    destination = store.quarantine_attempt_source(attempt_id)

    assert not attempt.path.exists()
    assert destination.name.startswith(f"{attempt_id}-")
    assert {path.name for path in destination.iterdir()} == expected_files
    assert tuple(destination.parent.iterdir()) == (destination,)
    assert not store.pending_attempts()


def test_streaming_partial_close_is_preserved_and_blocks_pending(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.accept_chunk(100, _record(1))
    attempt.close()

    reopened = StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)
    recovery = reopened.recover()
    assert not recovery.clean
    assert recovery.raw_bytes == RECORD_SIZE
    with pytest.raises(AttemptStateError, match="preserved partial evidence"):
        reopened.accept_chunk(100, _record(2))
    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending()
    assert attempt.path.exists()


def test_pending_attempts_fail_closed_on_unattributed_malformed_evidence(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt(100, 2)

    assert store.pending_attempts() == (matching.descriptor,)
    with pytest.raises(PendingAttemptError, match="blocks another READ"):
        store.assert_no_pending()

    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    (malformed / "attempt.json").write_text("{", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts()
    moved = store.quarantine_pending("unattributed malformed evidence")
    assert len(moved) == 2
    assert not matching.path.exists()
    assert not malformed.exists()


def test_pending_attempts_fail_closed_on_invalid_malformed_attribution(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    (malformed / "attempt.json").write_text(
        dumps({"attempt_id": malformed.name, "schema_version": 2}), encoding="utf-8"
    )

    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts()
    moved = store.quarantine_pending("invalid descriptor attribution")
    assert len(moved) == 1
    assert not malformed.exists()
    assert (moved[0].with_name(f"{moved[0].name}.json")).is_file()


def test_attributable_malformed_evidence_is_quarantined_without_read_authorization(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    descriptor = dumps({"attempt_id": malformed.name, "schema_version": 2})
    (malformed / "attempt.json").write_text(descriptor, encoding="utf-8")
    (malformed / "records.bin").write_bytes(b"unverified bytes")
    before = {path.name: path.read_bytes() for path in malformed.iterdir()}

    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts()
    moved = store.quarantine_pending("malformed descriptor")

    assert len(moved) == 1
    assert not malformed.exists()
    assert {path.name: path.read_bytes() for path in moved[0].iterdir() if path.name != "unprocessable.json"} == before
    store.assert_no_pending()


def test_quarantine_pending_moves_all_blockers_and_preserves_retired(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt(100, 2)
    unrelated = store.prepare_streaming_attempt(200, 2)
    published = store.prepare_streaming_attempt(300, 1)
    assert published.publish_prefix() is None
    published.close(durable=True)
    store.terminalize_prefix_attempt(published.attempt_id)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    (malformed / "attempt.json").write_text("{", encoding="utf-8")

    moved = store.quarantine_pending("manual recovery")

    assert len(moved) == 3
    assert not matching.path.exists()
    assert not malformed.exists()
    assert not unrelated.path.exists()
    assert published.path.exists()
    assert (published.path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()


def test_direct_quarantine_function_uses_concrete_filesystem(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt(100, 1)

    moved = quarantine_module.quarantine_pending(store._filesystem, "direct call")

    assert len(moved) == 1
    assert moved[0].parent == tmp_path / "quarantine"
    assert not matching.path.exists()


def test_quarantine_a_does_not_move_unattributed_entry_during_b_lease(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt(100, 2)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    before = malformed / "evidence"
    before.write_bytes(b"unattributed")

    with store.device_lock(), pytest.raises(DeviceAlreadyRunningError):
        store.quarantine_pending("race recovery")

    assert matching.path.exists()
    assert malformed.is_dir()
    assert before.read_bytes() == b"unattributed"


def test_quarantine_pending_moves_symlink_without_following_it(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    (tmp_path / "attempts").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "evidence"
    target.write_bytes(b"evidence")
    link = tmp_path / "attempts" / "link"
    link.symlink_to(target)

    moved = store.quarantine_pending("symlink recovery")

    assert len(moved) == 1
    assert moved[0].is_symlink()
    assert moved[0].readlink() == target
    assert not link.is_symlink()
    assert target.read_bytes() == b"evidence"


def test_opaque_quarantine_entries_expire_after_terminal_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    opaque_dir = tmp_path / "attempts" / "opaque-dir"
    opaque_dir.mkdir(parents=True)
    target = tmp_path / "outside"
    target.write_bytes(b"preserve")
    opaque_link = tmp_path / "attempts" / "opaque-link"
    opaque_link.symlink_to(target)

    moved = store.quarantine_pending("opaque evidence")
    now += 72_000_000_000

    assert set(store.sweep_terminal_quarantine()) == set(moved)
    assert not opaque_dir.exists()
    assert not opaque_link.is_symlink()
    assert target.read_bytes() == b"preserve"


def test_quarantine_pending_rejects_active_lease(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))

    with store.device_lock(), pytest.raises(DeviceAlreadyRunningError):
        store.quarantine_pending("manual recovery")


def test_pending_checks_empty_and_rejects_malformed_roots(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    store.assert_no_pending()

    partial = tmp_path / "attempts"
    partial.write_text("not a directory", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="not a directory"):
        store.pending_attempts()
    partial.unlink()
    target = tmp_path / "partial-target"
    target.mkdir()
    partial.symlink_to(target, target_is_directory=True)
    with pytest.raises(PendingAttemptError, match="not a directory"):
        store.pending_attempts()


@pytest.mark.parametrize("root_kind", ["file", "symlink"])
def test_quarantine_pending_moves_unsafe_partial_root_and_recreates_it(tmp_path: Path, root_kind: str) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    partial = tmp_path / "attempts"
    target = tmp_path / "outside"
    if root_kind == "file":
        partial.write_bytes(b"unsafe root")
    else:
        target.mkdir()
        (target / "evidence").write_bytes(b"keep me")
        partial.symlink_to(target, target_is_directory=True)

    if root_kind == "symlink":
        with pytest.raises(StagingError, match="attempts root must not be a symlink"):
            store.quarantine_pending("root recovery")
        return

    moved = store.quarantine_pending("root recovery")

    assert len(moved) == 1
    assert moved[0].name.startswith("attempts-")
    assert moved[0].is_symlink() is (root_kind == "symlink")
    if root_kind == "file":
        assert moved[0].read_bytes() == b"unsafe root"
    else:
        assert moved[0].readlink() == target
        assert (target / "evidence").read_bytes() == b"keep me"
    assert partial.is_dir()
    assert not partial.is_symlink()
    assert tuple(partial.iterdir()) == ()
    assert store.pending_attempts() == ()


def test_pending_sealed_looking_partial_requires_the_real_destination_bundle(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    raw_hash = sha256(_record(1)).hexdigest()
    (attempt.path / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (attempt.path / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")

    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending()


def test_pending_partial_with_a_corrupt_destination_bundle_remains_blocking(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    raw_hash = sha256(_record(1)).hexdigest()
    destination = attempt._bundle_path(raw_hash)
    destination.mkdir(parents=True)
    (destination / "records.bin").write_bytes(_record(9))
    (destination / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (destination / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")
    (attempt.path / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (attempt.path / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")

    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending()


def test_device_lock_is_exclusive_and_released_after_an_error(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    with store.device_lock(), pytest.raises(DeviceAlreadyRunningError), store.device_lock():
        pass
    with store.device_lock():
        pass


def test_device_lock_rejects_forged_cross_store_expired_and_reused_leases(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    other = StagingStore(tmp_path / "other", tmp_path / "other-captures")
    forged = DeviceLock(store._filesystem)
    cross_store = DeviceLock(other._filesystem)

    with pytest.raises(AttemptStateError, match="active spool lock"):
        forged.require_active()
    with store.device_lock() as active:
        active.require_active()
        with pytest.raises(AttemptStateError, match="active spool lock"):
            forged.require_active()
        with pytest.raises(AttemptStateError, match="active spool lock"):
            cross_store.require_active()
    with pytest.raises(AttemptStateError, match="active spool lock"):
        active.require_active()


def test_pending_ignores_non_directory_entries_and_reports_iterdir_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    partial = tmp_path / "attempts"
    partial.mkdir()
    (partial / "evidence.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts()

    entry = partial / "linked-evidence"
    entry_target = tmp_path / "entry-target"
    entry_target.mkdir()
    entry.symlink_to(entry_target, target_is_directory=True)
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts()
    moved = store.quarantine_pending("opaque local evidence")
    assert len(moved) == 2
    assert all(path.parent == tmp_path / "quarantine" for path in moved)
    assert all(not path.is_symlink() for path in partial.iterdir())
    assert entry_target.is_dir()

    original_iterdir = Path.iterdir

    def fail_iterdir(path: Path):
        if path == partial:
            raise OSError("simulated directory listing failure")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_iterdir)
    with pytest.raises(PendingAttemptError, match="cannot be inspected"):
        store.pending_attempts()


def test_quarantine_pending_rejects_whitespace_reason_before_moving_closed_partial(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    attempt.close(durable=True)
    before = {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()}

    with pytest.raises(AttemptStateError):
        store.quarantine_pending(" \t ")

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()} == before
    assert not (tmp_path / "quarantine").exists()
    moved = store.quarantine_pending("operator requested recovery")
    assert len(moved) == 1
    assert not attempt.path.exists()


def test_terminal_retired_sweep_keeps_opaque_attempt_file_and_removes_only_expired_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 10_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    opaque = tmp_path / "attempts" / "opaque-evidence"
    opaque.parent.mkdir(parents=True)
    opaque.write_bytes(b"opaque file evidence")
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)
    now += 1_000_000_000

    assert store.sweep_terminal_retired() == (attempt.path,)
    assert opaque.read_bytes() == b"opaque file evidence"
    assert not attempt.path.exists()


def test_terminal_quarantine_sweep_defers_without_touching_aged_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 10_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    entry = tmp_path / "quarantine" / "aged-evidence"
    entry.mkdir(parents=True)
    (entry / "records.bin").write_bytes(b"terminal evidence")
    store.mark_quarantine_unprocessable(entry, "not publishable")
    now += 1_000_000_000
    before = {path.relative_to(entry): path.read_bytes() for path in entry.rglob("*") if path.is_file()}

    assert tuple(store.sweep_terminal_quarantine(should_defer=lambda: True)) == ()

    assert entry.is_dir()
    assert {path.relative_to(entry): path.read_bytes() for path in entry.rglob("*") if path.is_file()} == before


def test_quarantine_state_reports_missing_owned_path_as_invalid_evidence(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))

    assert (
        store.quarantine_state(tmp_path / "quarantine" / "missing-owned-evidence") is QuarantineState.INVALID_EVIDENCE
    )


def test_terminal_retired_sweep_rechecks_attempts_root_after_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    outside = tmp_path / "outside-attempts"
    outside.mkdir()
    attempts_root = store._filesystem.attempts_root
    prepare_roots = store._filesystem._prepare_roots

    def swap_after_validation() -> None:
        prepare_roots()
        attempts_root.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(store._filesystem, "_prepare_roots", swap_after_validation)

    with pytest.raises(StagingError, match="terminal-retired partial root is not a directory"):
        store.sweep_terminal_retired()

    assert attempts_root.is_symlink()
    assert outside.is_dir()


def test_quarantine_state_classifies_non_directory_as_invalid_evidence(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    quarantine_root = store._filesystem.quarantine_root
    quarantine_root.mkdir()
    opaque = quarantine_root / "opaque-evidence"
    opaque.write_text("preserved evidence", encoding="utf-8")

    assert store.quarantine_state(opaque) is QuarantineState.INVALID_EVIDENCE
    assert opaque.read_text(encoding="utf-8") == "preserved evidence"


def test_sidecar_plus_one_internal_quarantine_marker_remains_live_at_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    entry = tmp_path / "quarantine" / "sidecar-conflict"
    entry.mkdir(parents=True)
    raw = entry / "records.bin"
    raw.write_bytes(b"preserve contradictory evidence")
    store.mark_quarantine_published(entry)
    sidecar = entry.with_name(f"{entry.name}.json")
    sidecar.write_text(
        dumps(
            {
                "version": 1,
                "state": "unprocessable",
                "classified_at_unix_ns": now,
                "reason": "preserve conflicting attribution",
                "original_name": entry.name,
            }
        ),
        encoding="utf-8",
    )
    marker = entry / "published.json"
    before = {raw: raw.read_bytes(), marker: marker.read_bytes(), sidecar: sidecar.read_bytes()}
    now += 1_000_000_000

    assert store.sweep_terminal_quarantine() == ()
    assert set(store.quarantined_attempts()) == {entry}
    assert {path: path.read_bytes() for path in before} == before


def test_published_quarantine_marker_with_extra_field_is_retained_for_reauthentication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    entry = tmp_path / "quarantine" / "extra-published-field"
    entry.mkdir(parents=True)
    (entry / "records.bin").write_bytes(b"published but malformed marker")
    store.mark_quarantine_published(entry)
    marker = entry / "published.json"
    _rewrite_checkpoint(marker, "unexpected", True)
    original = marker.read_bytes()
    now += 1_000_000_000

    assert store.sweep_terminal_quarantine() == ()
    assert set(store.quarantined_attempts()) == {entry}
    assert marker.read_bytes() == original


def test_zero_timestamp_opaque_quarantine_expires_but_fresh_evidence_remains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 0
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    aged_source = attempts / "aged-opaque"
    aged_source.write_bytes(b"aged")
    aged = store.quarantine_pending("aged opaque evidence")[0]
    now = 1_000_000_000
    fresh_source = attempts / "fresh-opaque"
    fresh_source.write_bytes(b"fresh")
    fresh = store.quarantine_pending("fresh opaque evidence")[0]

    assert set(store.sweep_terminal_quarantine()) == {aged}
    assert not aged.exists()
    assert not Path(f"{aged}.json").exists()
    assert fresh.exists()
    assert Path(f"{fresh}.json").exists()


def test_invalid_prefix_publication_marker_stays_pending_and_blocks_read(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    attempt.close(durable=True)
    marker = attempt.path / "prefix-publication.json"
    marker.write_bytes(b'{"version":99}')
    original = {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()}

    assert store.pending_attempts() == (attempt.descriptor,)
    with pytest.raises(PendingAttemptError):
        store.assert_no_pending()

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()} == original


@pytest.mark.parametrize("collision_kind", ["destination", "sidecar"])
def test_quarantine_name_allocation_skips_existing_destination_or_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, collision_kind: str
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    entry = attempts / "opaque-entry"
    entry.write_bytes(b"opaque source")
    quarantine_root = tmp_path / "quarantine"
    quarantine_root.mkdir()
    first_name = f"opaque-entry-{'a' * 32}"
    collision_path = quarantine_root / (first_name if collision_kind == "destination" else f"{first_name}.json")
    collision_path.write_bytes(b"pre-existing collision evidence")
    choices = iter(("a" * 32, "b" * 32))
    monkeypatch.setattr(quarantine_module, "uuid4", lambda: SimpleNamespace(hex=next(choices)))

    moved = store.quarantine_pending("opaque collision fixture")

    expected = quarantine_root / f"opaque-entry-{'b' * 32}"
    assert moved == (expected,)
    assert collision_path.read_bytes() == b"pre-existing collision evidence"
    sidecar = expected.with_name(f"{expected.name}.json")
    marker = cast(dict[str, object], loads(sidecar.read_text(encoding="utf-8")))
    assert marker["original_name"] == "opaque-entry"


def test_quarantine_pending_reuses_existing_root_and_rejects_non_directory_root(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    first = store.prepare_streaming_attempt(100, 1)
    first.record_read_begin(ReadBeginNotification(100, 1))
    first.close(durable=True)
    first_moved = store.quarantine_pending("first attempt")[0]
    first_bytes = {path.name: path.read_bytes() for path in first_moved.iterdir() if path.is_file()}
    second = store.prepare_streaming_attempt(101, 1)
    second.record_read_begin(ReadBeginNotification(101, 1))
    second.close(durable=True)

    second_moved = store.quarantine_pending("second attempt")[0]

    assert first_moved.is_dir()
    assert {path.name: path.read_bytes() for path in first_moved.iterdir() if path.is_file()} == first_bytes
    assert second_moved.is_dir()
    assert first_moved != second_moved

    blocked_root = tmp_path / "blocked"
    blocked_capture = _capture_root(blocked_root)
    blocked = StagingStore(blocked_root, blocked_capture)
    source = blocked.prepare_streaming_attempt(200, 1)
    source.record_read_begin(ReadBeginNotification(200, 1))
    source.close(durable=True)
    before = {path.name: path.read_bytes() for path in source.path.iterdir() if path.is_file()}
    quarantine_root = blocked_root / "quarantine"
    quarantine_root.write_bytes(b"not a directory")

    with pytest.raises(StagingError):
        blocked.quarantine_pending("blocked destination")

    assert quarantine_root.read_bytes() == b"not a directory"
    assert {path.name: path.read_bytes() for path in source.path.iterdir() if path.is_file()} == before


@pytest.mark.parametrize("entry_kind", ["symlink", "file"])
def test_device_lock_quarantines_unsafe_capture_temporary_with_sidecar_only(tmp_path: Path, entry_kind: str) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    capture_root = _capture_root(tmp_path)
    capture_root.mkdir(parents=True)
    temporary = capture_root / f".unsafe.{('a' * 32)}.tmp"
    outside = tmp_path / "external-temporary"
    if entry_kind == "symlink":
        outside.mkdir()
        target = outside / "evidence"
        target.write_bytes(b"external target remains untouched")
        temporary.symlink_to(outside, target_is_directory=True)
    else:
        temporary.write_bytes(b"opaque temporary bytes")

    with store.device_lock():
        pass

    moved = tuple((tmp_path / "quarantine").glob("capture-temporary-*.tmp*"))
    evidence = next(path for path in moved if not path.name.endswith(".json"))
    sidecar = evidence.with_name(f"{evidence.name}.json")
    marker = cast(dict[str, object], loads(sidecar.read_text(encoding="utf-8")))
    assert not temporary.exists() and not temporary.is_symlink()
    assert marker["original_name"] == temporary.name
    assert not (evidence / "unprocessable.json").exists()
    if entry_kind == "symlink":
        assert evidence.is_symlink()
        assert evidence.readlink() == outside
        assert (outside / "evidence").read_bytes() == b"external target remains untouched"
    else:
        assert evidence.read_bytes() == b"opaque temporary bytes"
