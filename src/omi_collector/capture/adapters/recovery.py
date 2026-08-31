"""Verification and hydration of recoverable staging prefixes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from json import JSONDecodeError, loads
from pathlib import Path
from typing import cast

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RECORD_SIZE
from .staging_contract import (
    _CHECKPOINT_NAME,
    _COMMITS_NAME,
    _RAW_NAME,
    AttemptDescriptor,
    AttemptStateError,
    DurablePrefix,
    _is_int,
    _read_checkpoint,
)
from .staging_filesystem import (
    StagingFilesystem,
    _hash_prefix,
    _never_defer,
    _require_regular_file,
)


@dataclass(frozen=True, slots=True)
class Recovery:
    """The longest verified commit prefix present in one partial attempt."""

    attempt_id: str
    valid_records: int
    raw_bytes: int
    commit_bytes: int
    clean: bool
    issue: str | None


def _recover_prefix(path: Path, descriptor: AttemptDescriptor, filesystem: StagingFilesystem) -> Recovery:
    raw_path = path / _RAW_NAME
    commit_path = path / _COMMITS_NAME
    raw_size = filesystem.file_size(raw_path)
    commit_size = filesystem.file_size(commit_path)
    commits = _commit_lines(commit_path)
    raw = raw_path.read_bytes() if raw_path.exists() else b""
    valid, consumed, issue = _validated_prefix(descriptor, commits, raw)
    clean = issue is None and raw_size == valid * RECORD_SIZE and consumed == commit_size
    if issue is None and not clean:
        issue = "uncommitted raw bytes or commit bytes remain"
    return Recovery(descriptor.attempt_id, valid, raw_size, commit_size, clean, issue)


def _unbound_recovery(attempt_id: str, raw_size: int, commit_size: int) -> Recovery:
    clean = raw_size == 0 and commit_size == 0
    issue = None if clean else "READ_BEGIN is not persisted"
    return Recovery(attempt_id, 0, raw_size, commit_size, clean, issue)


def _commit_lines(path: Path) -> list[bytes]:
    return path.read_bytes().splitlines(keepends=True) if path.exists() else []


def _validated_prefix(descriptor: AttemptDescriptor, lines: list[bytes], raw: bytes) -> tuple[int, int, str | None]:
    start = descriptor.read_begin_start
    count = descriptor.read_begin_count
    if start is None or count is None:
        return 0, 0, "READ_BEGIN is not persisted"
    valid = 0
    consumed = 0
    for line in lines:
        parsed = _parse_commit(line)
        if parsed is None:
            return valid, consumed, "commit is malformed"
        if not _commit_matches(parsed, valid, start, raw):
            return valid, consumed, "commit is not contiguous or raw bytes do not match"
        valid += 1
        consumed += len(line)
        if valid > count:
            return valid - 1, consumed - len(line), "commit count exceeds READ_BEGIN"
    return valid, consumed, None


def _parse_commit(line: bytes) -> dict[str, object] | None:
    if not line.endswith(b"\n"):
        return None
    try:
        value = cast(object, loads(line))
    except JSONDecodeError, UnicodeDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _commit_matches(commit: dict[str, object], index: int, start: int, raw: bytes) -> bool:
    record = raw[index * RECORD_SIZE : (index + 1) * RECORD_SIZE]
    return (
        _is_int(commit.get("index"), index)
        and _is_int(commit.get("sequence"), start + index)
        and commit.get("sha256") == sha256(record).hexdigest()
        and len(record) == RECORD_SIZE
    )


def _published_prefix(
    source: Path,
    descriptor: AttemptDescriptor,
    filesystem: StagingFilesystem,
    *,
    io_chunk_bytes: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
    should_defer: Callable[[], bool] = _never_defer,
) -> DurablePrefix:
    raw_path = source / _RAW_NAME
    checkpoint_path = source / _CHECKPOINT_NAME
    _require_regular_file(raw_path, "published partial raw")
    _require_regular_file(checkpoint_path, "published partial checkpoint")
    checkpoint = _read_checkpoint(checkpoint_path, descriptor.attempt_id)
    raw_size = raw_path.stat().st_size
    if checkpoint.record_count > descriptor.packet_count:
        raise AttemptStateError("published partial range is invalid")
    prefix_size = checkpoint.record_count * RECORD_SIZE
    if (
        raw_size < prefix_size
        or _hash_prefix(
            raw_path,
            prefix_size,
            chunk_size=io_chunk_bytes or filesystem._durability.io_chunk_bytes,
            should_defer=should_defer,
        )
        != checkpoint.raw_sha256
    ):
        raise AttemptStateError("published partial prefix is invalid")
    return DurablePrefix(
        descriptor.start_sequence,
        descriptor.start_sequence + checkpoint.record_count,
        checkpoint.record_count,
        checkpoint.raw_sha256,
    )
