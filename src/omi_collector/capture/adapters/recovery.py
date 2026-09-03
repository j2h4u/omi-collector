"""Verification of the single streaming staging recovery frontier."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RECORD_SIZE
from .staging_contract import (
    _CHECKPOINT_NAME,
    _RAW_NAME,
    AttemptDescriptor,
    AttemptStateError,
    DurablePrefix,
    _read_checkpoint,
)
from .staging_filesystem import StagingFilesystem, _hash_prefix, _never_defer, _require_regular_file


@dataclass(frozen=True, slots=True)
class Recovery:
    """The checkpoint-authenticated prefix and current raw evidence size."""

    attempt_id: str
    valid_records: int
    raw_bytes: int
    clean: bool
    issue: str | None


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
    if raw_size % RECORD_SIZE or checkpoint.record_count > descriptor.packet_count:
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
