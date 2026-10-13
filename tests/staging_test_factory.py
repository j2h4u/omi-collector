"""Small test-only adapter for staging fixtures during StateDB migration."""

from __future__ import annotations

from collections.abc import Callable
from json import dumps
from os import fsync, statvfs
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    CaptureSource,
    InternalAudioStore,
    read_capture_source,
)
from omi_collector.capture.adapters.staging_filesystem import DEFAULT_STAGING_IO, StagingIO, StagingPaths
from omi_collector.capture.adapters.staging_store import StagingStore as _StagingStore
from omi_collector.config import CollectorConfig


class StagingStore(_StagingStore):
    """Construct the production store with isolated state for legacy-style tests."""

    def __init__(
        self,
        spool: Path,
        capture_root: Path,
        **options: object,
    ) -> None:
        state_db = cast(StateDB | None, options.pop("state_db", None))
        io = cast(StagingIO | None, options.pop("io", None))
        fsync_fn = cast(Callable[[int], None], options.pop("fsync_fn", fsync))
        statvfs_fn = cast(Callable[[str | Path], object], options.pop("statvfs_fn", statvfs))
        config = cast(CollectorConfig, options.pop("config", DEFAULT_STAGING_IO.config))
        if options:
            raise TypeError(f"unexpected staging test options: {', '.join(sorted(options))}")
        super().__init__(
            spool,
            capture_root,
            state_db=state_db or StateDB(Path(spool) / "collector-state.sqlite3"),
            io=io or StagingIO(fsync_fn=fsync_fn, statvfs_fn=statvfs_fn, config=config),
        )

    @classmethod
    def from_paths(cls, paths: StagingPaths, **options: object) -> StagingStore:
        state_db = cast(StateDB | None, options.pop("state_db", None))
        io = cast(StagingIO | None, options.pop("io", None))
        publication_root = cast(Path | None, options.pop("publication_root", None))
        config = cast(CollectorConfig, options.pop("config", DEFAULT_STAGING_IO.config))
        if options:
            raise TypeError(f"unexpected staging test options: {', '.join(sorted(options))}")
        store = super().from_paths(
            paths,
            state_db=state_db or StateDB(paths.state_db_path),
            io=io or StagingIO(config=config),
            publication_root=publication_root,
        )
        assert isinstance(store, cls)
        return store


def closure_payload(store: StagingStore) -> dict[str, object] | None:
    """Read the authoritative closure row for direct state assertions."""
    return store.state_db.get(StateKind.READY_CLOSURE, "current")


def closure_bytes(store: StagingStore) -> bytes:
    """Return deterministic serialized DB payload for before/after assertions."""
    payload = closure_payload(store)
    return dumps(payload, sort_keys=True).encode() if payload is not None else b""


def closure_rows(store: StagingStore) -> list[dict[str, object]]:
    """Return the stored closure entries, treating the missing row as empty."""
    payload = closure_payload(store)
    if payload is None:
        return []
    closures = payload.get("closures")
    return cast(list[dict[str, object]], closures) if isinstance(closures, list) else []


def attempt_source(store: StagingStore, attempt_id: str) -> CaptureSource | None:
    """Read the typed SQL source snapshot used by an attempt."""
    return read_capture_source(store.state_db, attempt_id)


def attempt_records(store: StagingStore, attempt_id: str) -> tuple[bytes, ...]:
    """Read committed attempt records through the typed SQL store."""
    with InternalAudioStore(store.state_db) as audio_store:
        return tuple(record.raw_bytes for record in audio_store.read_record_page(attempt_id, limit=128))


def quarantine_payload(store: StagingStore, item_id: str) -> dict[str, object]:
    """Read one authoritative quarantine row, failing on missing state."""
    payload = store.state_db.get(StateKind.QUARANTINE, item_id)
    assert payload is not None
    return payload
