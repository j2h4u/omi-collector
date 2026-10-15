from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.legacy_audio_import import (
    LegacyAttempt,
    LegacyAudioImportError,
    import_legacy_attempt,
    import_legacy_draft,
    inspect_legacy_attempt,
    inspect_legacy_draft,
    verify_imported_attempt,
    verify_imported_draft,
)


def _draft(root: Path, *, count: int = 257) -> tuple[Path, bytes]:
    raw = b"".join(index.to_bytes(4, "big") + bytes([index % 251]) * 440 for index in range(count))
    digest = hashlib.sha256(raw).hexdigest()
    path = root / f"700-{700 + count}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "start_sequence": 700,
                "next_sequence": 700 + count,
                "record_count": count,
                "record_size": 444,
                "raw_sha256": digest,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path, raw


@dataclass(frozen=True, slots=True)
class _AttemptSpec:
    attempt_id: str = "a" * 32
    directory_name: str | None = None
    count: int = 257
    packet_count: int | None = None
    read_begin: bool = True
    tail: bytes = b""


_DEFAULT_ATTEMPT_SPEC = _AttemptSpec()


def _attempt(root: Path, spec: _AttemptSpec = _DEFAULT_ATTEMPT_SPEC) -> tuple[Path, bytes, dict[str, object]]:
    packet_count = spec.count if spec.packet_count is None else spec.packet_count
    attempt_id = spec.attempt_id
    count = spec.count
    complete = b"".join(index.to_bytes(4, "big") + bytes([index % 251]) * 440 for index in range(count))
    raw = complete + spec.tail
    checkpoint_count = min(count, max(0, count - 3))
    checkpoint_raw = complete[: checkpoint_count * 444]
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 1000,
        "packet_count": packet_count,
        "record_size": 444,
        "read_begin_start": 1000 if spec.read_begin else None,
        "read_begin_count": packet_count if spec.read_begin else None,
    }
    row = {
        "descriptor": descriptor,
        "checkpoint": {
            "version": 1,
            "attempt_id": attempt_id,
            "record_count": checkpoint_count,
            "raw_sha256": hashlib.sha256(checkpoint_raw).hexdigest(),
        },
        "prefix_publication": None,
        "terminal_retirement": None,
    }
    path = root / (spec.directory_name or attempt_id)
    path.mkdir(parents=True)
    (path / "attempt.json").write_text(json.dumps(descriptor, sort_keys=True), encoding="utf-8")
    (path / "checkpoint.json").write_text(json.dumps(row["checkpoint"], sort_keys=True), encoding="utf-8")
    (path / "records.bin").write_bytes(raw)
    return path, raw, row


def test_attempt_imports_257_rows_and_verifies_typed_source(tmp_path: Path) -> None:
    attempt_path, raw, row = _attempt(tmp_path)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    with closing(state_db.open_connection()) as connection:
        import_legacy_attempt(connection, attempt, authoritative_row=row, chunk_limit=128)
        verify_imported_attempt(connection, attempt)
        source = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT lifecycle, committed_count, certified_prefix_count, coverage_digest FROM capture_source WHERE source_id = ?",
                (attempt.attempt_id,),
            ).fetchone(),
        )
    assert source is not None
    checkpoint = cast(dict[str, object], row["checkpoint"])
    assert tuple(source) == ("interrupted", 257, 254, checkpoint["raw_sha256"])
    assert raw == (attempt_path / "records.bin").read_bytes()


def test_sql_authoritative_raw_only_attempt_imports_and_verifies(tmp_path: Path) -> None:
    attempt_path, raw, row = _attempt(tmp_path)
    (attempt_path / "attempt.json").unlink()
    (attempt_path / "checkpoint.json").unlink()
    assert {entry.name for entry in attempt_path.iterdir()} == {"records.bin"}
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    with closing(state_db.open_connection()) as connection:
        import_legacy_attempt(connection, attempt, authoritative_row=row)
        verify_imported_attempt(connection, attempt)
    assert (attempt_path / "records.bin").read_bytes() == raw


def test_sql_authoritative_raw_only_attempt_rejects_changed_prefix_and_partial_json(tmp_path: Path) -> None:
    attempt_path, raw, row = _attempt(tmp_path)
    (attempt_path / "attempt.json").unlink()
    (attempt_path / "checkpoint.json").unlink()
    changed = bytes([raw[0] ^ 1]) + raw[1:]
    (attempt_path / "records.bin").write_bytes(changed)
    with pytest.raises(LegacyAudioImportError, match="raw prefix differs"):
        inspect_legacy_attempt(attempt_path, "a" * 32, row)
    (attempt_path / "records.bin").write_bytes(raw)
    (attempt_path / "attempt.json").write_text(json.dumps(row["descriptor"]), encoding="utf-8")
    with pytest.raises(LegacyAudioImportError, match="invalid"):
        inspect_legacy_attempt(attempt_path, "a" * 32, row)


def test_attempt_import_commits_no_more_than_one_record_page_per_transaction(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path)
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    statements: list[str] = []
    with closing(state_db.open_connection()) as connection:
        connection.set_trace_callback(statements.append)
        import_legacy_attempt(connection, attempt, authoritative_row=row, chunk_limit=128)
    assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 4
    assert sum(statement == "COMMIT" for statement in statements) == 4


def test_attempt_parity_rejects_same_length_tail_and_certification_corruption(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path, _AttemptSpec(count=4, packet_count=5, tail=b"tail"))
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    with closing(state_db.open_connection()) as connection:
        import_legacy_attempt(connection, attempt, authoritative_row=row)
        connection.execute("UPDATE capture_tail SET bytes=? WHERE source_id=?", (b"foul", attempt.attempt_id))
        with pytest.raises(LegacyAudioImportError, match="tail bytes differ"):
            verify_imported_attempt(connection, attempt)
        connection.execute("UPDATE capture_tail SET bytes=? WHERE source_id=?", (b"tail", attempt.attempt_id))
        connection.execute(
            "UPDATE capture_source SET certified_prefix_count=0 WHERE source_id=?", (attempt.attempt_id,)
        )
        with pytest.raises(LegacyAudioImportError, match="typed digest differs"):
            verify_imported_attempt(connection, attempt)


def test_attempt_import_accepts_checkpoint_behind_aligned_raw(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path, _AttemptSpec(count=8, packet_count=10))
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    with closing(state_db.open_connection()) as connection:
        import_legacy_attempt(connection, attempt)
        source = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT committed_count, certified_prefix_count FROM capture_source WHERE source_id = ?",
                (attempt.attempt_id,),
            ).fetchone(),
        )
    assert source is not None
    assert tuple(source) == (8, 5)


@pytest.mark.parametrize(("count", "tail"), ((3, b"tail" * 20), (0, b"tail")))
def test_attempt_inspection_refuses_private_bytes_without_read_begin(tmp_path: Path, count: int, tail: bytes) -> None:
    attempt_path, raw, row = _attempt(tmp_path, _AttemptSpec(count=count, packet_count=5, read_begin=False, tail=tail))
    with pytest.raises(LegacyAudioImportError, match="without READ_BEGIN"):
        inspect_legacy_attempt(attempt_path, "a" * 32, row)
    assert (attempt_path / "records.bin").read_bytes() == raw


def test_attempt_import_defensively_refuses_private_bytes_without_read_begin(tmp_path: Path) -> None:
    attempt_path, raw, row = _attempt(tmp_path, _AttemptSpec(count=1, packet_count=2))
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    unsupported = replace(attempt, descriptor=replace(attempt.descriptor, read_begin_start=None, read_begin_count=None))
    with closing(state_db.open_connection()) as connection:
        with pytest.raises(LegacyAudioImportError, match="without READ_BEGIN"):
            import_legacy_attempt(connection, unsupported)
        assert connection.execute("SELECT 1 FROM capture_source").fetchone() is None
    assert (attempt_path / "records.bin").read_bytes() == raw


def test_renamed_attempt_inspector_uses_explicit_identity(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path, _AttemptSpec(directory_name="quarantine-renamed"))
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    assert isinstance(attempt, LegacyAttempt)
    assert attempt.attempt_id == "a" * 32


def test_authoritative_cleanup_intent_is_accepted_when_pending(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path)
    authoritative = dict(row)
    authoritative["cleanup_intent"] = {"version": 1, "state": "pending"}
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, authoritative)
    assert attempt.attempt_id == "a" * 32


def test_attempt_inspection_rejects_unknown_entry_and_symlink(tmp_path: Path) -> None:
    attempt_path, _, _ = _attempt(tmp_path)
    (attempt_path / "unknown.json").write_text("{}", encoding="utf-8")
    with pytest.raises(LegacyAudioImportError, match="unsupported"):
        inspect_legacy_attempt(attempt_path, "a" * 32)
    (attempt_path / "unknown.json").unlink()
    (attempt_path / "records.bin").unlink()
    (attempt_path / "records.bin").symlink_to(tmp_path / "outside")
    with pytest.raises(LegacyAudioImportError, match="invalid"):
        inspect_legacy_attempt(attempt_path, "a" * 32)


def test_attempt_import_rejects_authoritative_row_mismatch(tmp_path: Path) -> None:
    attempt_path, _, row = _attempt(tmp_path)
    attempt = inspect_legacy_attempt(attempt_path, "a" * 32, row)
    wrong = dict(row)
    wrong["descriptor"] = dict(cast(dict[str, object], row["descriptor"]))
    wrong["descriptor"]["packet_count"] = 1
    with pytest.raises(LegacyAudioImportError, match="authoritative"):
        inspect_legacy_attempt(attempt_path, "a" * 32, wrong)
    assert attempt.attempt_id == "a" * 32


def test_offline_draft_import_is_bounded_and_byte_exact(tmp_path: Path) -> None:
    draft_path, raw = _draft(tmp_path)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    draft = inspect_legacy_draft(draft_path)
    with closing(state_db.open_connection()) as connection:
        import_legacy_draft(connection, draft, chunk_limit=7)
        verify_imported_draft(connection, draft)
        rows = connection.execute(
            "SELECT ordinal, device_timestamp, payload FROM capture_record WHERE source_id = ? ORDER BY ordinal",
            (draft.source_id,),
        ).fetchall()
    assert len(rows) == 257
    typed_rows = cast(Iterable[tuple[int, int, bytes]], rows)
    assert b"".join(timestamp.to_bytes(4, "big") + payload for _, timestamp, payload in typed_rows) == raw


def test_offline_draft_import_rejects_changed_raw_and_symlink(tmp_path: Path) -> None:
    draft_path, _ = _draft(tmp_path, count=1)
    draft = inspect_legacy_draft(draft_path)
    (draft_path / "records.bin").write_bytes(b"X" * 444)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    with closing(state_db.open_connection()) as connection, pytest.raises(LegacyAudioImportError, match="hash differs"):
        import_legacy_draft(connection, draft)
    (draft_path / "records.bin").unlink()
    (draft_path / "records.bin").symlink_to(tmp_path / "outside")
    with pytest.raises(LegacyAudioImportError, match="not regular"):
        inspect_legacy_draft(draft_path)


def test_offline_draft_parity_catches_candidate_byte_change(tmp_path: Path) -> None:
    draft_path, _ = _draft(tmp_path, count=1)
    draft = inspect_legacy_draft(draft_path)
    state_db = StateDB(tmp_path / "candidate.sqlite3")
    with closing(state_db.open_connection()) as connection:
        import_legacy_draft(connection, draft)
        connection.execute("UPDATE capture_record SET payload = ? WHERE source_id = ?", (b"X" * 440, draft.source_id))
        with pytest.raises(LegacyAudioImportError, match="stream differs"):
            verify_imported_draft(connection, draft)
