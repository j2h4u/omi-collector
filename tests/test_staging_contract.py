"""Leaf import and persisted-contract tests."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.publication import TerminalRetirementEvidence
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    DurablePrefix,
    StreamingCheckpoint,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from staging_test_factory import StagingStore, staged_attempt_payload


def test_contract_validation_is_filesystem_independent() -> None:
    script = """
import sys
from omi_collector.capture.adapters.staging_contract import AttemptStateError, _validate_attempt_id

assert "omi_collector.capture.adapters.staging_filesystem" not in sys.modules
try:
    _validate_attempt_id("not-an-attempt-id")
except Exception as error:
    assert type(error) is AttemptStateError
    assert str(error) == "attempt id is invalid"
else:
    raise AssertionError("invalid attempt id was accepted")
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True)


@pytest.mark.parametrize("timestamp", [0, -1], ids=["zero", "negative"])
def test_terminal_retirement_json_rejects_nonpositive_timestamp(timestamp: int) -> None:
    value = TerminalRetirementEvidence(1).as_dict()
    value["terminalized_at_unix_ns"] = timestamp

    with pytest.raises(AttemptStateError, match="terminalized_at_unix_ns must be a positive integer"):
        TerminalRetirementEvidence.from_json(value)


def _attempt(tmp_path: Path) -> StagedAttempt:
    return _store(tmp_path).prepare_streaming_attempt(10, 2)


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures")


def _replace_attempt_field(attempt: StagedAttempt, field: str, value: dict[str, object]) -> None:
    row = staged_attempt_payload(attempt)
    assert row is not None
    row[field] = value
    attempt._filesystem.state_db.put(StateKind.ATTEMPT, attempt.attempt_id, row)


def _ample_statvfs(_: str | Path) -> object:
    return SimpleNamespace(f_bavail=10**15, f_frsize=1)


@pytest.mark.parametrize(
    ("value", "field", "replacement"),
    [
        (DurablePrefix(10, 12, 2, "a" * 64), "record_count", 3),
        (StreamingCheckpoint(1, "a" * 32, 2, "a" * 64), "record_count", 3),
    ],
    ids=["durable-prefix", "streaming-checkpoint"],
)
def test_durable_staging_values_are_immutable(
    value: DurablePrefix | StreamingCheckpoint, field: str, replacement: int
) -> None:
    original = value.record_count

    with pytest.raises(FrozenInstanceError):
        setattr(value, field, replacement)

    assert value.record_count == original


def test_open_persisted_attempt_accepts_protocol_maximum_without_capacity_preflight(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    descriptor = row["descriptor"]
    assert isinstance(descriptor, dict)
    descriptor["packet_count"] = (1 << 32) - 1
    _replace_attempt_field(attempt, "descriptor", descriptor)

    reopened = _store(tmp_path).open_attempt(attempt.attempt_id)

    assert reopened.descriptor.packet_count == (1 << 32) - 1
    reopened.close()


def test_prepare_accepts_uint32_maximum_with_injected_capacity_without_allocation(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures", statvfs_fn=_ample_statvfs)

    attempt = store.prepare_streaming_attempt(0, (1 << 32) - 1)
    try:
        assert attempt.descriptor.packet_count == (1 << 32) - 1
        assert (attempt.path / "records.bin").stat().st_size == 0
        with pytest.raises(AttemptStateError):
            store.prepare_streaming_attempt(0, 1 << 32)
    finally:
        attempt.close()


@pytest.mark.parametrize("invalid_count", [0, (1 << 32), True])
def test_open_rejects_invalid_persisted_counts_without_rewriting_evidence(
    tmp_path: Path, invalid_count: int | bool
) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    descriptor = row["descriptor"]
    assert isinstance(descriptor, dict)
    descriptor["packet_count"] = invalid_count
    _replace_attempt_field(attempt, "descriptor", descriptor)
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence
    assert staged_attempt_payload(attempt) == state_before


@pytest.mark.parametrize("invalid_id", ["g" * 32, "a" * 31])
def test_open_rejects_malformed_attempt_id_without_touching_attempts(tmp_path: Path, invalid_id: str) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(10, 2)
    attempt.close()
    before = tuple(sorted(path.name for path in store.attempts_root.iterdir()))

    with pytest.raises(AttemptStateError, match="attempt id is invalid"):
        store.open_attempt(invalid_id)

    assert tuple(sorted(path.name for path in store.attempts_root.iterdir())) == before


def test_open_rejects_descriptor_identity_mismatch_without_rewriting_evidence(tmp_path: Path) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    descriptor = row["descriptor"]
    assert isinstance(descriptor, dict)
    descriptor["attempt_id"] = "b" * 32
    _replace_attempt_field(attempt, "descriptor", descriptor)
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError, match="identity does not match"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence
    assert staged_attempt_payload(attempt) == state_before


@pytest.mark.parametrize("persisted_id", ["g" * 32, "a" * 31])
def test_open_rejects_invalid_persisted_descriptor_id_without_rewriting_evidence(
    tmp_path: Path, persisted_id: str
) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    descriptor = row["descriptor"]
    checkpoint = row["checkpoint"]
    assert isinstance(descriptor, dict)
    assert isinstance(checkpoint, dict)
    descriptor["attempt_id"] = persisted_id
    checkpoint["attempt_id"] = persisted_id
    _replace_attempt_field(attempt, "descriptor", descriptor)
    row = staged_attempt_payload(attempt)
    assert row is not None
    row["checkpoint"] = checkpoint
    attempt._filesystem.state_db.put(StateKind.ATTEMPT, attempt.attempt_id, row)
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence
    assert staged_attempt_payload(attempt) == state_before


def test_open_rejects_negative_checkpoint_count_with_valid_empty_hash_and_raw_record(tmp_path: Path) -> None:
    from hashlib import sha256

    attempt = _attempt(tmp_path)
    attempt.record_read_begin(ReadBeginNotification(10, 2))
    attempt.accept_chunk(10, b"x" * RECORD_SIZE)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    checkpoint = row["checkpoint"]
    assert isinstance(checkpoint, dict)
    checkpoint["record_count"] = -1
    checkpoint["raw_sha256"] = sha256(b"").hexdigest()
    _replace_attempt_field(attempt, "checkpoint", checkpoint)
    raw_before = (attempt.path / "records.bin").read_bytes()
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError, match="checkpoint is malformed"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert (attempt.path / "records.bin").read_bytes() == raw_before
    assert staged_attempt_payload(attempt) == state_before


@pytest.mark.parametrize("field,value", [("record_count", -1), ("raw_sha256", "x" * 64)])
def test_open_rejects_malformed_checkpoint_without_rewriting_evidence(
    tmp_path: Path, field: str, value: object
) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    row = staged_attempt_payload(attempt)
    assert row is not None
    checkpoint = row["checkpoint"]
    assert isinstance(checkpoint, dict)
    checkpoint[field] = value
    _replace_attempt_field(attempt, "checkpoint", checkpoint)
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError, match="streaming checkpoint is malformed"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence
    assert staged_attempt_payload(attempt) == state_before
