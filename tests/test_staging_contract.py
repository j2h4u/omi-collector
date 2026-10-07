"""Leaf import and persisted-contract tests."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import FrozenInstanceError
from json import dumps, loads
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.publication import TerminalRetirementEvidence
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    DurablePrefix,
    StreamingCheckpoint,
)
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification


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
    descriptor_path = attempt.path / "attempt.json"
    descriptor = cast(dict[str, object], loads(descriptor_path.read_text(encoding="utf-8")))
    descriptor["packet_count"] = (1 << 32) - 1
    descriptor_path.write_text(dumps(descriptor), encoding="utf-8")

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
    descriptor_path = attempt.path / "attempt.json"
    descriptor = cast(dict[str, object], loads(descriptor_path.read_text(encoding="utf-8")))
    descriptor["packet_count"] = invalid_count
    descriptor_path.write_text(dumps(descriptor), encoding="utf-8")
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}

    with pytest.raises(AttemptStateError):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence


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
    descriptor_path = attempt.path / "attempt.json"
    descriptor = cast(dict[str, object], loads(descriptor_path.read_text(encoding="utf-8")))
    descriptor["attempt_id"] = "b" * 32
    descriptor_path.write_text(dumps(descriptor), encoding="utf-8")
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}

    with pytest.raises(AttemptStateError, match="identity or record size"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence


@pytest.mark.parametrize("persisted_id", ["g" * 32, "a" * 31])
def test_open_rejects_invalid_persisted_descriptor_id_without_rewriting_evidence(
    tmp_path: Path, persisted_id: str
) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    descriptor_path = attempt.path / "attempt.json"
    invalid_path = attempt.path.with_name(persisted_id)
    descriptor = cast(dict[str, object], loads(descriptor_path.read_text(encoding="utf-8")))
    descriptor["attempt_id"] = persisted_id
    descriptor_path.write_text(dumps(descriptor), encoding="utf-8")
    checkpoint_path = attempt.path / "checkpoint.json"
    checkpoint = cast(dict[str, object], loads(checkpoint_path.read_text(encoding="utf-8")))
    checkpoint["attempt_id"] = persisted_id
    checkpoint_path.write_text(dumps(checkpoint), encoding="utf-8")
    attempt.path.rename(invalid_path)
    evidence = {path.name: path.read_bytes() for path in invalid_path.iterdir()}

    with pytest.raises(AttemptStateError):
        _store(tmp_path).open_attempt(persisted_id)

    assert {path.name: path.read_bytes() for path in invalid_path.iterdir()} == evidence


def test_open_rejects_negative_checkpoint_count_with_valid_empty_hash_and_raw_record(tmp_path: Path) -> None:
    from hashlib import sha256

    attempt = _attempt(tmp_path)
    attempt.record_read_begin(ReadBeginNotification(10, 2))
    attempt.accept_chunk(10, b"x" * RECORD_SIZE)
    attempt.close()
    checkpoint_path = attempt.path / "checkpoint.json"
    checkpoint = cast(dict[str, object], loads(checkpoint_path.read_text(encoding="utf-8")))
    checkpoint["record_count"] = -1
    checkpoint["raw_sha256"] = sha256(b"").hexdigest()
    checkpoint_path.write_text(dumps(checkpoint), encoding="utf-8")
    raw_before = (attempt.path / "records.bin").read_bytes()
    checkpoint_before = checkpoint_path.read_bytes()

    with pytest.raises(AttemptStateError, match="checkpoint is malformed"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert (attempt.path / "records.bin").read_bytes() == raw_before
    assert checkpoint_path.read_bytes() == checkpoint_before


@pytest.mark.parametrize("field,value", [("record_count", -1), ("raw_sha256", "x" * 64)])
def test_open_rejects_malformed_checkpoint_without_rewriting_evidence(
    tmp_path: Path, field: str, value: object
) -> None:
    attempt = _attempt(tmp_path)
    attempt.close()
    checkpoint_path = attempt.path / "checkpoint.json"
    checkpoint = cast(dict[str, object], loads(checkpoint_path.read_text(encoding="utf-8")))
    checkpoint[field] = value
    checkpoint_path.write_text(dumps(checkpoint), encoding="utf-8")
    evidence = {path.name: path.read_bytes() for path in attempt.path.iterdir()}

    with pytest.raises(AttemptStateError, match="streaming checkpoint is malformed"):
        _store(tmp_path).open_attempt(attempt.attempt_id)

    assert {path.name: path.read_bytes() for path in attempt.path.iterdir()} == evidence
