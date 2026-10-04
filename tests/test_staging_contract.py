"""Leaf import and persisted-contract tests."""

from __future__ import annotations

import subprocess
import sys
from json import dumps, loads
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.staging_contract import AttemptStateError
from omi_collector.capture.adapters.staging_store import StagingStore


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


def _attempt(tmp_path: Path) -> StagedAttempt:
    return _store(tmp_path).prepare_streaming_attempt(10, 2)


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures")


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
