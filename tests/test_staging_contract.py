"""Leaf import and persisted-contract tests."""

from __future__ import annotations

import subprocess
import sys
from contextlib import closing
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from omi_collector.capture.adapters.publication import TerminalRetirementEvidence
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    DurablePrefix,
    StreamingCheckpoint,
)
from staging_test_factory import StagingStore, attempt_source


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


def test_sqlite_attempt_accepts_protocol_maximum_without_record_allocation(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    store = StagingStore(spool, tmp_path / "captures")
    attempt = store.prepare_streaming_attempt(0, (1 << 32) - 1)
    try:
        assert attempt.descriptor.packet_count == (1 << 32) - 1
        assert attempt.source.committed_count == 0
        assert attempt.path is None
        attempt.close()

        reopened = store.open_attempt(attempt.attempt_id)
        assert reopened.descriptor.packet_count == (1 << 32) - 1
        assert reopened.durable_prefix.record_count == 0
    finally:
        attempt.close()


@pytest.mark.parametrize("invalid_count", [0, (1 << 32), True])
def test_sqlite_attempt_rejects_invalid_counts_without_creating_source(
    tmp_path: Path, invalid_count: int | bool
) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")

    with pytest.raises(AttemptStateError):
        store.prepare_streaming_attempt(0, invalid_count)

    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM capture_source").fetchone()[0] == 0


@pytest.mark.parametrize("invalid_id", ["g" * 32, "a" * 31])
def test_open_rejects_malformed_attempt_id_without_touching_typed_sources(tmp_path: Path, invalid_id: str) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")
    attempt = store.prepare_streaming_attempt(10, 2)
    attempt_id = attempt.attempt_id
    attempt.close()
    before = attempt_source(store, attempt_id)

    with pytest.raises(AttemptStateError, match="attempt id is invalid"):
        store.open_attempt(invalid_id)

    assert attempt_source(store, attempt_id) == before
