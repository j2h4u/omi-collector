"""Leaf import and persisted-contract tests."""

from __future__ import annotations

import subprocess
import sys


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
