"""Verification of the single streaming staging recovery frontier."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Recovery:
    """The checkpoint-authenticated prefix and current raw evidence size."""

    attempt_id: str
    valid_records: int
    raw_bytes: int
    clean: bool
    issue: str | None
