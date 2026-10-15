"""Shared publication values and strict read-only v1 marker decoders."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .staging_contract import (
    _TERMINAL_RETIRED_STATE,
    _TERMINAL_RETIREMENT_VERSION,
    AttemptStateError,
    _validate_terminalized_at,
)


@dataclass(frozen=True, slots=True)
class SealResult:
    """A published bundle, or an already-existing identical bundle."""

    bundle_path: Path
    deduplicated: bool


@dataclass(frozen=True, slots=True)
class PrefixPublicationEvidence:
    """Canonical decoder for a legacy v1 published-prefix marker."""

    state: str = "published"

    @classmethod
    def from_json(cls, value: object) -> PrefixPublicationEvidence:
        if not isinstance(value, Mapping):
            raise AttemptStateError("prefix publication marker must be a JSON object")
        if set(value) != {"version", "state"}:
            raise AttemptStateError("prefix publication marker schema is invalid")
        version = value.get("version")
        if isinstance(version, bool) or not isinstance(version, int) or version != 1:
            raise AttemptStateError("prefix publication marker version is invalid")
        if value.get("state") != "published":
            raise AttemptStateError("prefix publication marker state is invalid")
        return cls("published")

    def as_dict(self) -> dict[str, object]:
        return {"version": 1, "state": self.state}


@dataclass(frozen=True, slots=True)
class TerminalRetirementEvidence:
    """Canonical decoder for a legacy v1 terminal-retirement marker."""

    terminalized_at_unix_ns: int

    @classmethod
    def from_json(cls, value: object) -> TerminalRetirementEvidence:
        if not isinstance(value, Mapping):
            raise AttemptStateError("terminal-retired marker must be a JSON object")
        expected = {"version", "state", "terminalized_at_unix_ns"}
        if set(value) != expected:
            raise AttemptStateError("terminal-retired marker schema is invalid")
        version = value.get("version")
        state = value.get("state")
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != _TERMINAL_RETIREMENT_VERSION
            or state != _TERMINAL_RETIRED_STATE
        ):
            raise AttemptStateError("terminal-retired marker identity is invalid")
        terminalized_at = value.get("terminalized_at_unix_ns")
        if isinstance(terminalized_at, bool) or not isinstance(terminalized_at, int):
            raise AttemptStateError("terminal-retired marker timestamp is invalid")
        _validate_terminalized_at(terminalized_at)
        return cls(terminalized_at)

    def as_dict(self) -> dict[str, object]:
        return {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _TERMINAL_RETIRED_STATE,
            "terminalized_at_unix_ns": self.terminalized_at_unix_ns,
        }
