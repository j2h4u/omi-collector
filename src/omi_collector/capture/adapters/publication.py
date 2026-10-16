"""Published bundle result value."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SealResult:
    """A published bundle, or an already-existing identical bundle."""

    bundle_path: Path
    deduplicated: bool
