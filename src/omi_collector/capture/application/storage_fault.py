"""Local storage faults shared by capture control and persistence adapters."""

from __future__ import annotations


class FatalInternalAudioStorageError(RuntimeError):
    """Storage integrity is uncertain and automatic capture must stop."""

    def __init__(self, message: str, *, sqlite_errorcode: int | None = None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = sqlite_errorcode


class TransientInternalAudioStorageError(RuntimeError):
    """SQLite is temporarily unavailable; preserve custody and retry."""

    def __init__(self, message: str, *, sqlite_errorcode: int) -> None:
        super().__init__(message)
        self.sqlite_errorcode = sqlite_errorcode
