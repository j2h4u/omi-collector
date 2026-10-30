"""Decide how an authenticated conflict enters quarantine."""

from dataclasses import dataclass

from .internal_audio_machine import (
    QuarantineDecision,
    QuarantineDisposition,
    QuarantineOpened,
    quarantine_transition,
)


@dataclass(frozen=True, slots=True)
class ConflictDisposition:
    classification: QuarantineDecision
    block_reason: str


def classify_conflict(committed_count: int, has_canonical_records: bool, reason: str) -> ConflictDisposition:
    """Classify retained conflict evidence and its custody reason."""
    classification = quarantine_transition(
        QuarantineDisposition.UNCLASSIFIED, QuarantineOpened(committed_count, has_canonical_records)
    )
    block_reason = "source_authentication_mismatch" if committed_count == 0 and has_canonical_records else reason
    return ConflictDisposition(classification, block_reason)
