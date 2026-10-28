"""Pure classification of an ambiguous public publication commit."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class StageCustody(StrEnum):
    """Whether the stage path is absent, still owned, or has conflicting custody."""

    ABSENT = "absent"
    OWNED = "owned"
    CONFLICTING = "conflicting"


@dataclass(frozen=True, slots=True)
class CohortFact:
    phase: str
    cursor: int


@dataclass(frozen=True, slots=True)
class MemberCursor:
    ordinal: int
    cursor: int


@dataclass(frozen=True, slots=True)
class ProofFact:
    lifecycle: str
    stage_id: str
    owner: str


@dataclass(frozen=True, slots=True)
class CommitEvidence:
    """Observed durable values reduced to their publication meanings."""

    entry: object
    frontier: object
    cohort: CohortFact | None
    members: tuple[MemberCursor, ...]
    proof: ProofFact | None
    stage: StageCustody
    sources: tuple[object | None, ...]


@dataclass(frozen=True, slots=True)
class SourceExpectation:
    before: object | None
    proposed: object | None


@dataclass(frozen=True, slots=True)
class CommitExpectation:
    """Expected old and proposed durable states for one commit attempt."""

    entry: object
    old_frontier: object
    proposed_frontier: object
    old_cohort_cursor: int
    complete: bool
    old_members: tuple[MemberCursor, ...]
    proposed_members: tuple[MemberCursor, ...]
    stage_id: str
    owner: str
    sources: tuple[SourceExpectation, ...]


def classify_commit(evidence: CommitEvidence, expectation: CommitExpectation) -> Literal["proposed", "old", "third"]:
    """Classify exact proposed, exact old, or unrecognized durable custody."""
    proposed_cohort = (
        evidence.cohort is None
        if expectation.complete
        else evidence.cohort == CohortFact("accepted", expectation.old_cohort_cursor + 1)
        and evidence.members == expectation.proposed_members
    )
    proposed_sources = all(
        actual == expected.proposed for actual, expected in zip(evidence.sources, expectation.sources, strict=True)
    )
    if all(
        (
            evidence.entry == expectation.entry,
            evidence.frontier == expectation.proposed_frontier,
            evidence.stage is StageCustody.ABSENT,
            evidence.proof == ProofFact("active", expectation.stage_id, expectation.owner),
            proposed_cohort,
            proposed_sources,
        )
    ):
        return "proposed"

    old_sources = all(
        actual == expected.before for actual, expected in zip(evidence.sources, expectation.sources, strict=True)
    )
    if all(
        (
            evidence.entry is None,
            evidence.frontier == expectation.old_frontier,
            evidence.cohort == CohortFact("staging", expectation.old_cohort_cursor),
            evidence.stage is StageCustody.OWNED,
            evidence.proof == ProofFact("staging", expectation.stage_id, expectation.owner),
            evidence.members == expectation.old_members,
            old_sources,
        )
    ):
        return "old"
    return "third"
