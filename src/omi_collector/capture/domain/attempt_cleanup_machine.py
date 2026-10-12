"""Pure authorization decisions for terminal streaming-attempt cleanup."""

from __future__ import annotations

from enum import StrEnum


class AttemptLifecycle(StrEnum):
    ACTIVE = "active"
    TERMINAL = "terminal"
    MALFORMED = "malformed"


class RawEvidence(StrEnum):
    COMPLETE = "complete"
    EMPTY = "empty"
    TAIL = "tail"
    MISSING = "missing"
    CORRUPT = "corrupt"


class CopyEvidence(StrEnum):
    NONE = "none"
    DRAFT = "draft"
    READY = "ready"
    RETIRED = "retired"
    UNKNOWN = "unknown"


class AttemptCleanupAction(StrEnum):
    KEEP = "keep"
    DELETE = "delete"
    FINALIZE = "finalize"
    REFUSE = "refuse"


def decide_attempt_cleanup(
    *,
    guard_held: bool,
    lifecycle: AttemptLifecycle,
    raw: RawEvidence,
    copy: CopyEvidence,
    intent: bool,
) -> AttemptCleanupAction:
    """Return the only cleanup action authorized by the supplied evidence."""
    if not guard_held or lifecycle is AttemptLifecycle.MALFORMED:
        return AttemptCleanupAction.REFUSE
    if raw is RawEvidence.CORRUPT or copy is CopyEvidence.UNKNOWN:
        return AttemptCleanupAction.REFUSE
    if lifecycle is AttemptLifecycle.ACTIVE or raw is RawEvidence.TAIL:
        return AttemptCleanupAction.KEEP
    if lifecycle is AttemptLifecycle.TERMINAL:
        if (raw is RawEvidence.EMPTY and copy is CopyEvidence.NONE) or (
            raw is RawEvidence.COMPLETE and copy in {CopyEvidence.DRAFT, CopyEvidence.READY, CopyEvidence.RETIRED}
        ):
            return AttemptCleanupAction.DELETE
        if raw is RawEvidence.MISSING and intent:
            return AttemptCleanupAction.FINALIZE
    return AttemptCleanupAction.REFUSE
