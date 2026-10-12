"""Exhaustive decision-table tests for attempt cleanup authorization."""

from __future__ import annotations

from itertools import product

from omi_collector.capture.domain.attempt_cleanup_machine import (
    AttemptCleanupAction,
    AttemptLifecycle,
    CopyEvidence,
    RawEvidence,
    decide_attempt_cleanup,
)


def _expected(
    guard_held: bool,
    lifecycle: AttemptLifecycle,
    raw: RawEvidence,
    copy: CopyEvidence,
    intent: bool,
) -> AttemptCleanupAction:
    result = AttemptCleanupAction.REFUSE
    if not guard_held:
        return result
    if lifecycle is AttemptLifecycle.MALFORMED or raw is RawEvidence.CORRUPT or copy is CopyEvidence.UNKNOWN:
        return result
    if lifecycle is AttemptLifecycle.ACTIVE or raw is RawEvidence.TAIL:
        result = AttemptCleanupAction.KEEP
    elif lifecycle is AttemptLifecycle.TERMINAL and (
        (raw is RawEvidence.EMPTY and copy is CopyEvidence.NONE)
        or (raw is RawEvidence.COMPLETE and copy in {CopyEvidence.DRAFT, CopyEvidence.READY, CopyEvidence.RETIRED})
    ):
        result = AttemptCleanupAction.DELETE
    elif lifecycle is AttemptLifecycle.TERMINAL and raw is RawEvidence.MISSING and intent:
        result = AttemptCleanupAction.FINALIZE
    return result


def test_every_evidence_tuple_matches_the_independent_specification() -> None:
    for guard_held, lifecycle, raw, copy, intent in product(
        (False, True), AttemptLifecycle, RawEvidence, CopyEvidence, (False, True)
    ):
        assert decide_attempt_cleanup(
            guard_held=guard_held,
            lifecycle=lifecycle,
            raw=raw,
            copy=copy,
            intent=intent,
        ) is _expected(guard_held, lifecycle, raw, copy, intent)


def test_empty_terminal_attempt_without_copy_is_deleted() -> None:
    assert (
        decide_attempt_cleanup(
            guard_held=True,
            lifecycle=AttemptLifecycle.TERMINAL,
            raw=RawEvidence.EMPTY,
            copy=CopyEvidence.NONE,
            intent=False,
        )
        is AttemptCleanupAction.DELETE
    )


def test_complete_terminal_attempt_requires_authenticated_copy() -> None:
    for copy in (CopyEvidence.DRAFT, CopyEvidence.READY, CopyEvidence.RETIRED):
        assert (
            decide_attempt_cleanup(
                guard_held=True,
                lifecycle=AttemptLifecycle.TERMINAL,
                raw=RawEvidence.COMPLETE,
                copy=copy,
                intent=False,
            )
            is AttemptCleanupAction.DELETE
        )


def test_missing_raw_with_cleanup_intent_finalizes_database_state() -> None:
    assert (
        decide_attempt_cleanup(
            guard_held=True,
            lifecycle=AttemptLifecycle.TERMINAL,
            raw=RawEvidence.MISSING,
            copy=CopyEvidence.NONE,
            intent=True,
        )
        is AttemptCleanupAction.FINALIZE
    )


def test_tail_and_untrusted_evidence_never_delete() -> None:
    for raw, copy in product(RawEvidence, CopyEvidence):
        action = decide_attempt_cleanup(
            guard_held=True,
            lifecycle=AttemptLifecycle.TERMINAL,
            raw=raw,
            copy=copy,
            intent=False,
        )
        if raw is RawEvidence.TAIL and copy is not CopyEvidence.UNKNOWN:
            assert action is AttemptCleanupAction.KEEP
        if copy is CopyEvidence.UNKNOWN:
            assert action is AttemptCleanupAction.REFUSE
        if raw in {RawEvidence.TAIL, RawEvidence.MISSING, RawEvidence.CORRUPT} or copy is CopyEvidence.UNKNOWN:
            assert action is not AttemptCleanupAction.DELETE
