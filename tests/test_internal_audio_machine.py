from __future__ import annotations

from itertools import product
from typing import get_args

import pytest

from omi_collector.capture.domain.internal_audio_machine import (
    Ack,
    AudioAction,
    AudioDecision,
    AudioEvent,
    AudioPhase,
    AuthenticatedConflict,
    Barrier,
    BeginRetirement,
    CanonicalExpiry,
    CertifiedPrefix,
    CohortAction,
    CohortConflict,
    CohortEvent,
    CohortPhase,
    CompleteRetire,
    Create,
    Data,
    Done,
    DurableDestination,
    ExistingReadyCustody,
    Interruption,
    MarkUnprocessable,
    OwnedIncompleteRetry,
    PrepareLeg,
    QualifiedDrained,
    QuarantineAction,
    QuarantineDisposition,
    QuarantineEvent,
    QuarantineOpened,
    ReadBegin,
    ReadyCustody,
    ResidualExpiry,
    RetirementStep,
    SourceIntegrityHold,
    Stage,
    TerminalPrefix,
    cohort_transition,
    quarantine_transition,
    transition,
)

PHASES = tuple(AudioPhase)
EVENTS = (
    Create(),
    PrepareLeg(),
    ReadBegin(),
    Data(),
    Barrier(),
    Done(),
    TerminalPrefix(),
    Interruption(),
    AuthenticatedConflict(),
    SourceIntegrityHold(),
    ReadyCustody(),
    BeginRetirement(),
    RetirementStep(),
    Ack(),
)


def _result(decision: AudioDecision, phase: AudioPhase | None, action: AudioAction) -> None:
    assert decision.next_phase is phase
    assert decision.action is action
    assert decision.reason


def test_audio_matrix_refuses_every_unsupported_phase_event_cell() -> None:
    assert {type(event) for event in EVENTS} == set(get_args(AudioEvent))
    supported = {
        (None, Create): AudioAction.PERSIST,
        (AudioPhase.PREPARED, PrepareLeg): AudioAction.PERSIST,
        (AudioPhase.PREPARED, Barrier): AudioAction.PERSIST,
        (AudioPhase.PREPARED, Interruption): AudioAction.PERSIST,
        (AudioPhase.PREPARED, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.PREPARED, TerminalPrefix): AudioAction.PERSIST,
        (AudioPhase.LEG_PREPARED, ReadBegin): AudioAction.PERSIST,
        (AudioPhase.LEG_PREPARED, Barrier): AudioAction.COMMIT_BARRIER,
        (AudioPhase.LEG_PREPARED, Interruption): AudioAction.PERSIST,
        (AudioPhase.LEG_PREPARED, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.LEG_PREPARED, TerminalPrefix): AudioAction.PERSIST,
        (AudioPhase.RECEIVING, Data): AudioAction.PERSIST,
        (AudioPhase.RECEIVING, Barrier): AudioAction.COMMIT_BARRIER,
        (AudioPhase.RECEIVING, Done): AudioAction.PERSIST,
        (AudioPhase.RECEIVING, Interruption): AudioAction.PERSIST,
        (AudioPhase.RECEIVING, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.RECEIVING, TerminalPrefix): AudioAction.PERSIST,
        (AudioPhase.INTERRUPTED, PrepareLeg): AudioAction.PERSIST,
        (AudioPhase.INTERRUPTED, Barrier): AudioAction.COMMIT_BARRIER,
        (AudioPhase.INTERRUPTED, Done): AudioAction.PERSIST,
        (AudioPhase.INTERRUPTED, Interruption): AudioAction.IDEMPOTENT,
        (AudioPhase.INTERRUPTED, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.INTERRUPTED, TerminalPrefix): AudioAction.PERSIST,
        (AudioPhase.SEALED_WAITING, Done): AudioAction.IDEMPOTENT,
        (AudioPhase.SEALED_WAITING, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.SEALED_WAITING, ReadyCustody): AudioAction.RETIRE,
        (AudioPhase.SEALED_WAITING, Barrier): AudioAction.INSPECT_ONLY,
        (AudioPhase.SEALED_WAITING, Interruption): AudioAction.INSPECT_ONLY,
        (AudioPhase.SEALED_WAITING, TerminalPrefix): AudioAction.IDEMPOTENT,
        (AudioPhase.BLOCKED, AuthenticatedConflict): AudioAction.BLOCK,
        (AudioPhase.BLOCKED, SourceIntegrityHold): AudioAction.BLOCK,
        (AudioPhase.BLOCKED, Barrier): AudioAction.INSPECT_ONLY,
        (AudioPhase.BLOCKED, Interruption): AudioAction.INSPECT_ONLY,
        (AudioPhase.COVERED, ReadyCustody): AudioAction.IDEMPOTENT,
        (AudioPhase.COVERED, BeginRetirement): AudioAction.RETIRE,
        (AudioPhase.COVERED, SourceIntegrityHold): AudioAction.BLOCK,
        (AudioPhase.RETIRING, RetirementStep): AudioAction.RETIRE,
        (AudioPhase.RETIRING, SourceIntegrityHold): AudioAction.BLOCK,
    }
    for phase, event in product(PHASES, EVENTS):
        decision = transition(phase, event)
        if (phase, type(event)) not in supported:
            assert decision.next_phase is phase
            assert decision.action is AudioAction.REFUSE
    assert transition(None, Create()).next_phase is AudioPhase.PREPARED
    assert transition(None, Barrier()).action is AudioAction.REFUSE


def test_prepare_leg_after_receiving_requires_atomic_barrier() -> None:
    assert transition(AudioPhase.RECEIVING, PrepareLeg()).action is AudioAction.REFUSE
    decision = transition(AudioPhase.RECEIVING, PrepareLeg(barrier_committed=True))
    _result(decision, AudioPhase.LEG_PREPARED, AudioAction.COMMIT_BARRIER)


def test_prepare_leg_identity_and_read_begin_guards() -> None:
    assert transition(AudioPhase.PREPARED, PrepareLeg(bounded=False)).action is AudioAction.REFUSE
    assert transition(AudioPhase.LEG_PREPARED, PrepareLeg(exact_identity=True)).action is AudioAction.IDEMPOTENT
    assert transition(AudioPhase.LEG_PREPARED, PrepareLeg()).action is AudioAction.REFUSE
    assert transition(AudioPhase.PREPARED, ReadBegin()).action is AudioAction.REFUSE
    assert transition(AudioPhase.LEG_PREPARED, ReadBegin(matching=False)).action is AudioAction.REFUSE
    assert transition(AudioPhase.RECEIVING, ReadBegin()).action is AudioAction.REFUSE
    assert transition(AudioPhase.RECEIVING, ReadBegin(duplicate=True)).action is AudioAction.IDEMPOTENT


def test_data_replay_and_conflict_branches() -> None:
    assert transition(AudioPhase.RECEIVING, Data()).action is AudioAction.PERSIST
    assert transition(AudioPhase.RECEIVING, Data(replay=True)).action is AudioAction.IDEMPOTENT
    conflict = transition(AudioPhase.RECEIVING, Data(matching=False))
    _result(conflict, AudioPhase.BLOCKED, AudioAction.BLOCK)
    assert transition(AudioPhase.RECEIVING, Data(matching=False, evidence_retained=False)).action is AudioAction.REFUSE
    assert transition(AudioPhase.BLOCKED, Data()).action is AudioAction.REFUSE


def test_done_interruption_and_authenticated_prefix_guards() -> None:
    assert transition(AudioPhase.RECEIVING, Done()).next_phase is AudioPhase.SEALED_WAITING
    assert transition(AudioPhase.INTERRUPTED, Done()).next_phase is AudioPhase.SEALED_WAITING
    assert transition(AudioPhase.INTERRUPTED, Done(authenticated_prefix=False)).action is AudioAction.REFUSE
    assert transition(AudioPhase.SEALED_WAITING, Done(exact_identity=True)).action is AudioAction.IDEMPOTENT
    assert transition(AudioPhase.PREPARED, Done()).action is AudioAction.REFUSE
    assert transition(AudioPhase.PREPARED, TerminalPrefix()).next_phase is AudioPhase.SEALED_WAITING
    assert transition(AudioPhase.RECEIVING, TerminalPrefix(authenticated=False)).action is AudioAction.REFUSE
    assert transition(AudioPhase.BLOCKED, TerminalPrefix()).action is AudioAction.REFUSE
    assert transition(AudioPhase.RECEIVING, Interruption()).next_phase is AudioPhase.INTERRUPTED
    assert transition(AudioPhase.INTERRUPTED, Interruption()).action is AudioAction.IDEMPOTENT


def test_interruption_before_read_begin_and_after_terminal_or_blocked_state() -> None:
    prepared = transition(AudioPhase.PREPARED, Interruption())
    assert prepared.next_phase is AudioPhase.INTERRUPTED
    leg_prepared = transition(AudioPhase.LEG_PREPARED, Interruption())
    assert leg_prepared.next_phase is AudioPhase.INTERRUPTED
    recovery_leg = transition(prepared.next_phase, PrepareLeg())
    assert recovery_leg.next_phase is AudioPhase.LEG_PREPARED
    assert transition(recovery_leg.next_phase, ReadBegin()).next_phase is AudioPhase.RECEIVING
    assert transition(AudioPhase.SEALED_WAITING, Interruption()).action is AudioAction.INSPECT_ONLY
    sealed = transition(AudioPhase.SEALED_WAITING, Interruption())
    assert sealed.next_phase is AudioPhase.SEALED_WAITING
    blocked = transition(AudioPhase.BLOCKED, Interruption())
    assert blocked.next_phase is AudioPhase.BLOCKED
    assert blocked.action is AudioAction.INSPECT_ONLY


def test_coverage_and_bounded_retirement() -> None:
    assert transition(AudioPhase.SEALED_WAITING, ReadyCustody()).next_phase is AudioPhase.COVERED
    assert transition(AudioPhase.BLOCKED, ReadyCustody()).action is AudioAction.REFUSE
    assert (
        transition(AudioPhase.BLOCKED, ReadyCustody(certified_quarantine_prefix=True)).next_phase is AudioPhase.COVERED
    )
    assert (
        transition(AudioPhase.BLOCKED, ReadyCustody(proved=False, certified_quarantine_prefix=True)).action
        is AudioAction.REFUSE
    )
    assert transition(AudioPhase.COVERED, BeginRetirement()).next_phase is AudioPhase.RETIRING
    assert transition(AudioPhase.RETIRING, RetirementStep()).next_phase is AudioPhase.RETIRING
    assert transition(AudioPhase.RETIRING, RetirementStep(final=True)).next_phase is None
    assert transition(AudioPhase.RETIRING, RetirementStep(custody_proved=False)).action is AudioAction.REFUSE
    for phase in (AudioPhase.COVERED, AudioPhase.RETIRING):
        for event in (PrepareLeg(), ReadBegin(), Data(), Ack()):
            assert transition(phase, event).action is AudioAction.REFUSE


def test_cohort_acceptance_permit_survives_2000_plus_1600_split() -> None:
    accepted = cohort_transition(CohortPhase.UNQUALIFIED, QualifiedDrained())
    assert accepted.next_phase is CohortPhase.ACCEPTED
    staged = cohort_transition(accepted.next_phase, Stage())
    assert staged.next_phase is CohortPhase.STAGING
    retry = cohort_transition(staged.next_phase, OwnedIncompleteRetry())
    assert retry.action is CohortAction.RECONCILE
    continued = cohort_transition(staged.next_phase, DurableDestination(cohort_complete=False))
    assert continued.next_phase is CohortPhase.ACCEPTED
    next_staged = cohort_transition(CohortPhase.ACCEPTED, Stage())
    assert next_staged.next_phase is CohortPhase.STAGING
    finished = cohort_transition(
        next_staged.next_phase,
        DurableDestination(cohort_complete=True),
    )
    assert finished.next_phase is CohortPhase.COMPLETE
    assert cohort_transition(CohortPhase.COMPLETE, CompleteRetire()).next_phase is None
    assert cohort_transition(CohortPhase.UNQUALIFIED, QualifiedDrained(False)).action is CohortAction.WAIT
    assert cohort_transition(CohortPhase.COMPLETE, CompleteRetire(False)).action is CohortAction.REFUSE
    existing = cohort_transition(CohortPhase.ACCEPTED, ExistingReadyCustody())
    assert (existing.next_phase, existing.action) == (CohortPhase.ACCEPTED, CohortAction.COMMIT)
    completed = cohort_transition(CohortPhase.ACCEPTED, ExistingReadyCustody(cohort_complete=True))
    assert (completed.next_phase, completed.action) == (CohortPhase.COMPLETE, CohortAction.COMMIT)
    blocked = cohort_transition(CohortPhase.ACCEPTED, CohortConflict())
    assert (blocked.next_phase, blocked.action) == (None, CohortAction.BLOCK)
    remainder = cohort_transition(CohortPhase.ACCEPTED, CohortConflict(accepted_remainder=True))
    assert (remainder.next_phase, remainder.action) == (CohortPhase.ACCEPTED, CohortAction.BLOCK)
    assert (
        cohort_transition(CohortPhase.ACCEPTED, ExistingReadyCustody(proved_progress=False)).action
        is CohortAction.REFUSE
    )


def test_cohort_invalid_cells_refuse_without_mutation() -> None:
    events = (
        QualifiedDrained(),
        Stage(),
        OwnedIncompleteRetry(),
        DurableDestination(),
        CompleteRetire(),
        ExistingReadyCustody(),
        CohortConflict(),
    )
    assert {type(event) for event in events} == set(get_args(CohortEvent))
    for phase, event in product(CohortPhase, events):
        decision = cohort_transition(phase, event)
        if not (
            (phase is CohortPhase.UNQUALIFIED and isinstance(event, QualifiedDrained))
            or (phase is CohortPhase.ACCEPTED and isinstance(event, Stage))
            or (phase is CohortPhase.ACCEPTED and isinstance(event, (ExistingReadyCustody, CohortConflict)))
            or (phase is CohortPhase.STAGING and isinstance(event, (OwnedIncompleteRetry, DurableDestination)))
            or (phase is CohortPhase.COMPLETE and isinstance(event, CompleteRetire))
        ):
            assert decision.next_phase is phase
            assert decision.action is CohortAction.REFUSE


def test_quarantine_prefix_below_threshold_survives_72h() -> None:
    prefix = quarantine_transition(QuarantineDisposition.UNCLASSIFIED, CertifiedPrefix())
    assert prefix.next_disposition is QuarantineDisposition.PUBLISHED_PREFIX
    assert prefix.action is QuarantineAction.PUBLISH_PREFIX
    retained = quarantine_transition(
        prefix.next_disposition,
        ResidualExpiry(age_expired=True),
    )
    assert retained.action is QuarantineAction.RETIRE
    assert retained.next_disposition is QuarantineDisposition.PUBLISHED_PREFIX
    assert (
        quarantine_transition(prefix.next_disposition, ResidualExpiry(age_expired=False)).action
        is QuarantineAction.KEEP
    )


def test_quarantine_residue_and_canonical_expiry_have_distinct_custody_guards() -> None:
    assert quarantine_transition(QuarantineDisposition.UNCLASSIFIED, ResidualExpiry()).action is QuarantineAction.REFUSE
    assert (
        quarantine_transition(QuarantineDisposition.PUBLISHED_PREFIX, ResidualExpiry(terminal=False)).action
        is QuarantineAction.REFUSE
    )
    for disposition in (QuarantineDisposition.UNCLASSIFIED, QuarantineDisposition.PUBLISHED_PREFIX):
        assert quarantine_transition(disposition, CanonicalExpiry(AudioPhase.BLOCKED)).action is QuarantineAction.REFUSE
    for event in (
        CanonicalExpiry(AudioPhase.COVERED),
        CanonicalExpiry(AudioPhase.BLOCKED, age_expired=False),
        CanonicalExpiry(AudioPhase.BLOCKED, cohort_references=True),
        CanonicalExpiry(AudioPhase.BLOCKED, stage_references=True),
    ):
        assert quarantine_transition(QuarantineDisposition.UNPROCESSABLE, event).action is not QuarantineAction.RETIRE
    assert (
        quarantine_transition(QuarantineDisposition.UNPROCESSABLE, CanonicalExpiry(AudioPhase.BLOCKED)).action
        is QuarantineAction.RETIRE
    )
    assert (
        quarantine_transition(QuarantineDisposition.UNCLASSIFIED, QuarantineOpened(1)).action is QuarantineAction.KEEP
    )
    assert (
        quarantine_transition(QuarantineDisposition.UNCLASSIFIED, QuarantineOpened(0)).next_disposition
        is QuarantineDisposition.UNPROCESSABLE
    )
    assert (
        quarantine_transition(
            QuarantineDisposition.UNCLASSIFIED, QuarantineOpened(0, has_canonical_records=True)
        ).action
        is QuarantineAction.KEEP
    )
    assert (
        quarantine_transition(QuarantineDisposition.UNCLASSIFIED, QuarantineOpened(-1)).action
        is QuarantineAction.REFUSE
    )
    assert (
        quarantine_transition(QuarantineDisposition.UNCLASSIFIED, MarkUnprocessable("bad bytes")).next_disposition
        is QuarantineDisposition.UNPROCESSABLE
    )
    assert (
        quarantine_transition(QuarantineDisposition.UNCLASSIFIED, MarkUnprocessable("")).action
        is QuarantineAction.REFUSE
    )
    assert (
        quarantine_transition(
            QuarantineDisposition.UNCLASSIFIED, MarkUnprocessable("conflict", custody_conflict=True)
        ).action
        is QuarantineAction.REFUSE
    )
    assert (
        quarantine_transition(QuarantineDisposition.PUBLISHED_PREFIX, MarkUnprocessable("again")).action
        is QuarantineAction.REFUSE
    )


def test_quarantine_event_matrix_refuses_unsupported_disposition_changes() -> None:
    events = (
        QuarantineOpened(1),
        CertifiedPrefix(),
        MarkUnprocessable("invalid bytes"),
        ResidualExpiry(),
        CanonicalExpiry(AudioPhase.BLOCKED),
    )
    assert {type(event) for event in events} == set(get_args(QuarantineEvent))
    allowed = {
        (QuarantineDisposition.UNCLASSIFIED, QuarantineOpened),
        (QuarantineDisposition.UNCLASSIFIED, CertifiedPrefix),
        (QuarantineDisposition.UNCLASSIFIED, MarkUnprocessable),
        (QuarantineDisposition.UNCLASSIFIED, ResidualExpiry),
        (QuarantineDisposition.PUBLISHED_PREFIX, ResidualExpiry),
        (QuarantineDisposition.UNPROCESSABLE, ResidualExpiry),
        (QuarantineDisposition.UNPROCESSABLE, CanonicalExpiry),
    }
    for disposition, event in product(QuarantineDisposition, events):
        decision = quarantine_transition(disposition, event)
        if (disposition, type(event)) not in allowed:
            assert decision.next_disposition is disposition
            assert decision.action is QuarantineAction.REFUSE


@pytest.mark.parametrize("value", [object(), "prepared", 1])
def test_audio_transition_rejects_invalid_phase_without_mutation(value: object) -> None:
    decision = transition(value, Create())  # type: ignore[arg-type]
    assert decision.next_phase is None
    assert decision.action is AudioAction.REFUSE
