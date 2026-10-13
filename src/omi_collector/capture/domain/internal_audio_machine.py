"""Pure lifecycle reducers for internal audio, cohorts, and quarantine evidence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class AudioPhase(StrEnum):
    """Durable lifecycle phases for one internal capture source."""

    PREPARED = "prepared"
    LEG_PREPARED = "leg_prepared"
    RECEIVING = "receiving"
    INTERRUPTED = "interrupted"
    SEALED_WAITING = "sealed_waiting"
    BLOCKED = "blocked"
    COVERED = "covered"
    RETIRING = "retiring"


class AudioAction(StrEnum):
    PERSIST = "persist"
    IDEMPOTENT = "idempotent"
    COMMIT_BARRIER = "commit_barrier"
    INSPECT_ONLY = "inspect_only"
    RETIRE = "retire"
    REFUSE = "refuse"
    BLOCK = "block"


@dataclass(frozen=True, slots=True)
class Create:
    """Create a source before its first leg is prepared."""


@dataclass(frozen=True, slots=True)
class PrepareLeg:
    """Prepare a bounded leg; callers supply the validation guards."""

    bounded: bool = True
    exact_identity: bool = False
    barrier_committed: bool = False


@dataclass(frozen=True, slots=True)
class ReadBegin:
    """Bind the prepared leg to the device's matching READ_BEGIN."""

    matching: bool = True
    duplicate: bool = False


@dataclass(frozen=True, slots=True)
class Data:
    """Record data or an exact replay of already accepted data."""

    matching: bool = True
    replay: bool = False
    evidence_retained: bool = True


@dataclass(frozen=True, slots=True)
class Barrier:
    """Durably commit the current source frontier."""


@dataclass(frozen=True, slots=True)
class Done:
    """A validated DONE or authenticated terminal prefix."""

    validated: bool = True
    authenticated_prefix: bool = True
    exact_identity: bool = False


@dataclass(frozen=True, slots=True)
class Interruption:
    """Record a recoverable transport interruption."""


@dataclass(frozen=True, slots=True)
class AuthenticatedConflict:
    """Record a conflict whose rejected bytes can be retained exactly."""

    authenticated: bool = True


@dataclass(frozen=True, slots=True)
class ReadyCustody:
    """Prove complete durable ready custody and source lineage."""

    proved: bool = True


@dataclass(frozen=True, slots=True)
class BeginRetirement:
    """Start bounded deletion after the source is covered."""


@dataclass(frozen=True, slots=True)
class RetirementStep:
    """Advance a proven bounded retirement cursor, or remove the source."""

    custody_proved: bool = True
    final: bool = False


@dataclass(frozen=True, slots=True)
class Ack:
    """An external ready ACK, which never authorizes source capture retirement."""


AudioEvent = (
    Create
    | PrepareLeg
    | ReadBegin
    | Data
    | Barrier
    | Done
    | Interruption
    | AuthenticatedConflict
    | ReadyCustody
    | BeginRetirement
    | RetirementStep
    | Ack
)


@dataclass(frozen=True, slots=True)
class AudioDecision:
    """The next phase and the one adapter operation authorized for it."""

    next_phase: AudioPhase | None
    action: AudioAction
    reason: str


def _refuse(phase: AudioPhase | None, reason: str) -> AudioDecision:
    return AudioDecision(phase, AudioAction.REFUSE, reason)


def transition(phase: AudioPhase | None, event: AudioEvent) -> AudioDecision:
    """Apply one audio lifecycle event without mutating state or doing I/O."""

    if phase is not None and not isinstance(phase, AudioPhase):
        return _refuse(None, "unsupported audio phase")
    handler = _AUDIO_HANDLERS.get(type(event))
    if handler is None:
        return _refuse(phase, "unsupported audio event")
    return handler(phase, event)


def _prepare_leg(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, PrepareLeg)
    if not event.bounded:
        return _refuse(phase, "leg is outside the immutable source bounds")
    if phase is AudioPhase.PREPARED:
        return AudioDecision(AudioPhase.LEG_PREPARED, AudioAction.PERSIST, "bounded leg prepared")
    if phase is AudioPhase.INTERRUPTED:
        return AudioDecision(AudioPhase.LEG_PREPARED, AudioAction.PERSIST, "recovery leg prepared")
    if phase is AudioPhase.LEG_PREPARED and event.exact_identity:
        return AudioDecision(AudioPhase.LEG_PREPARED, AudioAction.IDEMPOTENT, "same leg already prepared")
    if phase is AudioPhase.RECEIVING and event.barrier_committed:
        return AudioDecision(AudioPhase.LEG_PREPARED, AudioAction.COMMIT_BARRIER, "barrier then prepare leg")
    return _refuse(phase, "PREPARE_LEG is not valid for this source phase")


def _barrier(phase: AudioPhase | None) -> AudioDecision:
    if phase is AudioPhase.PREPARED:
        return AudioDecision(AudioPhase.PREPARED, AudioAction.PERSIST, "durable zero prefix")
    if phase in {AudioPhase.LEG_PREPARED, AudioPhase.RECEIVING, AudioPhase.INTERRUPTED}:
        return AudioDecision(phase, AudioAction.COMMIT_BARRIER, "frontier barrier committed")
    if phase in {AudioPhase.SEALED_WAITING, AudioPhase.BLOCKED}:
        return AudioDecision(phase, AudioAction.INSPECT_ONLY, "barrier is inspection only")
    return _refuse(phase, "barrier is not valid after source coverage")


def _done(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, Done)
    if phase in {AudioPhase.RECEIVING, AudioPhase.INTERRUPTED} and event.validated and event.authenticated_prefix:
        return AudioDecision(AudioPhase.SEALED_WAITING, AudioAction.PERSIST, "authenticated source sealed")
    if phase is AudioPhase.SEALED_WAITING and event.validated and event.authenticated_prefix and event.exact_identity:
        return AudioDecision(AudioPhase.SEALED_WAITING, AudioAction.IDEMPOTENT, "seal already recorded")
    return _refuse(phase, "DONE lacks a validated authenticated prefix")


def _create(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, Create)
    return (
        AudioDecision(AudioPhase.PREPARED, AudioAction.PERSIST, "source created")
        if phase is None
        else _refuse(phase, "source already exists")
    )


def _read_begin(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, ReadBegin)
    if phase is AudioPhase.LEG_PREPARED and event.matching:
        return AudioDecision(AudioPhase.RECEIVING, AudioAction.PERSIST, "READ_BEGIN bound")
    if phase is AudioPhase.RECEIVING and event.matching and event.duplicate:
        return AudioDecision(AudioPhase.RECEIVING, AudioAction.IDEMPOTENT, "READ_BEGIN replay")
    return _refuse(phase, "READ_BEGIN is not a matching permitted leg binding")


def _data(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, Data)
    if phase is AudioPhase.RECEIVING and event.matching:
        action = AudioAction.IDEMPOTENT if event.replay else AudioAction.PERSIST
        return AudioDecision(AudioPhase.RECEIVING, action, "record data accepted")
    conflict_phases = {
        AudioPhase.PREPARED,
        AudioPhase.LEG_PREPARED,
        AudioPhase.RECEIVING,
        AudioPhase.INTERRUPTED,
    }
    if phase in conflict_phases and not event.matching and event.evidence_retained:
        return AudioDecision(AudioPhase.BLOCKED, AudioAction.BLOCK, "record conflict retained")
    return _refuse(phase, "record data requires a matching active READ_BEGIN")


def _interruption(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, Interruption)
    if phase in {AudioPhase.PREPARED, AudioPhase.LEG_PREPARED, AudioPhase.RECEIVING}:
        return AudioDecision(AudioPhase.INTERRUPTED, AudioAction.PERSIST, "transfer interrupted")
    if phase is AudioPhase.INTERRUPTED:
        return AudioDecision(AudioPhase.INTERRUPTED, AudioAction.IDEMPOTENT, "interruption already recorded")
    if phase in {AudioPhase.SEALED_WAITING, AudioPhase.BLOCKED}:
        return AudioDecision(phase, AudioAction.INSPECT_ONLY, "terminal phase is unchanged")
    return _refuse(phase, "interruption requires receiving audio")


def _conflict(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, AuthenticatedConflict)
    blockable_phases = {
        AudioPhase.PREPARED,
        AudioPhase.LEG_PREPARED,
        AudioPhase.RECEIVING,
        AudioPhase.INTERRUPTED,
        AudioPhase.SEALED_WAITING,
        AudioPhase.BLOCKED,
    }
    if not event.authenticated:
        return _refuse(phase, "conflict lacks authenticated rejected evidence")
    if phase in blockable_phases:
        return AudioDecision(AudioPhase.BLOCKED, AudioAction.BLOCK, "authenticated conflict retained")
    return _refuse(phase, "covered source cannot accept conflict evidence")


def _ready_custody(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, ReadyCustody)
    if phase is AudioPhase.SEALED_WAITING and event.proved:
        return AudioDecision(AudioPhase.COVERED, AudioAction.RETIRE, "ready custody covers source")
    if phase is AudioPhase.COVERED and event.proved:
        return AudioDecision(AudioPhase.COVERED, AudioAction.IDEMPOTENT, "ready custody already recorded")
    return _refuse(phase, "full ready custody is not valid for this source")


def _begin_retirement(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, BeginRetirement)
    if phase is AudioPhase.COVERED:
        return AudioDecision(AudioPhase.RETIRING, AudioAction.RETIRE, "retirement cursor started")
    return _refuse(phase, "retirement requires covered source")


def _retirement_step(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, RetirementStep)
    if phase is AudioPhase.RETIRING and event.custody_proved:
        next_phase = None if event.final else AudioPhase.RETIRING
        return AudioDecision(next_phase, AudioAction.RETIRE, "retirement cursor advanced")
    return _refuse(phase, "retirement requires current custody proof")


def _ack(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, Ack)
    return _refuse(phase, "ACK cannot change internal source lifecycle")


_AUDIO_HANDLERS: dict[type[object], Callable[[AudioPhase | None, object], AudioDecision]] = {
    Create: _create,
    PrepareLeg: _prepare_leg,
    ReadBegin: _read_begin,
    Data: _data,
    Barrier: lambda phase, _event: _barrier(phase),
    Done: _done,
    Interruption: _interruption,
    AuthenticatedConflict: _conflict,
    ReadyCustody: _ready_custody,
    BeginRetirement: _begin_retirement,
    RetirementStep: _retirement_step,
    Ack: _ack,
}


class CohortPhase(StrEnum):
    """Publication-cohort phases, including the persisted accepted permit."""

    UNQUALIFIED = "unqualified"
    ACCEPTED = "accepted"
    STAGING = "staging"
    COMPLETE = "complete"


class CohortAction(StrEnum):
    WAIT = "wait"
    ACCEPT = "accept"
    STAGE = "stage"
    RECONCILE = "reconcile"
    COMMIT = "commit"
    RETIRE = "retire"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class QualifiedDrained:
    qualified: bool = True


@dataclass(frozen=True, slots=True)
class Stage:
    next_group: bool = True


@dataclass(frozen=True, slots=True)
class OwnedIncompleteRetry:
    owned: bool = True


@dataclass(frozen=True, slots=True)
class DurableDestination:
    durable: bool = True
    cohort_complete: bool = False


@dataclass(frozen=True, slots=True)
class CompleteRetire:
    all_coverage_proved: bool = True


CohortEvent = QualifiedDrained | Stage | OwnedIncompleteRetry | DurableDestination | CompleteRetire


@dataclass(frozen=True, slots=True)
class CohortDecision:
    next_phase: CohortPhase | None
    action: CohortAction
    reason: str


def cohort_transition(phase: CohortPhase, event: CohortEvent) -> CohortDecision:
    """Reduce one publication-cohort event without reapplying its threshold."""

    if not isinstance(phase, CohortPhase):
        return CohortDecision(None, CohortAction.REFUSE, "unsupported cohort phase")
    decision = CohortDecision(phase, CohortAction.REFUSE, "cohort event is invalid for this phase")
    if isinstance(event, QualifiedDrained) and phase is CohortPhase.UNQUALIFIED:
        if event.qualified:
            decision = CohortDecision(CohortPhase.ACCEPTED, CohortAction.ACCEPT, "cohort permit persisted")
        else:
            decision = CohortDecision(CohortPhase.UNQUALIFIED, CohortAction.WAIT, "drained threshold is not qualified")
    elif isinstance(event, Stage) and phase is CohortPhase.ACCEPTED and event.next_group:
        decision = CohortDecision(CohortPhase.STAGING, CohortAction.STAGE, "accepted group staged")
    elif isinstance(event, OwnedIncompleteRetry) and phase is CohortPhase.STAGING and event.owned:
        decision = CohortDecision(CohortPhase.STAGING, CohortAction.RECONCILE, "owned stage reconciled")
    elif isinstance(event, DurableDestination) and phase is CohortPhase.STAGING and event.durable:
        next_phase = CohortPhase.COMPLETE if event.cohort_complete else CohortPhase.ACCEPTED
        decision = CohortDecision(next_phase, CohortAction.COMMIT, "durable destination committed")
    elif isinstance(event, CompleteRetire) and phase is CohortPhase.COMPLETE and event.all_coverage_proved:
        decision = CohortDecision(None, CohortAction.RETIRE, "cohort coverage retired")
    return decision


class QuarantineDisposition(StrEnum):
    """Disposition of residual quarantine evidence and its certified prefix."""

    UNCLASSIFIED = "unclassified"
    PUBLISHED_PREFIX = "published_prefix"
    UNPROCESSABLE = "unprocessable"


class QuarantineAction(StrEnum):
    PUBLISH_PREFIX = "publish_prefix"
    MARK_UNPROCESSABLE = "mark_unprocessable"
    KEEP = "keep"
    RETIRE = "retire"
    REFUSE = "refuse"


@dataclass(frozen=True, slots=True)
class CertifiedPrefix:
    checkpoint_certified: bool = True
    salvage_eligible: bool = True


@dataclass(frozen=True, slots=True)
class MarkUnprocessable:
    reason: str


@dataclass(frozen=True, slots=True)
class ResidualExpiry:
    terminal: bool = True
    age_expired: bool = True
    pending_references: bool = False
    cohort_references: bool = False
    ready_references: bool = False


QuarantineEvent = CertifiedPrefix | MarkUnprocessable | ResidualExpiry


@dataclass(frozen=True, slots=True)
class QuarantineDecision:
    next_disposition: QuarantineDisposition | None
    action: QuarantineAction
    reason: str


def quarantine_transition(disposition: QuarantineDisposition, event: QuarantineEvent) -> QuarantineDecision:
    """Reduce quarantine classification and guarded residual retention."""

    if not isinstance(disposition, QuarantineDisposition):
        return QuarantineDecision(None, QuarantineAction.REFUSE, "unsupported quarantine disposition")
    decision = QuarantineDecision(
        disposition, QuarantineAction.REFUSE, "quarantine event is invalid for this disposition"
    )
    if isinstance(event, CertifiedPrefix) and disposition is QuarantineDisposition.UNCLASSIFIED:
        if event.checkpoint_certified and event.salvage_eligible:
            decision = QuarantineDecision(
                QuarantineDisposition.PUBLISHED_PREFIX, QuarantineAction.PUBLISH_PREFIX, "certified salvage prefix"
            )
        else:
            decision = QuarantineDecision(
                disposition, QuarantineAction.REFUSE, "prefix checkpoint or salvage is untrusted"
            )
    elif isinstance(event, MarkUnprocessable) and disposition is QuarantineDisposition.UNCLASSIFIED:
        if event.reason:
            decision = QuarantineDecision(
                QuarantineDisposition.UNPROCESSABLE, QuarantineAction.MARK_UNPROCESSABLE, "terminal reason recorded"
            )
        else:
            decision = QuarantineDecision(disposition, QuarantineAction.REFUSE, "unprocessable reason is required")
    elif isinstance(event, ResidualExpiry) and disposition in {
        QuarantineDisposition.PUBLISHED_PREFIX,
        QuarantineDisposition.UNPROCESSABLE,
    }:
        if not event.terminal:
            decision = QuarantineDecision(
                disposition, QuarantineAction.REFUSE, "nonterminal evidence cannot be retired"
            )
        elif not event.age_expired or any((event.pending_references, event.cohort_references, event.ready_references)):
            decision = QuarantineDecision(disposition, QuarantineAction.KEEP, "residual evidence remains referenced")
        else:
            decision = QuarantineDecision(disposition, QuarantineAction.RETIRE, "expired unreferenced residual retired")
    return decision
