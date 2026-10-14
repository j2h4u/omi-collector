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
class TerminalPrefix:
    """Seal an authenticated durable prefix during recovery or drain."""

    authenticated: bool = True


@dataclass(frozen=True, slots=True)
class Interruption:
    """Record a recoverable transport interruption."""


@dataclass(frozen=True, slots=True)
class AuthenticatedConflict:
    """Record a conflict whose rejected bytes can be retained exactly."""

    authenticated: bool = True


@dataclass(frozen=True, slots=True)
class SourceIntegrityHold:
    """Retain quarantine bytes whose canonical records contradict their checkpoint."""


@dataclass(frozen=True, slots=True)
class ReadyCustody:
    """Prove complete durable ready custody and source lineage."""

    proved: bool = True
    certified_quarantine_prefix: bool = False


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
    | TerminalPrefix
    | Interruption
    | AuthenticatedConflict
    | SourceIntegrityHold
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


def _terminal_prefix(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, TerminalPrefix)
    if event.authenticated and phase in {
        AudioPhase.PREPARED,
        AudioPhase.LEG_PREPARED,
        AudioPhase.RECEIVING,
        AudioPhase.INTERRUPTED,
    }:
        return AudioDecision(AudioPhase.SEALED_WAITING, AudioAction.PERSIST, "authenticated terminal prefix sealed")
    if event.authenticated and phase is AudioPhase.SEALED_WAITING:
        return AudioDecision(AudioPhase.SEALED_WAITING, AudioAction.IDEMPOTENT, "terminal prefix already sealed")
    return _refuse(phase, "terminal prefix is unauthenticated or source phase is invalid")


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


def _source_integrity_hold(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, SourceIntegrityHold)
    if phase in {AudioPhase.BLOCKED, AudioPhase.COVERED, AudioPhase.RETIRING}:
        return AudioDecision(AudioPhase.BLOCKED, AudioAction.BLOCK, "source integrity mismatch retained")
    return _refuse(phase, "integrity hold requires quarantine custody")


def _ready_custody(phase: AudioPhase | None, event: object) -> AudioDecision:
    assert isinstance(event, ReadyCustody)
    if event.proved and (
        phase is AudioPhase.SEALED_WAITING or (phase is AudioPhase.BLOCKED and event.certified_quarantine_prefix)
    ):
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
    TerminalPrefix: _terminal_prefix,
    Interruption: _interruption,
    AuthenticatedConflict: _conflict,
    SourceIntegrityHold: _source_integrity_hold,
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
    BLOCK = "block"
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


@dataclass(frozen=True, slots=True)
class ExistingReadyCustody:
    """Proven existing ready output advances an accepted cohort without staging."""

    proved_progress: bool = True
    cohort_complete: bool = False


@dataclass(frozen=True, slots=True)
class CohortConflict:
    """Exclude a conflicting member while preserving any accepted remainder."""

    accepted_remainder: bool = False


CohortEvent = (
    QualifiedDrained
    | Stage
    | OwnedIncompleteRetry
    | DurableDestination
    | CompleteRetire
    | ExistingReadyCustody
    | CohortConflict
)


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
    elif isinstance(event, ExistingReadyCustody) and phase is CohortPhase.ACCEPTED and event.proved_progress:
        next_phase = CohortPhase.COMPLETE if event.cohort_complete else CohortPhase.ACCEPTED
        decision = CohortDecision(next_phase, CohortAction.COMMIT, "existing ready custody advanced cohort")
    elif isinstance(event, CohortConflict) and phase is CohortPhase.ACCEPTED:
        next_phase = CohortPhase.ACCEPTED if event.accepted_remainder else None
        decision = CohortDecision(next_phase, CohortAction.BLOCK, "custody conflict excluded from accepted cohort")
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


QUARANTINE_CUSTODY_HOLD_REASONS = frozenset({"ready_overlap_conflict", "source_authentication_mismatch"})


@dataclass(frozen=True, slots=True)
class CertifiedPrefix:
    checkpoint_certified: bool = True
    salvage_eligible: bool = True


@dataclass(frozen=True, slots=True)
class MarkUnprocessable:
    reason: str
    custody_conflict: bool = False


@dataclass(frozen=True, slots=True)
class QuarantineOpened:
    """Classify an authenticated capture conflict at the original SQL barrier."""

    committed_count: int
    has_canonical_records: bool = False


@dataclass(frozen=True, slots=True)
class ResidualExpiry:
    terminal: bool = True
    age_expired: bool = True


@dataclass(frozen=True, slots=True)
class CanonicalExpiry:
    """Delete canonical bytes only after terminal expiry without publication refs."""

    source_phase: AudioPhase
    age_expired: bool = True
    cohort_references: bool = False
    stage_references: bool = False


QuarantineEvent = CertifiedPrefix | MarkUnprocessable | QuarantineOpened | ResidualExpiry | CanonicalExpiry


@dataclass(frozen=True, slots=True)
class QuarantineDecision:
    next_disposition: QuarantineDisposition | None
    action: QuarantineAction
    reason: str


def quarantine_transition(disposition: QuarantineDisposition, event: QuarantineEvent) -> QuarantineDecision:
    """Reduce quarantine classification and guarded residual retention."""

    if not isinstance(disposition, QuarantineDisposition):
        return QuarantineDecision(None, QuarantineAction.REFUSE, "unsupported quarantine disposition")
    handler = _QUARANTINE_HANDLERS.get(type(event))
    return (
        QuarantineDecision(disposition, QuarantineAction.REFUSE, "unsupported quarantine event")
        if handler is None
        else handler(disposition, event)
    )


def _quarantine_refuse(disposition: QuarantineDisposition) -> QuarantineDecision:
    return QuarantineDecision(disposition, QuarantineAction.REFUSE, "quarantine event is invalid for this disposition")


def _quarantine_opened(disposition: QuarantineDisposition, event: object) -> QuarantineDecision:
    assert isinstance(event, QuarantineOpened)
    if (
        disposition is not QuarantineDisposition.UNCLASSIFIED
        or type(event.committed_count) is not int
        or type(event.has_canonical_records) is not bool
    ):
        return _quarantine_refuse(disposition)
    if event.committed_count < 0:
        return _quarantine_refuse(disposition)
    next_disposition = (
        QuarantineDisposition.UNCLASSIFIED
        if event.committed_count or event.has_canonical_records
        else QuarantineDisposition.UNPROCESSABLE
    )
    action = (
        QuarantineAction.KEEP
        if event.committed_count or event.has_canonical_records
        else QuarantineAction.MARK_UNPROCESSABLE
    )
    return QuarantineDecision(next_disposition, action, "authenticated conflict retained")


def _certified_prefix(disposition: QuarantineDisposition, event: object) -> QuarantineDecision:
    assert isinstance(event, CertifiedPrefix)
    if disposition is not QuarantineDisposition.UNCLASSIFIED or not (
        event.checkpoint_certified and event.salvage_eligible
    ):
        return _quarantine_refuse(disposition)
    return QuarantineDecision(
        QuarantineDisposition.PUBLISHED_PREFIX, QuarantineAction.PUBLISH_PREFIX, "certified salvage prefix"
    )


def _mark_unprocessable(disposition: QuarantineDisposition, event: object) -> QuarantineDecision:
    assert isinstance(event, MarkUnprocessable)
    if disposition is not QuarantineDisposition.UNCLASSIFIED or not event.reason or event.custody_conflict:
        return _quarantine_refuse(disposition)
    return QuarantineDecision(
        QuarantineDisposition.UNPROCESSABLE, QuarantineAction.MARK_UNPROCESSABLE, "terminal reason recorded"
    )


def _residual_expiry(disposition: QuarantineDisposition, event: object) -> QuarantineDecision:
    assert isinstance(event, ResidualExpiry)
    if disposition is QuarantineDisposition.UNCLASSIFIED or not event.terminal:
        return _quarantine_refuse(disposition)
    if not event.age_expired:
        return QuarantineDecision(disposition, QuarantineAction.KEEP, "residual evidence has not expired")
    return QuarantineDecision(disposition, QuarantineAction.RETIRE, "expired residual evidence retired")


def _canonical_expiry(disposition: QuarantineDisposition, event: object) -> QuarantineDecision:
    assert isinstance(event, CanonicalExpiry)
    if disposition is not QuarantineDisposition.UNPROCESSABLE or event.source_phase is not AudioPhase.BLOCKED:
        return _quarantine_refuse(disposition)
    if not event.age_expired or event.cohort_references or event.stage_references:
        return QuarantineDecision(disposition, QuarantineAction.KEEP, "canonical source remains pinned")
    return QuarantineDecision(disposition, QuarantineAction.RETIRE, "expired unreferenced canonical source retired")


_QUARANTINE_HANDLERS: dict[type[object], Callable[[QuarantineDisposition, object], QuarantineDecision]] = {
    QuarantineOpened: _quarantine_opened,
    CertifiedPrefix: _certified_prefix,
    MarkUnprocessable: _mark_unprocessable,
    ResidualExpiry: _residual_expiry,
    CanonicalExpiry: _canonical_expiry,
}
