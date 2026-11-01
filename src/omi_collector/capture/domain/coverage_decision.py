"""Decide the lifecycle after ready custody covers an admitted source."""

from .internal_audio_machine import AudioAction, AudioPhase, BeginRetirement, ReadyCustody, transition


class CoverageDecisionError(ValueError):
    """The source cannot enter the requested coverage state."""


def decide_coverage(lifecycle: str, admitted_count: int, committed_count: int) -> tuple[AudioPhase, int | None]:
    """Choose retirement or retain an uncovered certified prefix."""
    full = admitted_count == committed_count
    if not full and lifecycle != "blocked":
        raise CoverageDecisionError("only a certified blocked prefix may retain uncovered source rows")
    decision = transition(AudioPhase(lifecycle), ReadyCustody(certified_quarantine_prefix=lifecycle == "blocked"))
    if decision.action is not AudioAction.RETIRE or decision.next_phase is not AudioPhase.COVERED:
        raise CoverageDecisionError("capture source FSM refused ReadyCustody")
    if not full:
        return AudioPhase.COVERED, None
    decision = transition(AudioPhase.COVERED, BeginRetirement())
    if decision.action is not AudioAction.RETIRE or decision.next_phase is not AudioPhase.RETIRING:
        raise CoverageDecisionError("capture source FSM refused BeginRetirement")
    return AudioPhase.RETIRING, 0
