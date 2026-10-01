"""Pure ready-publication decision from durable capture facts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReadyState(StrEnum):
    WAITING_FOR_DRAIN = "waiting_for_drain"
    WAITING_FOR_THRESHOLD = "waiting_for_threshold"
    READY_TO_PUBLISH = "ready_to_publish"
    INVALID_GAP = "invalid_gap"


class ReadyCommand(StrEnum):
    WAIT = "wait"
    PUBLISH = "publish"


@dataclass(frozen=True, slots=True)
class ReadyDecision:
    state: ReadyState
    command: ReadyCommand


def decide_ready(*, drained: bool, has_audio: bool, threshold_met: bool, contiguous: bool) -> ReadyDecision:
    """Derive the only allowed next action without adding durable state."""
    if threshold_met and not has_audio:
        raise ValueError("an empty audio set cannot meet the publication threshold")
    if not drained:
        return ReadyDecision(ReadyState.WAITING_FOR_DRAIN, ReadyCommand.WAIT)
    if not has_audio or not threshold_met:
        return ReadyDecision(ReadyState.WAITING_FOR_THRESHOLD, ReadyCommand.WAIT)
    if not contiguous:
        return ReadyDecision(ReadyState.INVALID_GAP, ReadyCommand.WAIT)
    return ReadyDecision(ReadyState.READY_TO_PUBLISH, ReadyCommand.PUBLISH)
