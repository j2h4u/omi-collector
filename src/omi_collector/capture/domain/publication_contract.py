"""Pure transition and policy table for publication contract modes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from itertools import product


class ContractMode(StrEnum):
    LEGACY_OPEN = "legacy_open"
    QUIESCING = "quiescing"
    V2_OPEN = "v2_open"


class ContractEvent(StrEnum):
    BEGIN_CUTOVER = "begin_cutover"
    ACTIVATE_V2 = "activate_v2"


@dataclass(frozen=True, slots=True)
class ContractPolicy:
    allow_legacy_accept: bool
    allow_legacy_resume: bool
    allow_v2_accept: bool


_POLICIES: dict[ContractMode, ContractPolicy] = {
    ContractMode.LEGACY_OPEN: ContractPolicy(True, True, False),
    ContractMode.QUIESCING: ContractPolicy(False, True, False),
    ContractMode.V2_OPEN: ContractPolicy(False, False, True),
}
_TRANSITIONS: dict[tuple[ContractMode, ContractEvent], ContractMode | None] = {
    (ContractMode.LEGACY_OPEN, ContractEvent.BEGIN_CUTOVER): ContractMode.QUIESCING,
    (ContractMode.LEGACY_OPEN, ContractEvent.ACTIVATE_V2): None,
    (ContractMode.QUIESCING, ContractEvent.BEGIN_CUTOVER): None,
    (ContractMode.QUIESCING, ContractEvent.ACTIVATE_V2): ContractMode.V2_OPEN,
    (ContractMode.V2_OPEN, ContractEvent.BEGIN_CUTOVER): None,
    (ContractMode.V2_OPEN, ContractEvent.ACTIVATE_V2): None,
}


def _check_completeness() -> None:
    if _POLICIES.keys() != set(ContractMode) or _TRANSITIONS.keys() != set(product(ContractMode, ContractEvent)):
        raise AssertionError("publication contract tables are incomplete")


def contract_policy(mode: ContractMode) -> ContractPolicy:
    """Return the fixed acceptance policy for one contract mode."""
    _check_completeness()
    if type(mode) is not ContractMode:
        raise ValueError("invalid publication contract mode")
    return _POLICIES[mode]


def contract_transition(mode: ContractMode, event: ContractEvent) -> ContractMode:
    """Apply one explicitly supported contract transition."""
    _check_completeness()
    if type(mode) is not ContractMode or type(event) is not ContractEvent:
        raise ValueError("invalid publication contract transition")
    result = _TRANSITIONS[mode, event]
    if result is None:
        raise ValueError("unsupported publication contract transition")
    return result
