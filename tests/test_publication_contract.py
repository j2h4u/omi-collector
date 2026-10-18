from __future__ import annotations

from itertools import product
from typing import cast

import pytest

from omi_collector.capture.domain.publication_contract import (
    _POLICIES,
    _TRANSITIONS,
    ContractEvent,
    ContractMode,
    ContractPolicy,
    contract_policy,
    contract_transition,
)


@pytest.mark.parametrize(
    ("mode", "expected"),
    (
        (ContractMode.LEGACY_OPEN, ContractPolicy(True, True, False)),
        (ContractMode.QUIESCING, ContractPolicy(False, True, False)),
        (ContractMode.V2_OPEN, ContractPolicy(False, False, True)),
    ),
)
def test_policy_is_fixed_for_each_mode(mode: ContractMode, expected: ContractPolicy) -> None:
    assert contract_policy(mode) == expected


def test_every_mode_event_pair_has_an_explicit_result_or_rejection() -> None:
    valid = {
        (ContractMode.LEGACY_OPEN, ContractEvent.BEGIN_CUTOVER): ContractMode.QUIESCING,
        (ContractMode.QUIESCING, ContractEvent.ACTIVATE_V2): ContractMode.V2_OPEN,
    }
    for mode, event in product(ContractMode, ContractEvent):
        if (mode, event) in valid:
            assert contract_transition(mode, event) is valid[mode, event]
        else:
            with pytest.raises(ValueError, match="unsupported"):
                contract_transition(mode, event)


def test_transition_and_policy_tables_cover_the_complete_declared_space() -> None:
    assert set(_POLICIES) == set(ContractMode)
    assert set(_TRANSITIONS) == set(product(ContractMode, ContractEvent))


@pytest.mark.parametrize("value", ("legacy_open", "invalid", None, 1, True))
def test_invalid_modes_are_rejected(value: object) -> None:
    with pytest.raises(ValueError):
        contract_policy(cast(ContractMode, value))


def test_invalid_event_is_rejected() -> None:
    with pytest.raises(ValueError):
        contract_transition(ContractMode.LEGACY_OPEN, cast(ContractEvent, "activate_v2"))


def test_non_enum_mode_and_event_values_are_rejected() -> None:
    with pytest.raises(ValueError):
        contract_transition(cast(ContractMode, "legacy_open"), ContractEvent.BEGIN_CUTOVER)
    with pytest.raises(ValueError):
        contract_transition(ContractMode.LEGACY_OPEN, cast(ContractEvent, "begin_cutover"))
