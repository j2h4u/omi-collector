from __future__ import annotations

from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, publication_contract_state
from omi_collector.capture.domain.publication_contract import ContractMode


def test_new_database_starts_with_v2_publication_order_zero(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")

    assert publication_contract_state(database).mode is ContractMode.V2_OPEN
    assert publication_contract_state(database).next_public_order == 0


@pytest.mark.parametrize("mode", ["legacy_open", "quiescing"])
def test_legacy_publication_modes_are_rejected(tmp_path: Path, mode: str) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    database.put(
        StateKind.PUBLICATION_CONTRACT,
        "current",
        {"version": 1, "mode": mode, "next_public_order": 0},
    )

    with pytest.raises(ReadyBundleError, match="contract mode"):
        publication_contract_state(database)


def test_missing_v2_order_is_rejected(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    database.put(
        StateKind.PUBLICATION_CONTRACT,
        "current",
        {"version": 1, "mode": "v2_open", "next_public_order": None},
    )

    with pytest.raises(ReadyBundleError, match="publication order"):
        publication_contract_state(database)
