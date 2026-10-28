import json
from pathlib import Path

import pytest
from scripts.check_clones import contract, validate_acceptances
from scripts.clone_tools import CloneFinding


def _ledger(path: Path, records: list[dict[str, object]]) -> Path:
    path.write_text(json.dumps({"schema_version": 1, "contract": contract(), "accepted": records}), encoding="utf-8")
    return path


def _finding(identity: str = "a" * 64) -> CloneFinding:
    return CloneFinding("pyscn", identity, ("one.py:1-8", "two.py:1-8"))


def _acceptance(reason: str = "Independent validation contracts") -> dict[str, object]:
    return {"detector": "pyscn", "identity": "a" * 64, "count": 1, "reason": reason}


def test_new_clone_is_rejected(tmp_path: Path) -> None:
    path = _ledger(tmp_path / "ledger.json", [])
    assert "unreviewed" in validate_acceptances(path, [_finding()])[0]


def test_reviewed_pair_passes_and_third_copy_fails(tmp_path: Path) -> None:
    path = _ledger(tmp_path / "ledger.json", [_acceptance()])
    assert validate_acceptances(path, [_finding()]) == []
    assert "unreviewed" in validate_acceptances(path, [_finding(), _finding()])[0]


def test_removed_or_changed_pair_revokes_acceptance(tmp_path: Path) -> None:
    path = _ledger(tmp_path / "ledger.json", [_acceptance()])
    assert "stale" in validate_acceptances(path, [])[0]
    assert len(validate_acceptances(path, [_finding("b" * 64)])) == 2


def test_blank_reason_or_duplicate_acceptance_is_rejected(tmp_path: Path) -> None:
    path = _ledger(tmp_path / "ledger.json", [_acceptance(" ")])
    with pytest.raises(ValueError, match="reason"):
        validate_acceptances(path, [_finding()])
    _ledger(path, [_acceptance(), _acceptance()])
    with pytest.raises(ValueError, match="duplicate"):
        validate_acceptances(path, [_finding()])


def test_policy_change_requires_explicit_migration(tmp_path: Path) -> None:
    path = _ledger(tmp_path / "ledger.json", [])
    changed = contract()
    changed["roots"] = ["tests"]
    path.write_text(json.dumps({"schema_version": 1, "contract": changed, "accepted": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="policy changed"):
        validate_acceptances(path, [])
