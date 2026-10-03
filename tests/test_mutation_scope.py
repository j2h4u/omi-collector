from __future__ import annotations

from unittest.mock import Mock

import pytest
from scripts import mutation_scope


def test_start_rejects_existing_scope_before_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(mutation_scope, "_managed_units", Mock(return_value=["omi-mutation-audit.scope"]))
    launch = Mock()
    monkeypatch.setattr(mutation_scope.subprocess, "run", launch)

    assert mutation_scope.main(["start"]) == 1
    launch.assert_not_called()


def test_managed_units_select_active_scopes_and_ignore_foreign_names(monkeypatch: pytest.MonkeyPatch) -> None:
    listing = """omi-mutation-audit.scope loaded active running mutation audit
omi-mutation-pause-old.scope loaded active running paused audit
omi-mutation-extra.scope loaded active running unrelated scope
omi-mutation-pause-stopped.scope loaded inactive dead old scope"""
    monkeypatch.setattr(mutation_scope, "_run", Mock(return_value=listing))

    assert mutation_scope._managed_units("tester@.host") == [
        "omi-mutation-audit.scope",
        "omi-mutation-pause-old.scope",
    ]


def test_multiple_managed_units_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(
        mutation_scope,
        "_managed_units",
        Mock(return_value=["omi-mutation-audit.scope", "omi-mutation-pause-old.scope"]),
    )
    control = Mock()
    monkeypatch.setattr(mutation_scope, "_run", control)

    assert mutation_scope.main(["pause"]) == 1
    control.assert_not_called()


def test_pause_targets_the_only_managed_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(mutation_scope, "_managed_units", Mock(return_value=["omi-mutation-pause-old.scope"]))
    calls: list[list[str]] = []

    def run(args: list[str]) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(mutation_scope, "_run", run)

    assert mutation_scope.main(["pause"]) == 0
    assert calls == [["systemctl", "--user", "--machine=tester@.host", "freeze", "omi-mutation-pause-old.scope"]]


def test_status_reports_freezer_state(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(mutation_scope, "_managed_units", Mock(return_value=["omi-mutation-audit.scope"]))
    monkeypatch.setattr(mutation_scope, "_run", Mock(side_effect=["active", "frozen"]))

    assert mutation_scope.main(["status"]) == 0
    assert "active=active, freezer=frozen" in capsys.readouterr().out
