from __future__ import annotations

from subprocess import CompletedProcess
from unittest.mock import Mock

import pytest
from scripts import mutation_scope


def test_start_rejects_existing_scope_before_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def run(args: list[str], **_: object) -> CompletedProcess[str]:
        calls.append(args)
        return CompletedProcess(args, 0, "omi-mutation-audit.scope loaded active running audit\n", "")

    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main(["start"]) == 1
    assert len(calls) == 1
    assert calls[0][-1] == "list-units"


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


@pytest.mark.parametrize("action, suffix", [("start", []), ("fresh-start", ["fresh"])])
@pytest.mark.parametrize("status", [0, 17])
def test_start_passes_through_child_status_and_fresh_argument(
    monkeypatch: pytest.MonkeyPatch, action: str, suffix: list[str], status: int
) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "tester@.host")
    monkeypatch.setattr(mutation_scope, "_managed_units", Mock(return_value=[]))
    monkeypatch.setattr(mutation_scope.shutil, "which", lambda _: "/controlled/stub")
    calls: list[list[str]] = []

    def run(args: list[str], *, check: bool) -> CompletedProcess[str]:
        assert check is False
        calls.append(args)
        return CompletedProcess(args, status)

    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main([action]) == status
    assert calls[0][-len(suffix) :] == suffix if suffix else "mutation" in calls[0]


@pytest.mark.parametrize("stderr, stdout", [("  detailed stderr  ", "fallback"), ("", "  fallback stdout  ")])
def test_command_failure_uses_available_diagnostic(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], stderr: str, stdout: str
) -> None:
    def fail(*_: object, **__: object) -> CompletedProcess[str]:
        raise mutation_scope.subprocess.CalledProcessError(1, ["systemctl"], output=stdout, stderr=stderr)

    monkeypatch.setattr(mutation_scope.subprocess, "run", fail)

    assert mutation_scope.main(["start"]) == 1
    assert capsys.readouterr().err.strip().endswith(stderr.strip() or stdout.strip())
