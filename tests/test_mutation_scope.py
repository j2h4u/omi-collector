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


def test_public_status_resolves_account_and_targets_its_systemd_manager(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    resolved_uids: list[int] = []

    def get_account(uid: int) -> object:
        resolved_uids.append(uid)
        return type("Account", (), {"pw_name": "operator"})()

    monkeypatch.setattr(mutation_scope.pwd, "getpwuid", get_account)
    monkeypatch.setattr(mutation_scope.os, "getuid", lambda: 4242)
    calls: list[list[str]] = []

    def run(args: list[str], **kwargs: object) -> CompletedProcess[str]:
        assert kwargs == {"check": True, "capture_output": True, "text": True}
        calls.append(args)
        if args[-1] == "list-units":
            stdout = "omi-mutation-audit.scope loaded active running audit\n"
        elif "--property=FreezerState" in args:
            stdout = "frozen\n"
        else:
            stdout = "active\n"
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main(["status"]) == 0

    assert capsys.readouterr().out == "omi-mutation-audit.scope: active=active, freezer=frozen\n"
    assert resolved_uids == [4242]
    assert calls
    assert all("--machine=operator@.host" in args for args in calls)


def test_public_status_requires_checked_captured_text_command_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "operator@.host")
    listing = "omi-mutation-audit.scope loaded active running audit\n"

    def run(args: list[str], **kwargs: object) -> CompletedProcess[str]:
        if args[-1] == "list-units":
            return CompletedProcess(args, 0, listing, "")
        if kwargs.get("check") is not True:
            return CompletedProcess(args, 1, "unchecked failure", "")
        if kwargs.get("capture_output") is not True:
            raise mutation_scope.subprocess.CalledProcessError(1, args)
        diagnostic: str | bytes = "systemctl unavailable"
        if kwargs.get("text") is not True:
            diagnostic = diagnostic.encode()
        raise mutation_scope.subprocess.CalledProcessError(1, args, output="", stderr=diagnostic)

    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main(["status"]) == 1
    assert capsys.readouterr().err == "mutation scope: systemctl unavailable\n"


@pytest.mark.parametrize(
    ("listing", "diagnostic"),
    [
        ("", "found none"),
        (
            (
                "omi-mutation-audit.scope loaded active running audit\n"
                "omi-mutation-pause-old.scope loaded active running paused audit\n"
            ),
            "found omi-mutation-audit.scope, omi-mutation-pause-old.scope",
        ),
    ],
)
def test_public_pause_reports_zero_or_multiple_managed_units(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    listing: str,
    diagnostic: str,
) -> None:
    monkeypatch.setattr(mutation_scope, "_machine", lambda: "operator@.host")

    def run(args: list[str], **kwargs: object) -> CompletedProcess[str]:
        assert args[-1] == "list-units"
        assert kwargs == {"check": True, "capture_output": True, "text": True}
        return CompletedProcess(args, 0, listing, "")

    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main(["pause"]) == 1
    assert capsys.readouterr().err == (
        "mutation scope: expected exactly one managed mutation scope; " + diagnostic + "\n"
    )


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
