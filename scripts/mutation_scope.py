from __future__ import annotations

import argparse
import os
import pwd
import shutil
import subprocess
import sys
from collections.abc import Sequence
from typing import cast

CANONICAL_UNIT = "omi-mutation-audit.scope"
LEGACY_PREFIX = "omi-mutation-pause-"
ACTIVE_STATE_INDEX = 2
MIN_UNIT_FIELDS = 3


class ScopeError(RuntimeError):
    pass


def _machine() -> str:
    return f"{pwd.getpwuid(os.getuid()).pw_name}@.host"


def _run(args: Sequence[str]) -> str:
    try:
        result = cast(
            subprocess.CompletedProcess[str], subprocess.run(args, check=True, capture_output=True, text=True)
        )
    except FileNotFoundError as exc:
        raise ScopeError(f"required command is unavailable: {args[0]}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = cast(str | None, exc.stderr)
        stdout = cast(str | None, exc.stdout)
        detail = (stderr or stdout or str(exc)).strip()
        raise ScopeError(detail) from exc
    return result.stdout.strip()


def _managed_units(machine: str) -> list[str]:
    output = _run(
        [
            "systemctl",
            "--user",
            f"--machine={machine}",
            "--no-pager",
            "--plain",
            "--all",
            "--type=scope",
            "--no-legend",
            "list-units",
        ],
    )
    units = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < MIN_UNIT_FIELDS or fields[ACTIVE_STATE_INDEX] not in {
            "active",
            "activating",
            "reloading",
            "deactivating",
        }:
            continue
        unit = fields[0]
        if unit == CANONICAL_UNIT or (unit.startswith(LEGACY_PREFIX) and unit.endswith(".scope")):
            units.append(unit)
    return sorted(set(units))


def _single_managed_unit(machine: str) -> str:
    units = _managed_units(machine)
    if len(units) != 1:
        raise ScopeError("expected exactly one managed mutation scope; found " + (", ".join(units) or "none"))
    return units[0]


def _start(*, fresh: bool) -> int:
    machine = _machine()
    units = _managed_units(machine)
    if units:
        raise ScopeError("a managed mutation scope already exists: " + ", ".join(units))
    for command in ("systemd-run", "chrt", "ionice", "nice", "just"):
        if shutil.which(command) is None:
            raise ScopeError(f"required command is unavailable: {command}")
    command = [
        "systemd-run",
        "--user",
        "--scope",
        "--quiet",
        f"--machine={machine}",
        "--unit=omi-mutation-audit",
        "--",
        "chrt",
        "--idle",
        "0",
        "ionice",
        "-c",
        "3",
        "nice",
        "-n",
        "19",
        "just",
        "mutation",
    ]
    if fresh:
        command.append("fresh")
    try:
        return subprocess.run(command, check=False).returncode
    except FileNotFoundError as exc:
        raise ScopeError(f"required command is unavailable: {command[0]}") from exc


def _control(action: str) -> None:
    machine = _machine()
    unit = _single_managed_unit(machine)
    if action == "status":
        state = _run(
            [
                "systemctl",
                "--user",
                f"--machine={machine}",
                "--no-pager",
                "show",
                "--property=ActiveState",
                "--value",
                unit,
            ]
        )
        freezer = _run(
            [
                "systemctl",
                "--user",
                f"--machine={machine}",
                "--no-pager",
                "show",
                "--property=FreezerState",
                "--value",
                unit,
            ]
        )
        print(f"{unit}: active={state or 'unknown'}, freezer={freezer or 'unknown'}")
        return
    if action == "pause":
        _run(["systemctl", "--user", f"--machine={machine}", "freeze", unit])
    else:
        _run(["systemctl", "--user", f"--machine={machine}", "thaw", unit])
    print(f"{unit}: {action} requested")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Control the dedicated Omi mutation audit scope.")
    parser.add_argument("action", choices=("start", "fresh-start", "pause", "resume", "status"))
    args = parser.parse_args(argv)
    action = cast(str, args.action)
    try:
        if action in {"start", "fresh-start"}:
            return _start(fresh=action == "fresh-start")
        _control({"resume": "resume", "pause": "pause", "status": "status"}[action])
    except ScopeError as exc:
        print(f"mutation scope: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
