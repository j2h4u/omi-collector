"""Run the Collector's migration and service as the unprivileged QA user."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence

_APP_GID = 1000
_APP_UID = 1000
_APP_USER = "app"
_CONFIG_PATH = "/data/omi/config.toml"


def _drop_privileges() -> None:
    """Switch the process to the Collector account before execing the command."""
    if os.geteuid() != 0:
        raise RuntimeError("QA entrypoint must start as root")
    os.initgroups(_APP_USER, _APP_GID)
    os.setgid(_APP_GID)
    os.setuid(_APP_UID)


def _run(command: Sequence[str], *, healthcheck: bool) -> int:
    if not command:
        command = ("omi-collector", "service", "--config", _CONFIG_PATH)
    if not healthcheck:
        subprocess.run(
            ("omi-collector", "init-state", "--config", _CONFIG_PATH),
            check=True,
        )
    _drop_privileges()
    os.execvp(command[0], list(command))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    healthcheck = arguments[:1] == ["--healthcheck"]
    if healthcheck:
        arguments = ["omi-collector", "health"]
    try:
        return _run(arguments, healthcheck=healthcheck)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"docker QA entrypoint failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
