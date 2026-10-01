from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import cast


def test_runner_preserves_mutant_outcomes_with_fixture_and_parametrization(tmp_path: Path) -> None:
    source = tmp_path / "src"
    tests = tmp_path / "tests"
    source.mkdir()
    tests.mkdir()
    markers = tmp_path / "canary-markers"
    markers.mkdir()
    (source / "toy.py").write_text(
        "def is_positive(value: int) -> bool:\n    return value > 0\n",
        encoding="utf-8",
    )
    (tests / "test_toy.py").write_text(
        "import os\n"
        "from pathlib import Path\n"
        "import pytest\n"
        "from toy import is_positive\n"
        "\n"
        "@pytest.fixture(params=('canary-a', 'canary-b'))\n"
        "def canary(request):\n"
        "    return request.param\n"
        "\n"
        "@pytest.mark.parametrize('marker', (1, 2))\n"
        "def test_unrelated_canary(canary, marker):\n"
        "    marker_dir = Path(os.environ['CANARY_MARKER_DIR'])\n"
        "    (marker_dir / f'{canary}-{marker}').touch()\n"
        "    assert canary.startswith('canary-')\n"
        "    assert marker in (1, 2)\n",
        encoding="utf-8",
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\n"
        "pythonpath = ['src']\n"
        "\n"
        "[tool.pytest-gremlins]\n"
        "lightweight_runner = false\n"
        "workers = 1\n"
        "report = ['json']\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["COVERAGE_CORE"] = "ctrace"
    env["CANARY_MARKER_DIR"] = str(markers)
    result = subprocess.run(
        [
            "ionice",
            "-c",
            "3",
            "nice",
            "-n",
            "19",
            "timeout",
            "--signal=TERM",
            "--kill-after=5s",
            "60s",
            sys.executable,
            "-m",
            "pytest",
            "--gremlins",
            "--gremlin-no-coverage-filter",
            "--gremlin-targets=src/toy.py",
            "tests",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0, output
    assert {path.name for path in markers.iterdir()} == {
        "canary-a-1",
        "canary-a-2",
        "canary-b-1",
        "canary-b-2",
    }, output

    report_path = tmp_path / "coverage" / "gremlins" / "gremlins.json"
    report = cast(dict[str, object], json.loads(report_path.read_text(encoding="utf-8")))
    summary = cast(dict[str, int], report["summary"])
    assert summary["total"] > 0, output
    assert summary["survived"] == summary["total"], output
    assert summary["zapped"] == 0, output
    assert summary["timeout"] == 0, output
    assert summary["error"] == 0, output
