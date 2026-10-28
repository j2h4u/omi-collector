from pathlib import Path

import pytest
import scripts.check_supply_chain_pins as supply_chain_pins
from scripts.check_supply_chain_pins import _check_action_refs, _check_container_refs, _check_locked_python_sources

FORK_SHA = "d5c4d18d9b7b08d588e42e9ea92a9773f182df60"
FORK_URL = f"https://github.com/j2h4u/pytest-gremlins?rev={FORK_SHA}#{FORK_SHA}"


def _write_python_source_fixture(root: Path, extra_package: str = "") -> None:
    root.joinpath("pyproject.toml").write_text(
        '[project]\nname = "fixture_project"\nversion = "9.8.7"\ndependencies = []\n',
        encoding="utf-8",
    )
    root.joinpath("uv.lock").write_text(
        'version = 1\nrevision = 5\nrequires-python = ">=3.14"\n\n'
        '[[package]]\nname = "fixture-project"\nversion = "9.8.7"\n'
        'source = { editable = "." }\n\n'
        '[[package]]\nname = "coverage"\nversion = "7.0.0"\n'
        'source = { registry = "https://pypi.org/simple" }\n\n'
        '[[package]]\nname = "pytest-gremlins"\nversion = "1.9.0"\n'
        f'source = {{ git = "{FORK_URL}" }}\n'
        f"{extra_package}",
        encoding="utf-8",
    )


def test_workflows_require_full_commit_shas(tmp_path: Path) -> None:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "valid.yml").write_text(
        f"steps:\n  - uses: actions/checkout@{'a' * 40}\n",
        encoding="utf-8",
    )
    (workflows / "invalid.yaml").write_text(
        "steps:\n  - uses: actions/setup-python@v5.1.0\n",
        encoding="utf-8",
    )

    assert _check_action_refs(tmp_path) == [
        ".github/workflows/invalid.yaml uses actions/setup-python@v5.1.0; pin actions to a full 40-character SHA"
    ]


def test_remote_actions_require_full_commit_shas(tmp_path: Path) -> None:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "steps:\n  - uses: actions/checkout@v7.0.1\n",
        encoding="utf-8",
    )

    assert _check_action_refs(tmp_path) == [
        ".github/workflows/ci.yml uses actions/checkout@v7.0.1; pin actions to a full 40-character SHA"
    ]


def test_pinned_and_local_container_refs_pass(tmp_path: Path) -> None:
    digest = "b" * 64
    (tmp_path / "Dockerfile").write_text(
        f"FROM python:3.14-slim@sha256:{digest}\n"
        "FROM python:3.14-slim@sha256:" + "c" * 64 + " AS build\n"
        "FROM app:local AS runtime\n"
        "COPY --from=build /app /app\n"
        "COPY --from=0 /bin/tool /bin/tool\n"
        f"COPY --from=ghcr.io/example/tool@sha256:{'d' * 64} /tool /tool\n",
        encoding="utf-8",
    )
    (tmp_path / "docker-compose.yml").write_text(
        f"services:\n  app:\n    image: app:local\n  tool:\n    image: ghcr.io/example/tool@sha256:{'e' * 64}\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == []


def test_digest_pinned_container_images_pass(tmp_path: Path) -> None:
    digest = "a" * 64
    (tmp_path / "Dockerfile").write_text(
        f"FROM python:3.14-slim@sha256:{digest}\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == []


@pytest.mark.parametrize(
    ("image", "expected_errors"),
    [
        (f"python:3.14-slim@sha256:{'a' * 64}", []),
        ("python:3.14-slim", ["Dockerfile uses python:3.14-slim; pin container images to a sha256 digest"]),
    ],
)
def test_platform_from_images_are_checked_and_named_stages_are_recognized(
    tmp_path: Path, image: str, expected_errors: list[str]
) -> None:
    (tmp_path / "Dockerfile").write_text(
        f"FROM --platform=$BUILDPLATFORM {image} AS build\nCOPY --from=build /src /src\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == expected_errors


def test_tagged_external_copy_and_compose_images_fail_with_sources(tmp_path: Path) -> None:
    (tmp_path / "Dockerfile").write_text(
        f"FROM python:3.14-slim@sha256:{'f' * 64}\nCOPY --from=busybox:1.36 /bin/tool /bin/tool\n",
        encoding="utf-8",
    )
    (tmp_path / "docker-compose.yml").write_text(
        "services:\n  app:\n    image: redis:7.4\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == [
        "Dockerfile uses busybox:1.36; pin container images to a sha256 digest",
        "docker-compose.yml uses redis:7.4; pin container images to a sha256 digest",
    ]


def test_container_tags_and_missing_digests_fail(tmp_path: Path) -> None:
    (tmp_path / "Dockerfile").write_text(
        "FROM python:3.14-slim\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == [
        "Dockerfile uses python:3.14-slim; pin container images to a sha256 digest"
    ]


@pytest.mark.parametrize(
    ("action_errors", "container_errors"),
    [([], []), (["workflow pin error"], []), ([], ["container pin error"])],
)
def test_main_reports_combined_check_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    action_errors: list[str],
    container_errors: list[str],
) -> None:
    monkeypatch.setattr(supply_chain_pins, "_check_action_refs", lambda _: action_errors)
    monkeypatch.setattr(supply_chain_pins, "_check_container_refs", lambda _: container_errors)

    errors = [*action_errors, *container_errors]
    assert supply_chain_pins.main() == (1 if errors else 0)
    captured = capsys.readouterr()
    expected_stdout = (
        "Supply-chain pin check failed:\n" + "".join(f"  {error}\n" for error in errors)
        if errors
        else "Supply-chain pin check passed\n"
    )
    assert captured.out == expected_stdout
    assert captured.err == ""


def test_main_accepts_missing_workflows_and_docker_inputs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _write_python_source_fixture(tmp_path)
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    monkeypatch.setattr(supply_chain_pins, "__file__", str(script_dir / "check_supply_chain_pins.py"))

    assert supply_chain_pins.main() == 0
    captured = capsys.readouterr()
    assert captured.out == "Supply-chain pin check passed\n"
    assert captured.err == ""


def test_python_sources_accept_only_reviewed_fork_and_dynamic_project_root(tmp_path: Path) -> None:
    _write_python_source_fixture(tmp_path)

    assert _check_locked_python_sources(tmp_path) == []


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (FORK_URL, FORK_URL.replace("j2h4u/pytest-gremlins", "someone/pytest-gremlins")),
        (FORK_URL, FORK_URL.replace(FORK_SHA, "0" * 40)),
        ('version = "1.9.0"', 'version = "1.9.1"'),
    ],
)
def test_python_sources_reject_changed_fork_identity(tmp_path: Path, old: str, new: str) -> None:
    _write_python_source_fixture(tmp_path)
    lock = tmp_path / "uv.lock"
    lock.write_text(lock.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")

    assert _check_locked_python_sources(tmp_path)


@pytest.mark.parametrize(
    "extra_package",
    [
        '[[package]]\nname = "another-fork"\nversion = "1.0"\nsource = { git = "https://github.com/other/fork?rev=abc#abc" }\n',
        '[[package]]\nname = "local-package"\nversion = "1.0"\nsource = { editable = "../local-package" }\n',
        '[[package]]\nname = "pytest-gremlins"\nversion = "1.9.0"\nsource = { registry = "https://pypi.org/simple" }\n',
    ],
)
def test_python_sources_reject_additional_or_unreviewed_package_source(tmp_path: Path, extra_package: str) -> None:
    _write_python_source_fixture(tmp_path, extra_package)

    assert _check_locked_python_sources(tmp_path)


def test_python_sources_reject_missing_reviewed_fork(tmp_path: Path) -> None:
    _write_python_source_fixture(tmp_path)
    lock = tmp_path / "uv.lock"
    text = lock.read_text(encoding="utf-8")
    lock.write_text(text[: text.index('[[package]]\nname = "pytest-gremlins"')], encoding="utf-8")

    assert any("exactly the reviewed fork tuple" in error for error in _check_locked_python_sources(tmp_path))
