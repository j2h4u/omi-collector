from pathlib import Path

import pytest
import scripts.check_supply_chain_pins as supply_chain_pins
from scripts.check_supply_chain_pins import _check_action_refs, _check_container_refs


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
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    monkeypatch.setattr(supply_chain_pins, "__file__", str(script_dir / "check_supply_chain_pins.py"))

    assert supply_chain_pins.main() == 0
    captured = capsys.readouterr()
    assert captured.out == "Supply-chain pin check passed\n"
    assert captured.err == ""
