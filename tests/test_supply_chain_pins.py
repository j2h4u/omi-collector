from pathlib import Path

from scripts.check_supply_chain_pins import _check_action_refs, _check_container_refs


def test_remote_actions_require_full_commit_shas(tmp_path: Path) -> None:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "steps:\n  - uses: actions/checkout@v7.0.1\n",
        encoding="utf-8",
    )

    errors = _check_action_refs(tmp_path)

    assert errors == [".github/workflows/ci.yml uses actions/checkout@v7.0.1; pin actions to a full 40-character SHA"]


def test_digest_pinned_container_images_pass(tmp_path: Path) -> None:
    digest = "a" * 64
    (tmp_path / "Dockerfile").write_text(
        f"FROM python:3.14-slim@sha256:{digest}\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == []


def test_container_tags_and_missing_digests_fail(tmp_path: Path) -> None:
    (tmp_path / "Dockerfile").write_text(
        "FROM python:3.14-slim\n",
        encoding="utf-8",
    )

    assert _check_container_refs(tmp_path) == [
        "Dockerfile uses python:3.14-slim; pin container images to a sha256 digest"
    ]
