"""Exercise release selection against a real local Git remote."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_HELPER = Path(__file__).resolve().parents[1] / "scripts/dev-deploy-release.sh"


def _git(repo: Path, *args: str, env: dict[str, str]) -> str:
    result = subprocess.run(("git", "-C", str(repo), *args), env=env, check=True, capture_output=True, text=True)
    return result.stdout.strip()


@pytest.fixture
def release_repo(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    env = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
    remote = tmp_path / "origin.git"
    repo = tmp_path / "checkout"
    subprocess.run(("git", "init", "--bare", str(remote)), env=env, check=True, capture_output=True)
    subprocess.run(("git", "init", "-b", "main", str(repo)), env=env, check=True, capture_output=True)
    _git(repo, "config", "user.name", "Release Test", env=env)
    _git(repo, "config", "user.email", "test@example.invalid", env=env)
    _git(repo, "remote", "add", "origin", str(remote), env=env)
    scripts = repo / "scripts"
    scripts.mkdir()
    shutil.copy2(_HELPER, scripts / _HELPER.name)
    deployer = scripts / "deploy-systemd-service.sh"
    deployer.write_text(
        '#!/usr/bin/env bash\ngit -C "$(dirname "$0")" rev-parse HEAD > "$RELEASE_TEST_RECEIPT"\n',
        encoding="utf-8",
    )
    deployer.chmod(0o755)
    _git(repo, "add", "scripts", env=env)
    _git(repo, "commit", "-m", "candidate", env=env)
    _git(repo, "push", "origin", "main", env=env)
    env["RELEASE_TEST_RECEIPT"] = str(tmp_path / "receipt")
    env["TMPDIR"] = str(tmp_path)
    return repo, remote, env


def _run(repo: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (str(repo / "scripts/dev-deploy-release.sh"), "v1.2.3"),
        cwd=repo,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("annotated", [False, True])
def test_origin_tag_deploys_immutable_commit(
    release_repo: tuple[Path, Path, dict[str, str]],
    annotated: bool,
) -> None:
    repo, _, env = release_repo
    commit = _git(repo, "rev-parse", "HEAD", env=env)
    args = ("-a", "v1.2.3", "-m", "release") if annotated else ("v1.2.3",)
    _git(repo, "tag", *args, env=env)
    _git(repo, "push", "origin", "refs/tags/v1.2.3", env=env)
    result = _run(repo, env)
    assert result.returncode == 0, result.stderr
    assert Path(env["RELEASE_TEST_RECEIPT"]).read_text(encoding="utf-8").strip() == commit


def test_local_only_tag_is_refused(release_repo: tuple[Path, Path, dict[str, str]]) -> None:
    repo, _, env = release_repo
    _git(repo, "tag", "v1.2.3", env=env)
    result = _run(repo, env)
    assert result.returncode != 0
    assert not Path(env["RELEASE_TEST_RECEIPT"]).exists()


def test_distinct_annotated_tags_on_same_commit_are_refused(
    release_repo: tuple[Path, Path, dict[str, str]],
) -> None:
    repo, _, env = release_repo
    _git(repo, "tag", "-a", "v1.2.3", "-m", "remote", env=env)
    _git(repo, "push", "origin", "refs/tags/v1.2.3", env=env)
    _git(repo, "tag", "-f", "-a", "v1.2.3", "-m", "different", env=env)
    result = _run(repo, env)
    assert result.returncode != 0
    assert "conflicts with origin" in result.stderr
    assert not Path(env["RELEASE_TEST_RECEIPT"]).exists()


def test_local_tag_on_different_commit_is_refused(release_repo: tuple[Path, Path, dict[str, str]]) -> None:
    repo, _, env = release_repo
    _git(repo, "tag", "v1.2.3", env=env)
    _git(repo, "push", "origin", "refs/tags/v1.2.3", env=env)
    _git(repo, "commit", "--allow-empty", "-m", "different", env=env)
    _git(repo, "tag", "-f", "v1.2.3", env=env)
    result = _run(repo, env)
    assert result.returncode != 0
    assert "conflicts with origin" in result.stderr
    assert not Path(env["RELEASE_TEST_RECEIPT"]).exists()


def test_origin_tag_outside_main_is_refused(release_repo: tuple[Path, Path, dict[str, str]]) -> None:
    repo, _, env = release_repo
    _git(repo, "commit", "--allow-empty", "-m", "unreleased", env=env)
    _git(repo, "tag", "v1.2.3", env=env)
    _git(repo, "push", "origin", "refs/tags/v1.2.3", env=env)
    result = _run(repo, env)
    assert result.returncode != 0
    assert "not reachable" in result.stderr
    assert not Path(env["RELEASE_TEST_RECEIPT"]).exists()


@pytest.mark.parametrize("race", ["remote", "local"])
def test_tag_changes_cannot_change_selected_commit(
    release_repo: tuple[Path, Path, dict[str, str]],
    tmp_path: Path,
    race: str,
) -> None:
    repo, remote, env = release_repo
    selected = _git(repo, "rev-parse", "HEAD", env=env)
    _git(repo, "tag", "v1.2.3", env=env)
    _git(repo, "commit", "--allow-empty", "-m", "newer", env=env)
    newer = _git(repo, "rev-parse", "HEAD", env=env)
    _git(repo, "push", "origin", "main", "refs/tags/v1.2.3", env=env)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    wrapper = fake_bin / "git"
    real_git = shutil.which("git")
    assert real_git is not None
    wrapper.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$RELEASE_TEST_RACE" == remote && "${3:-}" == fetch && "${6:-}" == refs/tags/v1.2.3 ]]; then\n'
        '  "$RELEASE_TEST_GIT" --git-dir "$RELEASE_TEST_REMOTE" update-ref refs/tags/v1.2.3 "$RELEASE_TEST_NEWER" || exit 1\n'
        "fi\n"
        'if [[ "$RELEASE_TEST_RACE" == local && "${3:-}" == worktree && "${4:-}" == add ]]; then\n'
        '  "$RELEASE_TEST_GIT" -C "$2" update-ref refs/tags/v1.2.3 "$RELEASE_TEST_NEWER" || exit 1\n'
        "fi\n"
        'exec "$RELEASE_TEST_GIT" "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "RELEASE_TEST_GIT": real_git,
            "RELEASE_TEST_REMOTE": str(remote),
            "RELEASE_TEST_NEWER": newer,
            "RELEASE_TEST_RACE": race,
        }
    )
    result = _run(repo, env)
    receipt = Path(env["RELEASE_TEST_RECEIPT"])
    if race == "remote":
        assert result.returncode != 0
        assert "changed during fetch" in result.stderr
        assert not receipt.exists()
    else:
        assert result.returncode == 0, result.stderr
        assert receipt.read_text(encoding="utf-8").strip() == selected
