from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import cast

WORKFLOW_USES_PATTERN = re.compile(r"^\s*(?:-\s*)?uses:\s*([^@\s]+)@([^\s#]+)", re.MULTILINE)
FROM_PATTERN = re.compile(
    r"^\s*FROM\s+(?:--platform=[^\s]+\s+)?(?P<image>[^\s]+)(?:\s+AS\s+(?P<stage>[^\s]+))?",
    re.IGNORECASE | re.MULTILINE,
)
COPY_FROM_PATTERN = re.compile(r"^\s*COPY\s+--from=(?P<image>[^\s]+)", re.MULTILINE)
IMAGE_PATTERN = re.compile(r"^\s*image:\s*(?P<image>[^\s#]+)", re.MULTILINE)
PYPI_REGISTRY = "https://pypi.org/simple"
REVIEWED_FORK = {
    "name": "pytest-gremlins",
    "version": "1.9.0",
    "url": (
        "https://github.com/j2h4u/pytest-gremlins?rev="
        "d5c4d18d9b7b08d588e42e9ea92a9773f182df60#"
        "d5c4d18d9b7b08d588e42e9ea92a9773f182df60"
    ),
}


def _workflow_files(root: Path) -> list[Path]:
    workflows = root / ".github" / "workflows"
    if not workflows.exists():
        return []
    return sorted([*workflows.glob("*.yml"), *workflows.glob("*.yaml")])


def _check_action_refs(root: Path) -> list[str]:
    errors: list[str] = []
    for path in _workflow_files(root):
        text = path.read_text(encoding="utf-8")
        for match in WORKFLOW_USES_PATTERN.finditer(text):
            action, ref = match.groups()
            if not re.fullmatch(r"[0-9a-f]{40}", ref):
                errors.append(f"{path.relative_to(root)} uses {action}@{ref}; pin actions to a full 40-character SHA")
    return errors


def _is_local_image(image: str) -> bool:
    return "/" not in image and image.endswith(":local")


def _check_image_ref(image: str, source: str) -> list[str]:
    if _is_local_image(image):
        return []
    if not re.search(r"@sha256:[0-9a-f]{64}$", image):
        return [f"{source} uses {image}; pin container images to a sha256 digest"]
    return []


def _check_container_refs(root: Path) -> list[str]:
    errors: list[str] = []
    for dockerfile in sorted(root.glob("Dockerfile*")):
        if not dockerfile.is_file():
            continue
        text = dockerfile.read_text(encoding="utf-8")
        for match in FROM_PATTERN.finditer(text):
            image = match.group("image")
            errors.extend(_check_image_ref(image, str(dockerfile.relative_to(root))))
        stage_names = {match.group("stage") for match in FROM_PATTERN.finditer(text) if match.group("stage")}
        for match in COPY_FROM_PATTERN.finditer(text):
            image = match.group("image")
            if image in stage_names or image.isdigit():
                continue
            errors.extend(_check_image_ref(image, str(dockerfile.relative_to(root))))

    compose = root / "docker-compose.yml"
    if compose.exists():
        text = compose.read_text(encoding="utf-8")
        for match in IMAGE_PATTERN.finditer(text):
            errors.extend(_check_image_ref(match.group("image").strip('"').strip("'"), str(compose.relative_to(root))))
    return errors


def _canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _package_source_kind(
    name: str, version: str, source: dict[str, object], project_name: str, project_version: str
) -> tuple[str, str]:
    if _canonical_name(name) == REVIEWED_FORK["name"]:
        if version == REVIEWED_FORK["version"] and source == {"git": REVIEWED_FORK["url"]}:
            return "fork", REVIEWED_FORK["url"]
        return "unreviewed", ""
    if source == {"registry": PYPI_REGISTRY}:
        return "registry", ""
    if (
        source == {"editable": "."}
        and _canonical_name(name) == _canonical_name(project_name)
        and version == project_version
    ):
        return "project", ""
    return "unreviewed", ""


def _check_locked_packages(lock: dict[str, object], project_name: str, project_version: str) -> list[str]:
    packages = lock.get("package")
    if not isinstance(packages, list):
        return ["uv.lock is missing its package list"]
    errors: list[str] = []
    found_forks: list[tuple[str, str, str]] = []
    found_project_root = 0
    for package in packages:
        if not isinstance(package, dict):
            errors.append("uv.lock contains a malformed package entry")
            continue
        name, version, source = package.get("name"), package.get("version"), package.get("source")
        if not isinstance(name, str) or not isinstance(version, str) or not isinstance(source, dict):
            errors.append("uv.lock package entries require name, version, and source")
            continue
        kind, url = _package_source_kind(name, version, cast(dict[str, object], source), project_name, project_version)
        if kind == "registry":
            continue
        if kind == "fork":
            found_forks.append((name, version, url))
            continue
        if kind == "project":
            found_project_root += 1
            continue
        errors.append(f"uv.lock contains an unreviewed Python package source: {name} {version} {source}")

    expected_fork = (REVIEWED_FORK["name"], REVIEWED_FORK["version"], REVIEWED_FORK["url"])
    if found_forks != [expected_fork]:
        errors.append(f"uv.lock Git sources must contain exactly the reviewed fork tuple: {expected_fork}")
    if found_project_root != 1:
        errors.append("uv.lock must contain exactly one editable project root sourced from '.'")
    return errors


def _check_locked_python_sources(root: Path) -> list[str]:
    """Allow registry packages and the one reviewed source-pinned dev fork."""
    try:
        project = cast(dict[str, object], tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8")))
        lock = cast(dict[str, object], tomllib.loads((root / "uv.lock").read_text(encoding="utf-8")))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return [f"cannot read Python dependency source metadata: {exc}"]

    project_data = project.get("project")
    if not isinstance(project_data, dict):
        return ["pyproject.toml is missing project metadata"]
    project_name, project_version = project_data.get("name"), project_data.get("version")
    if not isinstance(project_name, str) or not isinstance(project_version, str):
        return ["pyproject.toml project name and version must be strings"]
    return _check_locked_packages(lock, project_name, project_version)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    errors = [*_check_action_refs(root), *_check_container_refs(root), *_check_locked_python_sources(root)]
    if errors:
        print("Supply-chain pin check failed:")
        for error in errors:
            print(f"  {error}")
        return 1

    print("Supply-chain pin check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
