from __future__ import annotations

import re
from pathlib import Path

WORKFLOW_USES_PATTERN = re.compile(r"^\s*(?:-\s*)?uses:\s*([^@\s]+)@([^\s#]+)", re.MULTILINE)
FROM_PATTERN = re.compile(r"^\s*FROM\s+(?P<image>[^\s]+)", re.MULTILINE)
STAGE_NAME_PATTERN = re.compile(r"^\s*FROM\s+[^\s]+\s+AS\s+(?P<stage>[^\s]+)", re.IGNORECASE | re.MULTILINE)
COPY_FROM_PATTERN = re.compile(r"^\s*COPY\s+--from=(?P<image>[^\s]+)", re.MULTILINE)
IMAGE_PATTERN = re.compile(r"^\s*image:\s*(?P<image>[^\s#]+)", re.MULTILINE)


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
        stage_names = {match.group("stage") for match in STAGE_NAME_PATTERN.finditer(text)}
        for match in FROM_PATTERN.finditer(text):
            image = match.group("image")
            if image.startswith("--"):
                continue
            errors.extend(_check_image_ref(image, str(dockerfile.relative_to(root))))
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


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    errors = [*_check_action_refs(root), *_check_container_refs(root)]
    if errors:
        print("Supply-chain pin check failed:")
        for error in errors:
            print(f"  {error}")
        return 1

    print("Supply-chain pin check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
