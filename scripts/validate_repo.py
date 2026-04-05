from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "sql/warehouse/contracts/source_manifest.json"

REQUIRED_DIRECTORIES = [
    "data/raw/trips",
    "data/raw/stations",
    "data/reference/census",
    "data/reference/geography",
    "data/reference/transport",
    "docs",
    "scripts",
    "sql/legacy/foundation",
    "sql/legacy/staging",
    "sql/legacy/features",
    "sql/legacy/marts",
    "sql/legacy/enrichment",
    "sql/warehouse/contracts",
    "sql/warehouse/orchestration",
    "sql/warehouse/utilities",
    "tests",
]

KEY_FILES = [
    "README.md",
    "data/README.md",
    "docs/architecture.md",
    "docs/legacy-inventory.md",
    "sql/legacy/README.md",
    "sql/warehouse/README.md",
    "sql/warehouse/orchestration/pipeline_runbook.md",
    "sql/warehouse/utilities/points.sql",
    "sql/warehouse/contracts/source_manifest.json",
    "scripts/validate_repo.py",
    "tests/test_repository_contract.py",
    "Makefile",
    ".gitignore",
]

BANNED_ABSOLUTE_PATH_FRAGMENT = "/Users/admin/OneDrive/Desktop"


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text())


def root_sql_files() -> list[str]:
    return sorted(path.name for path in ROOT.glob("*.sql"))


def disallowed_absolute_paths() -> list[str]:
    hits: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if path == Path(__file__).resolve():
            continue
        if "sql/legacy" in path.as_posix():
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        if BANNED_ABSOLUTE_PATH_FRAGMENT in text:
            hits.append(str(path.relative_to(ROOT)))
    return sorted(hits)


def validate_manifest(manifest: dict) -> list[str]:
    errors: list[str] = []

    for key in ("project_name", "version", "focus", "domains", "warehouse_assets"):
        if key not in manifest:
            errors.append(f"Manifest is missing required key: {key}")

    for domain in manifest.get("domains", []):
        for key in ("name", "layer", "path", "pattern", "description"):
            if key not in domain:
                errors.append(f"Domain entry is missing required key: {key}")
        domain_path = domain.get("path")
        if domain_path and not (ROOT / domain_path).exists():
            errors.append(f"Manifest path does not exist: {domain_path}")

    for asset in manifest.get("warehouse_assets", []):
        if "path" not in asset:
            errors.append("Warehouse asset is missing required key: path")
            continue
        if not (ROOT / asset["path"]).exists():
            errors.append(f"Warehouse asset path does not exist: {asset['path']}")

    return errors


def validate() -> list[str]:
    errors: list[str] = []

    for directory in REQUIRED_DIRECTORIES:
        if not (ROOT / directory).is_dir():
            errors.append(f"Missing directory: {directory}")

    for file_path in KEY_FILES:
        if not (ROOT / file_path).is_file():
            errors.append(f"Missing file: {file_path}")

    if root_sql_files():
        errors.append(f"SQL files should not live at repository root: {', '.join(root_sql_files())}")

    if MANIFEST_PATH.exists():
        errors.extend(validate_manifest(load_manifest()))

    banned_hits = disallowed_absolute_paths()
    if banned_hits:
        errors.append(
            "Non-legacy files still reference the old personal absolute path: "
            + ", ".join(banned_hits)
        )

    return errors


def main() -> int:
    errors = validate()
    if errors:
        print("Repository validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Repository structure and contracts look good.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
