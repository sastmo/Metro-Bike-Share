from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import validate_repo


class RepositoryContractTests(unittest.TestCase):
    def test_repository_root_has_no_sql_files(self) -> None:
        self.assertEqual(validate_repo.root_sql_files(), [])

    def test_manifest_declares_existing_paths(self) -> None:
        manifest = validate_repo.load_manifest()
        self.assertEqual(validate_repo.validate_manifest(manifest), [])

    def test_no_non_legacy_absolute_paths_remain(self) -> None:
        self.assertEqual(validate_repo.disallowed_absolute_paths(), [])

    def test_required_directories_exist(self) -> None:
        missing = [
            path for path in validate_repo.REQUIRED_DIRECTORIES
            if not (validate_repo.ROOT / path).is_dir()
        ]
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
