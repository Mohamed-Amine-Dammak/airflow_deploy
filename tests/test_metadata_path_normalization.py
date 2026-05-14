from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "metadata"
    / "metadata_path_normalization.py"
)
SPEC = importlib.util.spec_from_file_location("metadata_path_normalization", MODULE_PATH)
assert SPEC and SPEC.loader
metadata_path_normalization = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(metadata_path_normalization)


class MetadataPathNormalizationTests(unittest.TestCase):
    def test_strips_utf8_bom(self):
        raw = "\ufeffairflow/web_app_data/metadata/testpip/v5.json"
        normalized = metadata_path_normalization.normalize_metadata_path(raw)
        self.assertEqual(
            normalized,
            "airflow/web_app_data/metadata/testpip/v5.json",
        )


if __name__ == "__main__":
    unittest.main()
