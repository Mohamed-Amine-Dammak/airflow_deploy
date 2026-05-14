from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class InspectPromotionDecisionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="inspect-promotion-decisions-"))
        self.script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "promotion"
            / "inspect_promotion_decisions.py"
        )

    def test_blocked_decision_is_not_failure_and_exports_counts(self):
        decision_file = self.tmp / "decisions.json"
        expected_out = self.tmp / "expected.txt"
        env_out = self.tmp / "env.txt"
        payload = {
            "decisions": [
                {
                    "metadata_file": "airflow/web_app_data/metadata/testd/v2.json",
                    "candidate_version": "v2",
                    "should_promote": False,
                    "eligible": False,
                    "promoted": False,
                    "reason": "score_below_prod_champion_threshold",
                    "candidate_score": 79.7,
                    "prod_champion_version": "v1",
                    "prod_champion_score": 96.5,
                    "required_score": 101.325,
                }
            ]
        }
        decision_file.write_text(json.dumps(payload), encoding="utf-8")
        cmd = [
            sys.executable,
            str(self.script),
            "--decision-file",
            str(decision_file),
            "--expected-out",
            str(expected_out),
            "--env-out",
            str(env_out),
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

        env_map: dict[str, str] = {}
        for line in env_out.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                env_map[k.strip()] = v.strip()

        self.assertEqual(env_map.get("PROD_EXPECTED_PROMOTION_COUNT"), "0")
        self.assertEqual(env_map.get("PROD_DECISION_COUNT"), "1")
        self.assertEqual(env_map.get("PROD_SHOULD_PROMOTE_COUNT"), "0")
        self.assertEqual(env_map.get("PROD_BLOCKED_COUNT"), "1")


if __name__ == "__main__":
    unittest.main()
