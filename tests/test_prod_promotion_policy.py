from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def _version_payload(
    pipeline_id: str,
    version_id: str,
    promotion_status: str,
    score: float | None,
    critical_failures: list[str] | None = None,
) -> dict:
    return {
        "pipeline_id": pipeline_id,
        "version_id": version_id,
        "base_pipeline_id": pipeline_id,
        "local_dag_id": f"{pipeline_id}__{version_id}",
        "git_dag_id": f"{pipeline_id}_git__{version_id}",
        "local_dag_file": f"airflow/dags/{pipeline_id}__{version_id}.py",
        "git_dag_file": f"dags/{pipeline_id}_git__{version_id}.py",
        "promotion_status": promotion_status,
        "publish_status": "merged_to_git",
        "score": score,
        "score_type": "runtime_once",
        "critical_failures": critical_failures or [],
        "warnings": [],
        "updated_at": "2026-01-01T00:00:00+00:00",
    }


class ProdPromotionPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="prod-promotion-policy-"))
        self.eval_root = self.tmp / "eval-src"
        self.prod_root = self.tmp / "prod-worktree"
        self.eval_root.mkdir(parents=True, exist_ok=True)
        self.prod_root.mkdir(parents=True, exist_ok=True)
        repo_root = Path(__file__).resolve().parents[1]
        shutil.copytree(repo_root / "scripts", self.eval_root / "scripts")
        (self.eval_root / "dags").mkdir(parents=True, exist_ok=True)
        (self.prod_root / "dags").mkdir(parents=True, exist_ok=True)
        self.check_script = self.eval_root / "scripts" / "promotion" / "check_promotion_eligibility.py"
        self.promote_script = self.eval_root / "scripts" / "promotion" / "promote_changed_challengers.py"
        self.repair_script = self.eval_root / "scripts" / "promotion" / "repair_prod_champion.py"

    def _write_metadata(self, root: Path, payload: dict) -> Path:
        path = (
            root
            / "airflow"
            / "web_app_data"
            / "metadata"
            / str(payload["pipeline_id"])
            / f"{payload['version_id']}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def _write_dag_for(self, root: Path, payload: dict) -> None:
        dag_rel = str(payload["git_dag_file"])
        dag_file = root / dag_rel
        dag_file.parent.mkdir(parents=True, exist_ok=True)
        dag_file.write_text("from airflow import DAG\n", encoding="utf-8")

    def test_check_eligibility_uses_prod_champion_threshold(self):
        candidate = _version_payload("testpip", "v8", "challenger", 79.7)
        champion = _version_payload("testpip", "v6", "champion", 96.5)
        self._write_metadata(self.eval_root, candidate)
        self._write_metadata(self.prod_root, champion)
        decision_file = self.tmp / "eligibility-v8.json"
        cmd = [
            sys.executable,
            str(self.check_script),
            "--candidate-root",
            str(self.eval_root),
            "--prod-root",
            str(self.prod_root),
            "--pipeline-id",
            "testpip",
            "--version-id",
            "v8",
            "--promotion-threshold",
            "0.05",
            "--decision-out",
            str(decision_file),
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1, msg=proc.stdout + proc.stderr)
        decision = json.loads(decision_file.read_text(encoding="utf-8"))
        self.assertFalse(decision["should_promote"])
        self.assertEqual(decision["reason"], "score_below_prod_champion_threshold")
        self.assertEqual(decision["candidate_version"], "v8")
        self.assertEqual(decision["candidate_score"], 79.7)
        self.assertEqual(decision["prod_champion_version"], "v6")
        self.assertEqual(decision["prod_champion_score"], 96.5)
        self.assertEqual(decision["threshold"], 0.05)
        self.assertAlmostEqual(float(decision["required_score"]), 101.325, places=3)

    def test_promote_blocks_lower_score_and_keeps_existing_champion(self):
        candidate = _version_payload("testpip", "v8", "challenger", 79.7)
        champion = _version_payload("testpip", "v6", "champion", 96.5)
        cand_path = self._write_metadata(self.eval_root, candidate)
        self._write_metadata(self.prod_root, champion)
        self._write_dag_for(self.eval_root, candidate)
        self._write_dag_for(self.prod_root, champion)
        cmd = [
            sys.executable,
            str(self.promote_script),
            "--eval-root",
            str(self.eval_root),
            "--prod-root",
            str(self.prod_root),
            "--metadata-file",
            str(cand_path.relative_to(self.eval_root)).replace("\\", "/"),
            "--decision-file",
            "evaluation-results/promotion-decisions.json",
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

        champion_after = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v6.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(champion_after["promotion_status"], "champion")
        self.assertFalse(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v8.json").exists()
        )
        decisions = json.loads(
            (self.eval_root / "evaluation-results" / "promotion-decisions.json").read_text(encoding="utf-8")
        ).get("decisions", [])
        self.assertEqual(len(decisions), 1)
        d = decisions[0]
        self.assertFalse(d["should_promote"])
        self.assertFalse(d["eligible"])
        self.assertFalse(d["promoted"])
        self.assertEqual(d["reason"], "score_below_prod_champion_threshold")
        self.assertEqual(d["prod_champion_version"], "v6")

    def test_promotes_when_challenger_beats_prod_threshold(self):
        candidate = _version_payload("testpip", "v7", "challenger", 102.0)
        champion = _version_payload("testpip", "v6", "champion", 96.5)
        cand_path = self._write_metadata(self.eval_root, candidate)
        self._write_metadata(self.prod_root, champion)
        self._write_dag_for(self.eval_root, candidate)
        self._write_dag_for(self.prod_root, champion)
        cmd = [
            sys.executable,
            str(self.promote_script),
            "--eval-root",
            str(self.eval_root),
            "--prod-root",
            str(self.prod_root),
            "--metadata-file",
            str(cand_path.relative_to(self.eval_root)).replace("\\", "/"),
            "--decision-file",
            "evaluation-results/promotion-decisions.json",
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

        v6 = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v6.json").read_text(
                encoding="utf-8"
            )
        )
        v7 = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v7.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(v6["promotion_status"], "archived")
        self.assertEqual(v7["promotion_status"], "champion")
        decisions = json.loads(
            (self.eval_root / "evaluation-results" / "promotion-decisions.json").read_text(encoding="utf-8")
        ).get("decisions", [])
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0]["should_promote"])
        self.assertTrue(decisions[0]["promoted"])

    def test_no_prod_champion_promotes_first_eligible(self):
        candidate = _version_payload("testpip", "v1", "challenger", 80.0)
        cand_path = self._write_metadata(self.eval_root, candidate)
        self._write_dag_for(self.eval_root, candidate)
        cmd = [
            sys.executable,
            str(self.promote_script),
            "--eval-root",
            str(self.eval_root),
            "--prod-root",
            str(self.prod_root),
            "--metadata-file",
            str(cand_path.relative_to(self.eval_root)).replace("\\", "/"),
            "--decision-file",
            "evaluation-results/promotion-decisions.json",
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        promoted = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v1.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(promoted["promotion_status"], "champion")
        decisions = json.loads(
            (self.eval_root / "evaluation-results" / "promotion-decisions.json").read_text(encoding="utf-8")
        ).get("decisions", [])
        self.assertEqual(decisions[0]["reason"], "no_current_prod_champion")

    def test_repair_restores_best_valid_champion(self):
        v6 = _version_payload("testpip", "v6", "champion", 96.5)
        v7 = _version_payload("testpip", "v7", "champion", 95.7)
        v8 = _version_payload("testpip", "v8", "champion", 79.7)
        self._write_metadata(self.prod_root, v6)
        self._write_metadata(self.prod_root, v7)
        self._write_metadata(self.prod_root, v8)
        cmd = [
            sys.executable,
            str(self.repair_script),
            "--root",
            str(self.prod_root),
            "--rollback-retention-enabled",
            "true",
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        p6 = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v6.json").read_text(
                encoding="utf-8"
            )
        )
        p7 = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v7.json").read_text(
                encoding="utf-8"
            )
        )
        p8 = json.loads(
            (self.prod_root / "airflow" / "web_app_data" / "metadata" / "testpip" / "v8.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(p6["promotion_status"], "champion")
        self.assertEqual(p7["promotion_status"], "archived")
        self.assertEqual(p8["promotion_status"], "archived")


if __name__ == "__main__":
    unittest.main()
