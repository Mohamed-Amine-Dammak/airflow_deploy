from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


class EvaluateDagSelfContainedTests(unittest.TestCase):
    def test_no_services_pipeline_versions_import(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "evaluation" / "evaluate_dag.py"
        content = script.read_text(encoding="utf-8")
        self.assertNotIn("services.pipeline_versions", content)
        self.assertNotIn("from services.", content)

    def test_runs_with_minimal_store_and_dag(self):
        root = Path(tempfile.mkdtemp(prefix="eval-self-contained-"))
        store_path = root / "airflow" / "web_app_data" / "pipeline_versions_store.json"
        dag_path = root / "dags" / "testdataops_git__v7.py"
        dag_path.parent.mkdir(parents=True, exist_ok=True)
        dag_path.write_text(
            "from airflow import DAG\n"
            "with DAG(dag_id='testdataops_git__v7', tags=['x'], default_args={'owner':'u','retries':1}) as dag:\n"
            "    pass\n",
            encoding="utf-8",
        )
        store_path.parent.mkdir(parents=True, exist_ok=True)
        store_path.write_text(
            (
                "{\n"
                '  "pipelines": {\n'
                '    "testdataops": {\n'
                '      "versions": [\n'
                "        {\n"
                '          "version_id": "v7",\n'
                '          "local_dag_id": "testdataops__v7",\n'
                '          "local_dag_file": "airflow/dags/testdataops__v7.py",\n'
                '          "git_dag_id": "testdataops_git__v7",\n'
                '          "git_dag_file": "dags/testdataops_git__v7.py",\n'
                '          "dag_id": "testdataops__v7",\n'
                '          "airflow_dag_id": "testdataops__v7",\n'
                '          "promotion_status": "eval"\n'
                "        }\n"
                "      ]\n"
                "    }\n"
                "  }\n"
                "}\n"
            ),
            encoding="utf-8",
        )

        script = Path(__file__).resolve().parents[1] / "scripts" / "evaluation" / "evaluate_dag.py"
        cmd = [
            "python",
            str(script),
            "--root",
            str(root),
            "--pipeline-id",
            "testdataops",
            "--dag-id",
            "testdataops__v7",
            "--evaluation-mode",
            "static_only",
            "--run-count",
            "1",
        ]
        import subprocess

        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    unittest.main()
