from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "evaluation" / "airflow_client.py"
SPEC = importlib.util.spec_from_file_location("airflow_client", MODULE_PATH)
assert SPEC and SPEC.loader
airflow_client = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(airflow_client)


class AirflowClientUrlBuildingTests(unittest.TestCase):
    def test_v2_with_host_root_base(self):
        url = airflow_client.build_dag_endpoint("http://localhost:8080", "x", api_version="v2")
        self.assertEqual(url, "http://localhost:8080/api/v2/dags/x")

    def test_v2_with_api_suffix_base(self):
        url = airflow_client.build_dag_endpoint("http://localhost:8080/api/v2", "x", api_version="v2")
        self.assertEqual(url, "http://localhost:8080/api/v2/dags/x")


if __name__ == "__main__":
    unittest.main()

