from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any


def write_and_copy_dag(
    filename: str,
    content: str,
    generated_dags_dir: Path,
    airflow_dags_dir: Path,
) -> dict[str, Any]:
    generated_dags_dir.mkdir(parents=True, exist_ok=True)
    local_path = generated_dags_dir / filename
    local_path.write_text(content, encoding="utf-8")

    result: dict[str, Any] = {
        "filename": filename,
        "local_path": str(local_path),
        "airflow_path": str(airflow_dags_dir / filename),
        "airflow_copy_success": False,
        "warnings": [],
    }

    try:
        # Create target folder when possible instead of failing early.
        airflow_dags_dir.mkdir(parents=True, exist_ok=True)
        destination = airflow_dags_dir / filename
        shutil.copy2(local_path, destination)
        result["airflow_copy_success"] = True
    except PermissionError as exc:
        result["warnings"].append(
            "Permission denied while copying to Airflow DAG folder: "
            f"{exc}. Ensure write access to '{airflow_dags_dir}' or set AIRFLOW_DAGS_DIR to a writable folder."
        )
    except OSError as exc:
        result["warnings"].append(f"Failed to copy DAG to Airflow folder: {exc}")

    return result
