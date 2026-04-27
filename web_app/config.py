from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_path(name: str, default: Path | str) -> Path:
    default_path = default if isinstance(default, Path) else Path(default)
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default_path
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return BASE_DIR / candidate


class AppConfig:
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"
    ADMIN_USERNAME = os.getenv("APP_ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD = os.getenv("APP_ADMIN_PASSWORD", "admin123")
    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "").strip()
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "").strip()
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "").strip()
    AZURE_REDIRECT_URI = os.getenv("AZURE_REDIRECT_URI", "http://localhost:8000/auth/callback").strip()

    AIRFLOW_DAGS_DIR = _env_path("AIRFLOW_DAGS_DIR", r"D:\airflow\dags")
    AIRFLOW_API_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "http://localhost:8080").rstrip("/")
    AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_API_USERNAME", "airflow")
    AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "airflow")
    AIRFLOW_API_VERIFY_TLS = _env_bool("AIRFLOW_API_VERIFY_TLS", True)
    AIRFLOW_API_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_API_TIMEOUT_SECONDS", "30"))
    DAG_READY_POLL_TIMEOUT_SECONDS = int(os.getenv("DAG_READY_POLL_TIMEOUT_SECONDS", "600"))
    DAG_READY_POLL_INTERVAL_SECONDS = int(os.getenv("DAG_READY_POLL_INTERVAL_SECONDS", "5"))
    DAG_RUN_POLL_TIMEOUT_SECONDS = int(os.getenv("DAG_RUN_POLL_TIMEOUT_SECONDS", "1800"))
    DAG_RUN_POLL_INTERVAL_SECONDS = int(os.getenv("DAG_RUN_POLL_INTERVAL_SECONDS", "5"))
    AIRFLOW_DOCKER_SYNC_ENABLED = _env_bool("AIRFLOW_DOCKER_SYNC_ENABLED", True)
    AIRFLOW_DOCKER_SYNC_CONTAINERS = [
        item.strip()
        for item in str(
            os.getenv(
                "AIRFLOW_DOCKER_SYNC_CONTAINERS",
                "airflow-airflow-scheduler-1,airflow-airflow-worker-1",
            )
        ).split(",")
        if item.strip()
    ]
    AIRFLOW_DOCKER_SYNC_BASE_DIR = str(
        os.getenv("AIRFLOW_DOCKER_SYNC_BASE_DIR", "/opt/airflow/local_uploads")
    ).strip() or "/opt/airflow/local_uploads"

    GENERATED_DAGS_DIR = _env_path("GENERATED_DAGS_DIR", BASE_DIR / "generated_dags")
    SAVED_PIPELINES_DIR = _env_path("SAVED_PIPELINES_DIR", BASE_DIR / "saved_pipelines")
    DAG_TEMPLATES_DIR = BASE_DIR / "dag_templates"
    LOCAL_UPLOAD_STAGING_DIR = _env_path("LOCAL_UPLOAD_STAGING_DIR", BASE_DIR / "tmp_uploads")
    USERS_STORE_FILE = _env_path("USERS_STORE_FILE", BASE_DIR / "users_store.json")
    REQUESTS_STORE_FILE = _env_path("REQUESTS_STORE_FILE", BASE_DIR / "requests_store.json")
    PIPELINE_VERSIONS_STORE_FILE = _env_path(
        "PIPELINE_VERSIONS_STORE_FILE",
        BASE_DIR / "pipeline_versions_store.json",
    )
    GCP_CONNECTIONS_STORE_FILE = _env_path(
        "GCP_CONNECTIONS_STORE_FILE",
        BASE_DIR / "gcp_connections_store.json",
    )
    GCP_SERVICE_ACCOUNT_HOST_FILE = _env_path("GCP_SERVICE_ACCOUNT_HOST_FILE", r"D:\airflow\credgcp.json")
    GCP_SERVICE_ACCOUNT_CONTAINER_FILE = str(
        os.getenv("GCP_SERVICE_ACCOUNT_CONTAINER_FILE", "/opt/airflow/credgcp.json")
    ).strip() or "/opt/airflow/credgcp.json"
    GCP_SERVICE_ACCOUNT_DOCKER_CONTAINERS = [
        item.strip()
        for item in str(
            os.getenv(
                "GCP_SERVICE_ACCOUNT_DOCKER_CONTAINERS",
                "airflow-airflow-worker-1,airflow-airflow-scheduler-1",
            )
        ).split(",")
        if item.strip()
    ]
    USERS_FILE = _env_path("USERS_FILE", BASE_DIR / "users.json")
    DAG_PUBLISH_STORE_FILE = _env_path("DAG_PUBLISH_STORE_FILE", BASE_DIR / "dag_publish_store.json")

    GITHUB_APP_ID = os.getenv("GITHUB_APP_ID", "").strip()
    GITHUB_APP_CLIENT_ID = os.getenv("GITHUB_APP_CLIENT_ID", "").strip()
    GITHUB_APP_PRIVATE_KEY = os.getenv("GITHUB_APP_PRIVATE_KEY", "").strip()
    GITHUB_APP_PRIVATE_KEY_PATH = os.getenv("GITHUB_APP_PRIVATE_KEY_PATH", "").strip()
    GITHUB_APP_INSTALLATION_ID = os.getenv("GITHUB_APP_INSTALLATION_ID", "").strip()
    GITHUB_OWNER = os.getenv("GITHUB_OWNER", "").strip()
    GITHUB_REPO = os.getenv("GITHUB_REPO", "").strip()
    GITHUB_BASE_BRANCH = os.getenv("GITHUB_BASE_BRANCH", "main").strip() or "main"
    GITHUB_DAGS_REPO_DIR = os.getenv("GITHUB_DAGS_REPO_DIR", "airflow/dags").strip() or "airflow/dags"
    GITHUB_API_BASE_URL = os.getenv("GITHUB_API_BASE_URL", "https://api.github.com").rstrip("/")
    GITHUB_WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "").strip()
    GITHUB_BRANCH_COLLISION_STRATEGY = (
        os.getenv("GITHUB_BRANCH_COLLISION_STRATEGY", "suffix").strip().lower() or "suffix"
    )


def ensure_app_directories() -> None:
    AppConfig.GENERATED_DAGS_DIR.mkdir(parents=True, exist_ok=True)
    AppConfig.SAVED_PIPELINES_DIR.mkdir(parents=True, exist_ok=True)
    AppConfig.LOCAL_UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    for parent in {
        AppConfig.USERS_STORE_FILE.parent,
        AppConfig.REQUESTS_STORE_FILE.parent,
        AppConfig.PIPELINE_VERSIONS_STORE_FILE.parent,
        AppConfig.GCP_CONNECTIONS_STORE_FILE.parent,
        AppConfig.DAG_PUBLISH_STORE_FILE.parent,
        AppConfig.USERS_FILE.parent,
        AppConfig.GCP_SERVICE_ACCOUNT_HOST_FILE.parent,
    }:
        parent.mkdir(parents=True, exist_ok=True)
    if not AppConfig.PIPELINE_VERSIONS_STORE_FILE.exists():
        AppConfig.PIPELINE_VERSIONS_STORE_FILE.write_text(
            json.dumps({"pipelines": {}}, indent=2),
            encoding="utf-8",
        )
    if not AppConfig.GCP_CONNECTIONS_STORE_FILE.exists():
        AppConfig.GCP_CONNECTIONS_STORE_FILE.write_text(
            json.dumps({"connections": []}, indent=2),
            encoding="utf-8",
        )
    if not AppConfig.DAG_PUBLISH_STORE_FILE.exists():
        AppConfig.DAG_PUBLISH_STORE_FILE.write_text(
            json.dumps({"records": [], "processed_webhook_deliveries": []}, indent=2),
            encoding="utf-8",
        )
