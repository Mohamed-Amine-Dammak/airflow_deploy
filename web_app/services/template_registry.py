from __future__ import annotations

from typing import Any


def get_template_for_node(node: dict[str, Any]) -> str:
    module_type = node["type"]
    config = node.get("config", {})

    if module_type == "dataform":
        tags = config.get("dataform_tags") or []
        if tags:
            return "module_dataform_tags.py.j2"
        return "module_dataform.py.j2"

    if module_type == "azure":
        action = config.get("action", "upload")
        if action == "download":
            return "module_azure_download.py.j2"
        if action == "delete":
            return "module_azure_delete.py.j2"
        return "module_azure_upload.py.j2"

    if module_type == "y2":
        return "module_y2.py.j2"

    direct_map = {
        "n8n": "module_n8n.py.j2",
        "talend": "module_talend.py.j2",
        "cloudrun": "module_cloudrun.py.j2",
        "gcs": "module_gcs.py.j2",
        "powerbi": "module_powerbi.py.j2",
        "azure_blob_sensor": "module_azure_blob_sensor.py.j2",
        "sftp_upload": "module_sftp_upload.py.j2",
        "sftp_sensor": "module_sftp_sensor.py.j2",
        "delay": "module_delay.py.j2",
        "timeout_guard": "module_timeout_guard.py.j2",
        "manual_approval": "module_manual_approval.py.j2",
        "if_else_router": "module_if_else.py.j2",
        "switch_router": "module_switch_router.py.j2",
        "retry_wrapper": "module_retry_wrapper.py.j2",
        "try_catch": "module_try_catch.py.j2",
        "quorum_join": "module_quorum_join.py.j2",
        "parallel_limit": "module_parallel_limit.py.j2",
        "semaphore_lock": "module_semaphore_lock.py.j2",
        "parallel_fork": "module_parallel_fork.py.j2",
        "parallel_join": "module_parallel_join.py.j2",
        "conditional_router": "module_conditional_router.py.j2",
    }

    if module_type not in direct_map:
        raise ValueError(f"No template configured for module type '{module_type}'.")

    return direct_map[module_type]


def get_imports_for_module(module_type: str, template_name: str) -> set[str]:
    imports = {
        "import json",
        "import os",
        "import re",
        "import time",
        "import requests",
        "from datetime import datetime, timedelta, timezone",
        "from airflow.sdk import dag, task",
        "from airflow.exceptions import AirflowException",
        "from airflow.exceptions import AirflowSkipException",
        "from airflow.sdk.bases.hook import BaseHook",
    }

    conditional_imports: dict[str, set[str]] = {
        "module_cloudrun.py.j2": {
            "from google.oauth2 import service_account",
            "from google.auth.transport.requests import Request",
        },
        "module_dataform.py.j2": {
            "from google.oauth2 import service_account",
            "from google.auth.transport.requests import Request",
        },
        "module_dataform_tags.py.j2": {
            "from google.oauth2 import service_account",
            "from google.auth.transport.requests import Request",
        },
        "module_azure_upload.py.j2": {
            "from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator",
        },
        "module_azure_download.py.j2": {
            "from airflow.providers.microsoft.azure.hooks.wasb import WasbHook",
        },
        "module_azure_delete.py.j2": {
            "from airflow.providers.microsoft.azure.hooks.wasb import WasbHook",
        },
        "module_azure_blob_sensor.py.j2": {
            "from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor",
        },
        "module_sftp_upload.py.j2": {
            "from airflow.providers.sftp.hooks.sftp import SFTPHook",
        },
        "module_sftp_sensor.py.j2": {
            "from airflow.providers.sftp.sensors.sftp import SFTPSensor",
            "from airflow.providers.sftp.hooks.sftp import SFTPHook",
        },
        "module_gcs.py.j2": {
            "import tempfile",
            "from google.cloud import storage",
            "from google.oauth2 import service_account",
            "from googleapiclient.discovery import build",
            "from googleapiclient.http import MediaIoBaseDownload",
        },
        "module_parallel_fork.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_parallel_join.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_timeout_guard.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_retry_wrapper.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_parallel_limit.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_semaphore_lock.py.j2": {
            "from airflow.providers.standard.operators.empty import EmptyOperator",
        },
        "module_conditional_router.py.j2": {
            "from airflow.providers.standard.operators.python import BranchPythonOperator",
        },
        "module_if_else.py.j2": {
            "from airflow.providers.standard.operators.python import BranchPythonOperator",
        },
        "module_switch_router.py.j2": {
            "from airflow.providers.standard.operators.python import BranchPythonOperator",
        },
        "module_try_catch.py.j2": {
            "from airflow.providers.standard.operators.python import BranchPythonOperator",
        },
    }

    if module_type == "powerbi":
        imports.add("from urllib.parse import urlencode")

    imports.update(conditional_imports.get(template_name, set()))
    return imports
