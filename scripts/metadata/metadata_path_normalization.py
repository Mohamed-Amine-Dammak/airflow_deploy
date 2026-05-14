#!/usr/bin/env python3
"""Helpers to normalize metadata file paths from workflow artifacts/inputs."""

from __future__ import annotations

from typing import Iterable


def normalize_metadata_path(line: str) -> str:
    """Normalize one metadata file path line.

    Example:
    "\ufeffairflow/web_app_data/metadata/testpip/v5.json"
    -> "airflow/web_app_data/metadata/testpip/v5.json"
    """
    normalized = str(line or "")
    normalized = normalized.lstrip("\ufeff").strip().strip('"').strip("'")
    normalized = normalized.replace("\r", "")
    normalized = normalized.replace("\\", "/")
    return normalized


def normalize_metadata_paths(lines: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        normalized = normalize_metadata_path(str(line))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out

