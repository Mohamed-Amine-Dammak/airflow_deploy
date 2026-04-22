from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from werkzeug.security import check_password_hash, generate_password_hash


ROLE_DEFINITIONS: dict[str, str] = {
    "admin": "Full access, including user management and all platform actions.",
    "builder": "Can build/configure pipelines and generate DAG files.",
    "operator": "Can run/retry DAGs and inspect run/task logs.",
    "viewer": "Read-only access to dashboards, pipelines, and run status.",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_username(value: str) -> str:
    return str(value or "").strip().lower()


def _read_users_file(users_file: Path) -> dict[str, Any]:
    if not users_file.exists():
        return {"users": []}
    try:
        data = json.loads(users_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"users": []}
    if not isinstance(data, dict):
        return {"users": []}
    users = data.get("users")
    if not isinstance(users, list):
        data["users"] = []
    return data


def _write_users_file(users_file: Path, data: dict[str, Any]) -> None:
    users_file.parent.mkdir(parents=True, exist_ok=True)
    users_file.write_text(json.dumps(data, indent=2), encoding="utf-8")


def ensure_admin_user(users_file: Path, username: str, password: str) -> None:
    data = _read_users_file(users_file)
    users = data.get("users", [])
    admin_username = _sanitize_username(username)
    found = None
    for user in users:
        if _sanitize_username(user.get("username", "")) == admin_username:
            found = user
            break

    if found is None:
        users.append(
            {
                "username": admin_username,
                "password_hash": generate_password_hash(password),
                "roles": ["admin"],
                "is_active": True,
                "created_at": _utc_now_iso(),
                "created_by": "system",
            }
        )
        data["users"] = users
        _write_users_file(users_file, data)
        return

    roles = found.get("roles") if isinstance(found.get("roles"), list) else []
    if "admin" not in roles:
        roles.append("admin")
    found["roles"] = sorted(set(str(role).strip().lower() for role in roles if str(role).strip()))
    if "password_hash" not in found or not str(found.get("password_hash", "")).strip():
        found["password_hash"] = generate_password_hash(password)
    if "is_active" not in found:
        found["is_active"] = True
    data["users"] = users
    _write_users_file(users_file, data)


def verify_login(users_file: Path, username: str, password: str) -> dict[str, Any] | None:
    candidate = _sanitize_username(username)
    if not candidate:
        return None
    data = _read_users_file(users_file)
    for user in data.get("users", []):
        if _sanitize_username(user.get("username", "")) != candidate:
            continue
        if user.get("is_active", True) is False:
            return None
        password_hash = str(user.get("password_hash", "")).strip()
        if not password_hash:
            return None
        if check_password_hash(password_hash, password):
            roles = user.get("roles") if isinstance(user.get("roles"), list) else []
            clean_roles = sorted(
                set(str(role).strip().lower() for role in roles if str(role).strip().lower() in ROLE_DEFINITIONS)
            )
            return {
                "username": candidate,
                "roles": clean_roles or ["viewer"],
            }
        return None
    return None


def list_users(users_file: Path) -> list[dict[str, Any]]:
    data = _read_users_file(users_file)
    rows: list[dict[str, Any]] = []
    for user in data.get("users", []):
        username = _sanitize_username(user.get("username", ""))
        if not username:
            continue
        roles = user.get("roles") if isinstance(user.get("roles"), list) else []
        clean_roles = sorted(
            set(str(role).strip().lower() for role in roles if str(role).strip().lower() in ROLE_DEFINITIONS)
        )
        rows.append(
            {
                "username": username,
                "roles": clean_roles,
                "is_active": bool(user.get("is_active", True)),
                "created_at": str(user.get("created_at", "")).strip(),
                "created_by": str(user.get("created_by", "")).strip(),
            }
        )
    rows.sort(key=lambda item: item["username"])
    return rows


def create_user(
    users_file: Path,
    *,
    username: str,
    password: str,
    roles: list[str],
    created_by: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    clean_username = _sanitize_username(username)
    clean_password = str(password or "")
    if not clean_username:
        errors.append("Username is required.")
    elif not re.fullmatch(r"[a-zA-Z0-9_.-]{3,64}", clean_username):
        errors.append("Username must be 3-64 chars using letters, digits, dot, underscore, or dash.")

    if len(clean_password) < 6:
        errors.append("Password must be at least 6 characters.")

    cleaned_roles = sorted(
        set(str(role).strip().lower() for role in (roles or []) if str(role).strip().lower() in ROLE_DEFINITIONS)
    )
    if not cleaned_roles:
        errors.append("At least one valid role is required.")

    data = _read_users_file(users_file)
    users = data.get("users", [])
    if any(_sanitize_username(item.get("username", "")) == clean_username for item in users):
        errors.append(f"User '{clean_username}' already exists.")

    if errors:
        return None, errors

    created = {
        "username": clean_username,
        "password_hash": generate_password_hash(clean_password),
        "roles": cleaned_roles,
        "is_active": True,
        "created_at": _utc_now_iso(),
        "created_by": str(created_by or "").strip() or "admin",
    }
    users.append(created)
    data["users"] = users
    _write_users_file(users_file, data)

    return {
        "username": created["username"],
        "roles": created["roles"],
        "is_active": created["is_active"],
        "created_at": created["created_at"],
        "created_by": created["created_by"],
    }, []
