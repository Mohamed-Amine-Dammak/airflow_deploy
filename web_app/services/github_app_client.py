from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jwt
import requests
from requests import Response
from requests import Session
from requests.exceptions import RequestException


class GitHubClientError(Exception):
    def __init__(self, message: str, status_code: int | None = None, details: Any | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


@dataclass(frozen=True)
class GitHubAppSettings:
    app_id: str
    private_key: str
    installation_id: str
    owner: str
    repo: str
    base_branch: str
    dags_repo_dir: str
    api_base_url: str
    branch_collision_strategy: str


def load_private_key_from_config(private_key_inline: str, private_key_path: str, base_dir: Path) -> str:
    if private_key_inline.strip():
        return private_key_inline.strip()
    if private_key_path.strip():
        key_path = Path(private_key_path.strip())
        if not key_path.is_absolute():
            key_path = (base_dir / key_path).resolve()
        if key_path.exists():
            return key_path.read_text(encoding="utf-8").strip()
    return ""


class GitHubAppClient:
    def __init__(self, settings: GitHubAppSettings, session: Session | None = None):
        self.settings = settings
        self._session = session or requests.Session()

    def create_app_jwt(self) -> str:
        now = int(time.time())
        payload = {"iat": now - 60, "exp": now + (9 * 60), "iss": self.settings.app_id}
        return str(jwt.encode(payload, self.settings.private_key, algorithm="RS256"))

    def create_installation_token(self) -> str:
        app_jwt = self.create_app_jwt()
        response = self._request(
            "POST",
            f"/app/installations/{self.settings.installation_id}/access_tokens",
            token=app_jwt,
            token_kind="jwt",
            json_payload={},
        )
        token = str((response.get("token") or "")).strip()
        if not token:
            raise GitHubClientError("GitHub installation token is missing in API response.", 502, response)
        return token

    def get_branch_sha(self, *, branch: str, token: str) -> str | None:
        try:
            payload = self._request("GET", f"/repos/{self.settings.owner}/{self.settings.repo}/git/ref/heads/{branch}", token=token)
        except GitHubClientError as exc:
            if exc.status_code == 404:
                return None
            raise
        obj = payload.get("object") if isinstance(payload, dict) else {}
        sha = str((obj or {}).get("sha") or "").strip()
        return sha or None

    def create_branch(self, *, branch: str, source_sha: str, token: str) -> str:
        payload = self._request(
            "POST",
            f"/repos/{self.settings.owner}/{self.settings.repo}/git/refs",
            token=token,
            json_payload={"ref": f"refs/heads/{branch}", "sha": source_sha},
        )
        obj = payload.get("object") if isinstance(payload, dict) else {}
        sha = str((obj or {}).get("sha") or source_sha).strip()
        return sha or source_sha

    def upsert_text_file(
        self,
        *,
        repo_path: str,
        branch: str,
        content_text: str,
        commit_message: str,
        token: str,
    ) -> tuple[str, str]:
        existing = self.get_file(repo_path=repo_path, ref=branch, token=token)
        payload: dict[str, Any] = {
            "message": commit_message,
            "content": base64.b64encode(content_text.encode("utf-8")).decode("ascii"),
            "branch": branch,
        }
        if existing and existing.get("sha"):
            payload["sha"] = existing["sha"]

        response = self._request(
            "PUT",
            f"/repos/{self.settings.owner}/{self.settings.repo}/contents/{repo_path}",
            token=token,
            json_payload=payload,
        )
        content = response.get("content") if isinstance(response, dict) else {}
        commit = response.get("commit") if isinstance(response, dict) else {}
        file_sha = str((content or {}).get("sha") or "").strip()
        commit_sha = str((commit or {}).get("sha") or "").strip()
        if not commit_sha:
            raise GitHubClientError("GitHub response did not include commit SHA.", 502, response)
        return file_sha, commit_sha

    def get_file(self, *, repo_path: str, ref: str, token: str) -> dict[str, Any] | None:
        try:
            payload = self._request(
                "GET",
                f"/repos/{self.settings.owner}/{self.settings.repo}/contents/{repo_path}",
                token=token,
                query={"ref": ref},
            )
        except GitHubClientError as exc:
            if exc.status_code == 404:
                return None
            raise
        if not isinstance(payload, dict):
            return None
        return payload

    def list_open_pull_requests_for_branch(self, *, branch: str, token: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"/repos/{self.settings.owner}/{self.settings.repo}/pulls",
            token=token,
            query={
                "state": "open",
                "head": f"{self.settings.owner}:{branch}",
                "base": self.settings.base_branch,
            },
        )
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]

    def create_pull_request(self, *, title: str, body: str, head: str, token: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/repos/{self.settings.owner}/{self.settings.repo}/pulls",
            token=token,
            json_payload={
                "title": title,
                "body": body,
                "head": head,
                "base": self.settings.base_branch,
                "maintainer_can_modify": False,
            },
        )
        if not isinstance(payload, dict):
            raise GitHubClientError("GitHub PR creation returned invalid payload.", 502)
        return payload

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: str,
        token_kind: str = "installation",
        json_payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.settings.api_base_url}{path}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "airflow-platform-github-app-client",
        }
        if token_kind == "jwt":
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                json=json_payload,
                params=query,
                timeout=20,
            )
        except RequestException as exc:
            raise GitHubClientError(f"GitHub request failed: {exc}") from exc
        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: Response) -> Any:
        text = response.text or ""
        payload: Any
        try:
            payload = response.json() if text else {}
        except Exception:  # noqa: BLE001
            payload = {"raw": text[:1000]}

        if response.status_code >= 400:
            message = ""
            if isinstance(payload, dict):
                message = str(payload.get("message") or "").strip()
            message = message or f"GitHub API error {response.status_code}"
            raise GitHubClientError(message, status_code=response.status_code, details=payload)
        return payload


def safe_json_excerpt(payload: Any) -> str:
    try:
        encoded = json.dumps(payload, ensure_ascii=True, default=str)
    except Exception:  # noqa: BLE001
        return "<unserializable>"
    if len(encoded) > 600:
        return encoded[:600] + "..."
    return encoded
