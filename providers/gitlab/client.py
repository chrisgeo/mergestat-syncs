from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from connectors.utils.rate_limit_queue import RateLimitConfig, RateLimitGate


@dataclass(frozen=True)
class GitLabAuth:
    token: str
    base_url: str = "https://gitlab.com"


class GitLabWorkClient:
    """
    Work-tracking oriented GitLab client using python-gitlab.
    """

    def __init__(
        self,
        *,
        auth: GitLabAuth,
        per_page: int = 100,
        gate: Optional[RateLimitGate] = None,
    ) -> None:
        import gitlab  # python-gitlab

        self.auth = auth
        self.per_page = max(1, min(100, int(per_page)))
        self.gate = gate or RateLimitGate(RateLimitConfig(initial_backoff_seconds=1.0))

        self.gl = gitlab.Gitlab(
            auth.base_url,
            private_token=auth.token,
            per_page=self.per_page,
        )

    @classmethod
    def from_env(cls) -> "GitLabWorkClient":
        token = os.getenv("GITLAB_TOKEN") or ""
        url = os.getenv("GITLAB_URL") or "https://gitlab.com"
        if not token:
            raise ValueError("GitLab token required (set GITLAB_TOKEN)")
        return cls(auth=GitLabAuth(token=token, base_url=url))

    def get_project(self, project_id_or_path: str) -> Any:
        self.gate.wait_sync()
        try:
            project = self.gl.projects.get(project_id_or_path)
            self.gate.reset()
            return project
        except Exception:
            self.gate.penalize(None)
            raise

    def iter_project_issues(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Any]:
        project = self.get_project(project_id_or_path)
        params: Dict[str, Any] = {"state": state}
        if updated_after is not None:
            params["updated_after"] = updated_after.isoformat()
        issues = project.issues.list(iterator=True, **params)
        count = 0
        for issue in issues:
            yield issue
            count += 1
            if limit is not None and count >= int(limit):
                return

