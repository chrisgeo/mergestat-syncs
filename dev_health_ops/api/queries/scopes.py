from __future__ import annotations

import uuid
from typing import Any, Dict, Optional, Tuple

from .client import query_dicts


def parse_uuid(value: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


async def resolve_repo_id(client: Any, repo_ref: str) -> Optional[str]:
    repo_uuid = parse_uuid(repo_ref)
    if repo_uuid:
        return str(repo_uuid)
    query = """
        SELECT id
        FROM repos
        WHERE repo = %(repo_name)s
        LIMIT 1
    """
    rows = await query_dicts(client, query, {"repo_name": repo_ref})
    if not rows:
        return None
    return str(rows[0]["id"])


def build_scope_filter(
    scope_type: str,
    scope_id: str,
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    if scope_type == "team" and scope_id:
        return f" AND {team_column} = %(scope_id)s", {"scope_id": scope_id}
    if scope_type == "repo" and scope_id:
        return f" AND {repo_column} = %(repo_id)s", {"repo_id": scope_id}
    return "", {}
