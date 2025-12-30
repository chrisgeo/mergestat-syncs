"""SQL queries for aggregated flame graph data."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from .client import query_dicts


async def fetch_cycle_breakdown(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    team_id: Optional[str] = None,
    provider: Optional[str] = None,
    work_scope_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated state durations for cycle-time breakdown.

    Returns rows with (status, total_duration_hours, items_touched).
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if team_id:
        filters.append("team_id = %(team_id)s")
        params["team_id"] = team_id
    if provider:
        filters.append("provider = %(provider)s")
        params["provider"] = provider
    if work_scope_id:
        filters.append("work_scope_id = %(work_scope_id)s")
        params["work_scope_id"] = work_scope_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            status,
            sum(duration_hours) AS total_hours,
            sum(items_touched) AS total_items
        FROM work_item_state_durations_daily
        WHERE {where_clause}
        GROUP BY status
        ORDER BY total_hours DESC
    """
    return await query_dicts(client, query, params)


async def fetch_code_hotspots(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    repo_id: Optional[str] = None,
    limit: int = 500,
    min_churn: int = 1,
) -> List[Dict[str, Any]]:
    """
    Fetch aggregated file churn for code hotspot flame.

    Returns rows with (repo_id, file_path, total_churn).
    """
    params: Dict[str, Any] = {
        "start_day": start_day,
        "end_day": end_day,
        "limit": limit,
        "min_churn": min_churn,
    }

    filters = ["day >= %(start_day)s", "day < %(end_day)s"]
    if repo_id:
        filters.append("repo_id = %(repo_id)s")
        params["repo_id"] = repo_id

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT
            toString(repo_id) AS repo_id,
            path AS file_path,
            sum(churn) AS total_churn
        FROM file_metrics_daily
        WHERE {where_clause}
        GROUP BY repo_id, path
        HAVING total_churn >= %(min_churn)s
        ORDER BY total_churn DESC
        LIMIT %(limit)s
    """
    return await query_dicts(client, query, params)


async def fetch_repo_names(
    client: Any,
    *,
    repo_ids: List[str],
) -> Dict[str, str]:
    """Fetch repo names for given repo IDs."""
    if not repo_ids:
        return {}

    params = {"repo_ids": repo_ids}
    query = """
        SELECT
            toString(id) AS repo_id,
            repo AS repo_name
        FROM repos
        WHERE id IN %(repo_ids)s
    """
    rows = await query_dicts(client, query, params)
    return {row["repo_id"]: row["repo_name"] for row in rows}
