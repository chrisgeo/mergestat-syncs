from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from ..models.filters import MetricFilter
from ..queries.scopes import build_scope_filter_multi, resolve_repo_ids


def filter_cache_key(prefix: str, filters: MetricFilter, extra: Dict[str, Any] | None = None) -> str:
    payload = (
        filters.model_dump() if hasattr(filters, "model_dump") else filters.dict()
    )
    if extra:
        payload = {**payload, **extra}
    serialized = json.dumps(payload, sort_keys=True)
    return f"{prefix}:{serialized}"


def time_window(filters: MetricFilter) -> Tuple[date, date, date, date]:
    end_day = date.today() + timedelta(days=1)
    range_days = max(1, filters.time.range_days)
    compare_days = max(1, filters.time.compare_days)
    start_day = end_day - timedelta(days=range_days)
    compare_end = start_day
    compare_start = compare_end - timedelta(days=compare_days)
    return start_day, end_day, compare_start, compare_end


async def resolve_repo_filter_ids(client: Any, filters: MetricFilter) -> List[str]:
    repo_refs: List[str] = []
    if filters.scope.level == "repo":
        repo_refs.extend(filters.scope.ids)
    if filters.what.repos:
        repo_refs.extend(filters.what.repos)
    return await resolve_repo_ids(client, repo_refs)


async def scope_filter_for_metric(
    client: Any,
    *,
    metric_scope: str,
    filters: MetricFilter,
    team_column: str = "team_id",
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    if metric_scope == "team" and filters.scope.level == "team":
        return build_scope_filter_multi(
            "team", filters.scope.ids, team_column=team_column, repo_column=repo_column
        )
    if metric_scope == "repo":
        repo_ids = await resolve_repo_filter_ids(client, filters)
        return build_scope_filter_multi(
            "repo", repo_ids, team_column=team_column, repo_column=repo_column
        )
    return "", {}
