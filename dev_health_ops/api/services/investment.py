from __future__ import annotations

from typing import Dict, List

from ..models.filters import MetricFilter
from ..models.schemas import InvestmentCategory, InvestmentResponse, InvestmentSubtype
from ..queries.client import clickhouse_client
from ..queries.investment import fetch_investment_breakdown, fetch_investment_edges
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window, work_category_filter


async def build_investment_response(
    *,
    db_url: str,
    filters: MetricFilter,
) -> InvestmentResponse:
    start_day, end_day, _, _ = time_window(filters)

    async with clickhouse_client(db_url) as client:
        scope_filter, scope_params = "", {}
        if filters.scope.level == "team":
            scope_filter, scope_params = build_scope_filter_multi(
                "team", filters.scope.ids, team_column="team_id"
            )
        elif filters.scope.level == "repo":
            repo_ids = await resolve_repo_filter_ids(client, filters)
            scope_filter, scope_params = build_scope_filter_multi(
                "repo", repo_ids, repo_column="repo_id"
            )
        category_filter, category_params = work_category_filter(filters)
        scope_filter = f"{scope_filter}{category_filter}"
        scope_params = {**scope_params, **category_params}
        rows = await fetch_investment_breakdown(
            client,
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )
        edges = await fetch_investment_edges(
            client,
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )

    category_totals: Dict[str, float] = {}
    for row in rows:
        key = str(row.get("investment_area") or "Unassigned")
        category_totals[key] = category_totals.get(key, 0.0) + float(
            row.get("delivery_units") or 0.0
        )

    categories = [
        InvestmentCategory(key=key, name=key.title(), value=value)
        for key, value in category_totals.items()
    ]
    categories.sort(key=lambda item: item.value, reverse=True)

    subtypes: List[InvestmentSubtype] = []
    for row in rows:
        area = str(row.get("investment_area") or "Unassigned")
        stream = str(row.get("project_stream") or "Other")
        subtypes.append(
            InvestmentSubtype(
                name=stream.title(),
                value=float(row.get("delivery_units") or 0.0),
                parentKey=area,
            )
        )

    return InvestmentResponse(categories=categories, subtypes=subtypes, edges=edges)
