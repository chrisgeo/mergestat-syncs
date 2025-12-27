from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List

from ..models.schemas import InvestmentCategory, InvestmentResponse, InvestmentSubtype
from ..queries.client import clickhouse_client
from ..queries.investment import fetch_investment_breakdown, fetch_investment_edges
from ..queries.scopes import build_scope_filter


def _window(range_days: int):
    end_day = date.today() + timedelta(days=1)
    start_day = end_day - timedelta(days=range_days)
    return start_day, end_day


async def build_investment_response(
    *,
    db_url: str,
    scope_type: str,
    scope_id: str,
    range_days: int,
) -> InvestmentResponse:
    start_day, end_day = _window(range_days)

    async with clickhouse_client(db_url) as client:
        scope_filter, scope_params = build_scope_filter(
            scope_type, scope_id, team_column="team_id"
        )
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
