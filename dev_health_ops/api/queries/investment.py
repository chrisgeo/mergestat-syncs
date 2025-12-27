from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from .client import query_dicts


async def fetch_investment_breakdown(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            investment_area,
            project_stream,
            sum(delivery_units) AS delivery_units
        FROM investment_metrics_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
        GROUP BY investment_area, project_stream
        ORDER BY delivery_units DESC
    """
    params = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    return await query_dicts(client, query, params)


async def fetch_investment_edges(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            investment_area AS source,
            project_stream AS target,
            sum(delivery_units) AS value
        FROM investment_metrics_daily
        WHERE day >= %(start_day)s AND day < %(end_day)s
        {scope_filter}
        GROUP BY investment_area, project_stream
        ORDER BY value DESC
    """
    params = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    return await query_dicts(client, query, params)
