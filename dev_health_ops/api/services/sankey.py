from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from ..models.filters import MetricFilter, SankeyContext
from ..models.schemas import SankeyLink, SankeyNode, SankeyResponse
from ..queries.client import clickhouse_client
from ..queries.sankey import (
    fetch_expense_counts,
    fetch_hotspot_rows,
    fetch_investment_flow_items,
    fetch_state_transitions,
)
from ..queries.scopes import build_scope_filter_multi
from .filtering import resolve_repo_filter_ids, time_window


@dataclass(frozen=True)
class SankeyDefinition:
    label: str
    description: str
    unit: str


SANKEY_DEFINITIONS: Dict[str, SankeyDefinition] = {
    "investment": SankeyDefinition(
        label="Investment flow",
        description=(
            "Where effort allocates across initiatives, areas, issue types, and work items."
        ),
        unit="items",
    ),
    "expense": SankeyDefinition(
        label="Investment expense",
        description="How planned effort converts into unplanned work, rework, and rewrites.",
        unit="items",
    ),
    "state": SankeyDefinition(
        label="State flow",
        description="Execution paths that reveal stalls, loops, and retry patterns.",
        unit="items",
    ),
    "hotspot": SankeyDefinition(
        label="Code hotspot flow",
        description="Where change concentrates from repos to files and change intent.",
        unit="changes",
    ),
}

MAX_INVESTMENT_ITEMS = 60
MAX_STATE_EDGES = 120
MAX_HOTSPOT_ROWS = 150


def _apply_window_to_filters(
    filters: MetricFilter,
    window_start: Optional[date],
    window_end: Optional[date],
) -> MetricFilter:
    if not window_start and not window_end:
        return filters
    payload = (
        filters.model_dump(mode="json")
        if hasattr(filters, "model_dump")
        else filters.dict()
    )
    time_payload = payload.get("time") or {}
    if window_start:
        time_payload["start_date"] = window_start
    if window_end:
        time_payload["end_date"] = window_end
    if window_start and window_end:
        delta_days = max(1, (window_end - window_start).days)
        time_payload["range_days"] = delta_days
    payload["time"] = time_payload
    return MetricFilter(**payload)


def _normalize_label(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _trim_label(value: str, limit: int = 72) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _add_edge(
    edges: Dict[Tuple[str, str], float],
    source: str,
    target: str,
    value: float,
) -> None:
    if value <= 0:
        return
    key = (source, target)
    edges[key] = edges.get(key, 0.0) + value


def _touch_node(
    nodes: Dict[str, SankeyNode],
    name: str,
    group: Optional[str],
) -> None:
    if name in nodes:
        return
    nodes[name] = SankeyNode(name=name, group=group)


def _links_from_edges(edges: Dict[Tuple[str, str], float]) -> List[SankeyLink]:
    links = [
        SankeyLink(source=source, target=target, value=value)
        for (source, target), value in edges.items()
        if value > 0
    ]
    links.sort(key=lambda link: link.value, reverse=True)
    return links


async def _repo_scope_filter(
    client: Any,
    filters: MetricFilter,
    repo_column: str = "repo_id",
) -> Tuple[str, Dict[str, Any]]:
    repo_ids = await resolve_repo_filter_ids(client, filters)
    if not repo_ids:
        return "", {}
    return build_scope_filter_multi("repo", repo_ids, repo_column=repo_column)


async def _build_investment_flow(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    scope_filter, scope_params = await _repo_scope_filter(
        client, filters, repo_column="coalesce(inv.repo_id, wi.repo_id)"
    )
    rows = await fetch_investment_flow_items(
        client,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
        limit=MAX_INVESTMENT_ITEMS,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}
    for row in rows:
        value = float(row.get("item_count") or 0.0)
        if value <= 0:
            continue
        area = _normalize_label(row.get("investment_area"), "Unassigned")
        stream = _normalize_label(row.get("project_stream"), "Other")
        issue_type = _normalize_label(row.get("issue_type"), "Unspecified")
        item_label = _normalize_label(
            row.get("title") or row.get("artifact_id"),
            "Work item",
        )
        item_label = _trim_label(item_label)

        _touch_node(nodes, area, "initiative")
        _touch_node(nodes, stream, "project")
        _touch_node(nodes, issue_type, "issue_type")
        _touch_node(nodes, item_label, "work_item")

        _add_edge(edges, area, stream, value)
        _add_edge(edges, stream, issue_type, value)
        _add_edge(edges, issue_type, item_label, value)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_expense_flow(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    scope_filter, scope_params = await _repo_scope_filter(client, filters)
    rows = await fetch_expense_counts(
        client,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
    )
    if not rows:
        return [], []
    row = rows[0]
    unplanned = float(row.get("unplanned_items") or 0.0)
    rework = float(row.get("rework_items") or 0.0)
    abandoned = float(row.get("abandoned_items") or 0.0)

    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}

    _touch_node(nodes, "Planned work", "planned")
    _touch_node(nodes, "Unplanned work", "unplanned")
    _touch_node(nodes, "Rework", "rework")
    _touch_node(nodes, "Abandonment / rewrite", "abandonment")

    _add_edge(edges, "Planned work", "Unplanned work", unplanned)
    _add_edge(edges, "Unplanned work", "Rework", rework)
    _add_edge(edges, "Rework", "Abandonment / rewrite", abandoned)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_state_flow(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    scope_filter, scope_params = await _repo_scope_filter(client, filters)
    rows = await fetch_state_transitions(
        client,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
        limit=MAX_STATE_EDGES,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}
    for row in rows:
        value = float(row.get("value") or 0.0)
        if value <= 0:
            continue
        source = _normalize_label(row.get("source"), "Unknown")
        target = _normalize_label(row.get("target"), "Unknown")
        _touch_node(nodes, source, "state")
        _touch_node(nodes, target, "state")
        _add_edge(edges, source, target, value)

    return list(nodes.values()), _links_from_edges(edges)


async def _build_hotspot_flow(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    filters: MetricFilter,
) -> Tuple[List[SankeyNode], List[SankeyLink]]:
    scope_filter, scope_params = await _repo_scope_filter(client, filters)
    rows = await fetch_hotspot_rows(
        client,
        start_day=start_day,
        end_day=end_day,
        scope_filter=scope_filter,
        scope_params=scope_params,
        limit=MAX_HOTSPOT_ROWS,
    )
    nodes: Dict[str, SankeyNode] = {}
    edges: Dict[Tuple[str, str], float] = {}
    for row in rows:
        churn = float(row.get("churn") or 0.0)
        if churn <= 0:
            continue
        repo = _normalize_label(row.get("repo"), "Unknown repo")
        directory = _normalize_label(row.get("directory"), "(root)")
        file_path = _normalize_label(row.get("file_path"), "unknown file")
        change_type = _normalize_label(row.get("change_type"), "feature")

        directory_label = f"{repo} / {directory}"
        file_label = f"{repo} / {file_path}"

        _touch_node(nodes, repo, "repo")
        _touch_node(nodes, directory_label, "directory")
        _touch_node(nodes, file_label, "file")
        _touch_node(nodes, change_type, "change_type")

        _add_edge(edges, repo, directory_label, churn)
        _add_edge(edges, directory_label, file_label, churn)
        _add_edge(edges, file_label, change_type, churn)

    return list(nodes.values()), _links_from_edges(edges)


async def build_sankey_response(
    *,
    db_url: str,
    mode: str,
    filters: MetricFilter,
    context: Optional[SankeyContext] = None,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
) -> SankeyResponse:
    definition = SANKEY_DEFINITIONS.get(mode)
    if definition is None:
        raise ValueError(f"Unknown sankey mode: {mode}")

    resolved_filters = _apply_window_to_filters(filters, window_start, window_end)
    start_day, end_day, _, _ = time_window(resolved_filters)

    async with clickhouse_client(db_url) as client:
        if mode == "investment":
            nodes, links = await _build_investment_flow(
                client,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "expense":
            nodes, links = await _build_expense_flow(
                client,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "state":
            nodes, links = await _build_state_flow(
                client,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        elif mode == "hotspot":
            nodes, links = await _build_hotspot_flow(
                client,
                start_day=start_day,
                end_day=end_day,
                filters=resolved_filters,
            )
        else:
            nodes, links = [], []

    return SankeyResponse(
        mode=mode,
        nodes=nodes,
        links=links,
        unit=definition.unit,
        label=definition.label,
        description=definition.description,
    )
