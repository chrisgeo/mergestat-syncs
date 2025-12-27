from __future__ import annotations

from datetime import date, timedelta
from typing import List

from ..models.schemas import Contributor, ExplainResponse
from ..queries.client import clickhouse_client
from ..queries.explain import fetch_metric_contributors, fetch_metric_driver_delta
from ..queries.metrics import fetch_metric_value
from ..queries.scopes import build_scope_filter, resolve_repo_id
from .cache import TTLCache


_METRIC_CONFIG = {
    "cycle_time": {
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_metrics_daily",
        "column": "cycle_time_p50_hours",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "avg",
        "transform": lambda v: v / 24.0,
    },
    "review_latency": {
        "label": "Review Latency",
        "unit": "hours",
        "table": "repo_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "avg",
        "transform": lambda v: v,
    },
    "throughput": {
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_metrics_daily",
        "column": "items_completed",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "deploy_freq": {
        "label": "Deploy Frequency",
        "unit": "deploys",
        "table": "deploy_metrics_daily",
        "column": "deployments_count",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "churn": {
        "label": "Code Churn",
        "unit": "loc",
        "table": "repo_metrics_daily",
        "column": "total_loc_touched",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "wip_saturation": {
        "label": "WIP Saturation",
        "unit": "%",
        "table": "work_item_metrics_daily",
        "column": "wip_congestion_ratio",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
    },
    "blocked_work": {
        "label": "Blocked Work",
        "unit": "hours",
        "table": "work_item_state_durations_daily",
        "column": "duration_hours",
        "group_by": "team_id",
        "scope": "team",
        "aggregator": "sum",
        "transform": lambda v: v,
    },
    "change_failure_rate": {
        "label": "Change Failure Rate",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "change_failure_rate",
        "group_by": "repo_id",
        "scope": "repo",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
    },
}


def _window(range_days: int, compare_days: int):
    end_day = date.today() + timedelta(days=1)
    start_day = end_day - timedelta(days=range_days)
    compare_end = start_day
    compare_start = compare_end - timedelta(days=compare_days)
    return start_day, end_day, compare_start, compare_end


def _delta_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0


async def build_explain_response(
    *,
    db_url: str,
    metric: str,
    scope_type: str,
    scope_id: str,
    range_days: int,
    compare_days: int,
    cache: TTLCache,
) -> ExplainResponse:
    cache_key = f"explain:{metric}:{scope_type}:{scope_id}:{range_days}:{compare_days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    config = _METRIC_CONFIG.get(metric, _METRIC_CONFIG["cycle_time"])
    start_day, end_day, compare_start, compare_end = _window(
        range_days, compare_days
    )

    async with clickhouse_client(db_url) as client:
        scope_value = scope_id
        if scope_type == "repo" and scope_id:
            resolved = await resolve_repo_id(client, scope_id)
            if resolved:
                scope_value = resolved

        scope_filter, scope_params = "", {}
        if config["scope"] == "team" and scope_type == "team":
            scope_filter, scope_params = build_scope_filter(
                scope_type, scope_value, team_column="team_id"
            )
        elif config["scope"] == "repo" and scope_type == "repo":
            scope_filter, scope_params = build_scope_filter(
                scope_type, scope_value, repo_column="repo_id"
            )

        current_value = await fetch_metric_value(
            client,
            table=config["table"],
            column=config["column"],
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
            aggregator=config["aggregator"],
        )
        previous_value = await fetch_metric_value(
            client,
            table=config["table"],
            column=config["column"],
            start_day=compare_start,
            end_day=compare_end,
            scope_filter=scope_filter,
            scope_params=scope_params,
            aggregator=config["aggregator"],
        )

        delta_pct = _delta_pct(current_value, previous_value)

        drivers = await fetch_metric_driver_delta(
            client,
            table=config["table"],
            column=config["column"],
            group_by=config["group_by"],
            start_day=start_day,
            end_day=end_day,
            compare_start=compare_start,
            compare_end=compare_end,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )
        contributors = await fetch_metric_contributors(
            client,
            table=config["table"],
            column=config["column"],
            group_by=config["group_by"],
            start_day=start_day,
            end_day=end_day,
            scope_filter=scope_filter,
            scope_params=scope_params,
        )

    driver_models: List[Contributor] = []
    for row in drivers:
        driver_models.append(
            Contributor(
                id=str(row.get("id") or ""),
                label=str(row.get("id") or "Unknown"),
                value=config["transform"](float(row.get("value") or 0.0)),
                delta_pct=float(row.get("delta_pct") or 0.0),
                evidence_link=(
                    f"/api/v1/drilldown/prs?metric={metric}"
                    f"&scope_type={scope_type}&scope_id={scope_id}"
                ),
            )
        )

    contributor_models: List[Contributor] = []
    for row in contributors:
        contributor_models.append(
            Contributor(
                id=str(row.get("id") or ""),
                label=str(row.get("id") or "Unknown"),
                value=config["transform"](float(row.get("value") or 0.0)),
                delta_pct=0.0,
                evidence_link=(
                    f"/api/v1/drilldown/prs?metric={metric}"
                    f"&scope_type={scope_type}&scope_id={scope_id}"
                ),
            )
        )

    response = ExplainResponse(
        metric=metric,
        label=config["label"],
        unit=config["unit"],
        value=config["transform"](current_value),
        delta_pct=delta_pct,
        drivers=driver_models,
        contributors=contributor_models,
        drilldown_links={
            "prs": f"/api/v1/drilldown/prs?metric={metric}",
            "issues": f"/api/v1/drilldown/issues?metric={metric}",
        },
    )

    cache.set(cache_key, response)
    return response
