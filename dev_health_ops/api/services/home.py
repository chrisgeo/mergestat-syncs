from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..models.schemas import (
    ConstraintCard,
    ConstraintEvidence,
    Coverage,
    EventItem,
    Freshness,
    HomeResponse,
    MetricDelta,
    SparkPoint,
    SummarySentence,
)
from ..queries.client import clickhouse_client
from ..queries.explain import fetch_metric_driver_delta
from ..queries.freshness import fetch_coverage, fetch_last_ingested_at
from ..queries.metrics import fetch_blocked_hours, fetch_metric_series, fetch_metric_value
from ..queries.scopes import build_scope_filter, resolve_repo_id
from .cache import TTLCache


_METRICS = [
    {
        "metric": "cycle_time",
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_metrics_daily",
        "column": "cycle_time_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v / 24.0,
        "scope": "team",
    },
    {
        "metric": "review_latency",
        "label": "Review Latency",
        "unit": "hours",
        "table": "repo_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "aggregator": "avg",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "throughput",
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_metrics_daily",
        "column": "items_completed",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "deploy_freq",
        "label": "Deploy Frequency",
        "unit": "deploys",
        "table": "deploy_metrics_daily",
        "column": "deployments_count",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "churn",
        "label": "Code Churn",
        "unit": "loc",
        "table": "repo_metrics_daily",
        "column": "total_loc_touched",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "repo",
    },
    {
        "metric": "wip_saturation",
        "label": "WIP Saturation",
        "unit": "%",
        "table": "work_item_metrics_daily",
        "column": "wip_congestion_ratio",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "team",
    },
    {
        "metric": "blocked_work",
        "label": "Blocked Work",
        "unit": "hours",
        "table": "work_item_state_durations_daily",
        "column": "duration_hours",
        "aggregator": "sum",
        "transform": lambda v: v,
        "scope": "team",
    },
    {
        "metric": "change_failure_rate",
        "label": "Change Failure Rate",
        "unit": "%",
        "table": "repo_metrics_daily",
        "column": "change_failure_rate",
        "aggregator": "avg",
        "transform": lambda v: v * 100.0,
        "scope": "repo",
    },
]


def _window(range_days: int, compare_days: int) -> Tuple[date, date, date, date]:
    end_day = date.today() + timedelta(days=1)
    start_day = end_day - timedelta(days=range_days)
    compare_end = start_day
    compare_start = compare_end - timedelta(days=compare_days)
    return start_day, end_day, compare_start, compare_end


def _delta_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0


def _spark_points(rows: List[Dict[str, Any]], transform) -> List[SparkPoint]:
    points = []
    for row in rows:
        value = float(row.get("value") or 0.0)
        points.append(SparkPoint(ts=row["day"], value=transform(value)))
    return points


def _direction(delta_pct: float) -> str:
    if delta_pct > 0:
        return "rose"
    if delta_pct < 0:
        return "fell"
    return "held steady"


def _format_delta(delta_pct: float) -> str:
    return f"{abs(delta_pct):.0f}%"


async def _metric_deltas(
    client: Any,
    scope_type: str,
    scope_id: str,
    start_day: date,
    end_day: date,
    compare_start: date,
    compare_end: date,
) -> List[MetricDelta]:
    deltas: List[MetricDelta] = []

    scope_repo_id = scope_id
    if scope_type == "repo" and scope_id:
        resolved = await resolve_repo_id(client, scope_id)
        if resolved:
            scope_repo_id = resolved

    for metric in _METRICS:
        scope_filter, scope_params = "", {}
        if metric["scope"] == "team" and scope_type == "team":
            scope_filter, scope_params = build_scope_filter(
                scope_type, scope_id, team_column="team_id"
            )
        elif metric["scope"] == "repo" and scope_type == "repo":
            scope_filter, scope_params = build_scope_filter(
                scope_type, scope_repo_id, repo_column="repo_id"
            )

        if metric["metric"] == "blocked_work":
            current_value, current_series = await fetch_blocked_hours(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            previous_value, _ = await fetch_blocked_hours(
                client,
                start_day=compare_start,
                end_day=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            spark = _spark_points(current_series, metric["transform"])
        else:
            current_value = await fetch_metric_value(
                client,
                table=metric["table"],
                column=metric["column"],
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            previous_value = await fetch_metric_value(
                client,
                table=metric["table"],
                column=metric["column"],
                start_day=compare_start,
                end_day=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            series = await fetch_metric_series(
                client,
                table=metric["table"],
                column=metric["column"],
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                aggregator=metric["aggregator"],
            )
            spark = _spark_points(series, metric["transform"])

        delta_pct = _delta_pct(current_value, previous_value)
        deltas.append(
            MetricDelta(
                metric=metric["metric"],
                label=metric["label"],
                value=metric["transform"](current_value),
                unit=metric["unit"],
                delta_pct=delta_pct,
                spark=spark,
            )
        )

    return deltas


def _select_constraint(deltas: List[MetricDelta]) -> MetricDelta:
    if not deltas:
        return MetricDelta(
            metric="cycle_time",
            label="Cycle Time",
            value=0.0,
            unit="days",
            delta_pct=0.0,
            spark=[],
        )
    return sorted(deltas, key=lambda d: d.delta_pct)[-1]


async def build_home_response(
    *,
    db_url: str,
    scope_type: str,
    scope_id: str,
    range_days: int,
    compare_days: int,
    cache: TTLCache,
) -> HomeResponse:
    cache_key = f"home:{scope_type}:{scope_id}:{range_days}:{compare_days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    start_day, end_day, compare_start, compare_end = _window(
        range_days, compare_days
    )

    async with clickhouse_client(db_url) as client:
        last_ingested = await fetch_last_ingested_at(client)
        coverage = await fetch_coverage(client, start_day=start_day, end_day=end_day)
        deltas = await _metric_deltas(
            client,
            scope_type,
            scope_id,
            start_day,
            end_day,
            compare_start,
            compare_end,
        )

        sources = {
            "github": "ok" if last_ingested else "down",
            "gitlab": "ok" if last_ingested else "down",
            "jira": "ok" if last_ingested else "down",
            "ci": "ok" if last_ingested else "down",
        }

        summary_sentences: List[SummarySentence] = []
        top_delta = max(deltas, key=lambda d: abs(d.delta_pct), default=None)
        if top_delta:
            scope_filter, scope_params = "", {}
            scope_value = scope_id
            if scope_type == "repo" and scope_id:
                resolved = await resolve_repo_id(client, scope_id)
                if resolved:
                    scope_value = resolved
            if _metric_scope(top_delta.metric) == "team" and scope_type == "team":
                scope_filter, scope_params = build_scope_filter(
                    scope_type, scope_value, team_column="team_id"
                )
            elif _metric_scope(top_delta.metric) == "repo" and scope_type == "repo":
                scope_filter, scope_params = build_scope_filter(
                    scope_type, scope_value, repo_column="repo_id"
                )

            driver_rows = await fetch_metric_driver_delta(
                client,
                table=_metric_table(top_delta.metric),
                column=_metric_column(top_delta.metric),
                group_by=_metric_group(top_delta.metric),
                start_day=start_day,
                end_day=end_day,
                compare_start=compare_start,
                compare_end=compare_end,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            driver_labels = ", ".join(
                [str(row.get("id")) for row in driver_rows if row.get("id")] or []
            )
            driver_text = (
                f" driven by {driver_labels}." if driver_labels else "."
            )
            summary_sentences.append(
                SummarySentence(
                    id="s1",
                    text=(
                        f"{top_delta.label} {_direction(top_delta.delta_pct)} "
                        f"{_format_delta(top_delta.delta_pct)}{driver_text}"
                    ),
                    evidence_link=(
                        f"/api/v1/explain?metric={top_delta.metric}"
                        f"&scope_type={scope_type}&scope_id={scope_id}"
                        f"&range_days={range_days}&compare_days={compare_days}"
                    ),
                )
            )

        constraint_metric = _select_constraint(deltas)
        constraint = ConstraintCard(
            title=f"This week's constraint: {constraint_metric.label}",
            claim=(
                f"{constraint_metric.label} {_direction(constraint_metric.delta_pct)} "
                f"{_format_delta(constraint_metric.delta_pct)} over the last {range_days} days."
            ),
            evidence=[
                ConstraintEvidence(
                    label=f"Drill into {constraint_metric.label}",
                    link=(
                        f"/api/v1/explain?metric={constraint_metric.metric}"
                        f"&scope_type={scope_type}&scope_id={scope_id}"
                        f"&range_days={range_days}&compare_days={compare_days}"
                    ),
                )
            ],
            experiments=[
                "Rebalance reviewer rotation to reduce queueing.",
                "Set WIP limits per team and auto-alert at saturation.",
            ],
        )

        events: List[EventItem] = []
        for delta in deltas:
            if abs(delta.delta_pct) >= 25:
                events.append(
                    EventItem(
                        ts=datetime.utcnow(),
                        type="regression" if delta.delta_pct > 0 else "spike",
                        text=(
                            f"{delta.label} shifted {delta.delta_pct:.0f}% "
                            f"over the last {range_days} days."
                        ),
                        link=(
                            f"/api/v1/explain?metric={delta.metric}"
                            f"&scope_type={scope_type}&scope_id={scope_id}"
                            f"&range_days={range_days}&compare_days={compare_days}"
                        ),
                    )
                )

        tiles = {
            "understand": {
                "title": "Understand",
                "subtitle": "Flow stages",
                "link": "/explore?view=understand",
            },
            "measure": {
                "title": "Measure",
                "subtitle": "Coverage & freshness",
                "link": "/explore?view=measure",
            },
            "align": {
                "title": "Align",
                "subtitle": "Investment mix",
                "link": "/investment",
            },
            "execute": {
                "title": "Execute",
                "subtitle": "Top opportunities",
                "link": "/opportunities",
            },
        }

        response = HomeResponse(
            freshness=Freshness(
                last_ingested_at=last_ingested,
                sources=sources,
                coverage=Coverage(**coverage),
            ),
            deltas=deltas,
            summary=summary_sentences,
            tiles=tiles,
            constraint=constraint,
            events=events,
        )

    cache.set(cache_key, response)
    return response


def _metric_table(metric: str) -> str:
    return next(
        (cfg["table"] for cfg in _METRICS if cfg["metric"] == metric),
        "repo_metrics_daily",
    )


def _metric_column(metric: str) -> str:
    return next(
        (cfg["column"] for cfg in _METRICS if cfg["metric"] == metric),
        "pr_first_review_p50_hours",
    )


def _metric_group(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team_id"
    return "repo_id"


def _metric_scope(metric: str) -> str:
    if metric in {"cycle_time", "throughput", "wip_saturation", "blocked_work"}:
        return "team"
    return "repo"
