from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import json

import pytest

from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
from dev_health_ops.api.services.cache import TTLCache
from dev_health_ops.api.services import explain as explain_service
from dev_health_ops.api.services import home as home_service


def _payload(model):
    if hasattr(model, "model_dump"):
        try:
            return model.model_dump(mode="json")
        except TypeError:
            return model.model_dump()
    return model.dict()


@asynccontextmanager
async def _fake_clickhouse_client(_dsn):
    yield object()


@pytest.mark.asyncio
async def test_home_response_sanitizes_non_finite_values(monkeypatch):
    async def _fake_scope_filter_for_metric(*_args, **_kwargs):
        return "", {}

    async def _fake_fetch_metric_value(*_args, **_kwargs):
        return float("nan")

    async def _fake_fetch_metric_series(*_args, **_kwargs):
        return [{"day": date(2024, 1, 1), "value": float("nan")}]

    async def _fake_fetch_blocked_hours(*_args, **_kwargs):
        return float("nan"), [{"day": date(2024, 1, 1), "value": float("nan")}]

    async def _fake_fetch_last_ingested_at(*_args, **_kwargs):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)

    async def _fake_fetch_coverage(*_args, **_kwargs):
        return {
            "repos_covered_pct": 0.0,
            "prs_linked_to_issues_pct": 0.0,
            "issues_with_cycle_states_pct": 0.0,
        }

    async def _fake_fetch_metric_driver_delta(*_args, **_kwargs):
        return []

    monkeypatch.setattr(home_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(home_service, "scope_filter_for_metric", _fake_scope_filter_for_metric)
    monkeypatch.setattr(home_service, "fetch_metric_value", _fake_fetch_metric_value)
    monkeypatch.setattr(home_service, "fetch_metric_series", _fake_fetch_metric_series)
    monkeypatch.setattr(home_service, "fetch_blocked_hours", _fake_fetch_blocked_hours)
    monkeypatch.setattr(home_service, "fetch_last_ingested_at", _fake_fetch_last_ingested_at)
    monkeypatch.setattr(home_service, "fetch_coverage", _fake_fetch_coverage)
    monkeypatch.setattr(home_service, "fetch_metric_driver_delta", _fake_fetch_metric_driver_delta)

    response = await home_service.build_home_response(
        db_url="clickhouse://",
        filters=MetricFilter(
            time=TimeFilter(range_days=7, compare_days=7),
            scope=ScopeFilter(level="team", ids=["core"]),
        ),
        cache=TTLCache(ttl_seconds=1),
    )

    json.dumps(_payload(response), allow_nan=False)


@pytest.mark.asyncio
async def test_explain_response_sanitizes_non_finite_values(monkeypatch):
    async def _fake_scope_filter_for_metric(*_args, **_kwargs):
        return "", {}

    async def _fake_fetch_metric_value(*_args, **_kwargs):
        return float("nan")

    async def _fake_fetch_metric_driver_delta(*_args, **_kwargs):
        return [{"id": "team-a", "value": float("nan"), "delta_pct": float("nan")}]

    async def _fake_fetch_metric_contributors(*_args, **_kwargs):
        return [{"id": "team-b", "value": float("nan")}]

    monkeypatch.setattr(explain_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(explain_service, "scope_filter_for_metric", _fake_scope_filter_for_metric)
    monkeypatch.setattr(explain_service, "fetch_metric_value", _fake_fetch_metric_value)
    monkeypatch.setattr(explain_service, "fetch_metric_driver_delta", _fake_fetch_metric_driver_delta)
    monkeypatch.setattr(explain_service, "fetch_metric_contributors", _fake_fetch_metric_contributors)

    response = await explain_service.build_explain_response(
        db_url="clickhouse://",
        metric="wip_saturation",
        filters=MetricFilter(
            time=TimeFilter(range_days=7, compare_days=7),
            scope=ScopeFilter(level="team", ids=["core"]),
        ),
        cache=TTLCache(ttl_seconds=1),
    )

    json.dumps(_payload(response), allow_nan=False)
