from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from dev_health_ops.api.models.filters import (
    MetricFilter,
    ScopeFilter,
    TimeFilter,
    WhyFilter,
)
from dev_health_ops.api.services import investment as investment_service
from dev_health_ops.api.services import sankey as sankey_service


@asynccontextmanager
async def _fake_clickhouse_client(_dsn):
    yield object()


@pytest.mark.asyncio
async def test_investment_response_applies_work_category_filter(monkeypatch):
    captured = {}

    async def _fake_breakdown(
        _client, *, start_day, end_day, scope_filter, scope_params
    ):
        captured["breakdown"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_day": start_day,
            "end_day": end_day,
        }
        return [
            {
                "investment_area": "data",
                "project_stream": "etl",
                "delivery_units": 2,
            }
        ]

    async def _fake_edges(_client, *, start_day, end_day, scope_filter, scope_params):
        captured["edges"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_day": start_day,
            "end_day": end_day,
        }
        return [{"source": "data", "target": "etl", "value": 2}]

    monkeypatch.setattr(investment_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(investment_service, "fetch_investment_breakdown", _fake_breakdown)
    monkeypatch.setattr(investment_service, "fetch_investment_edges", _fake_edges)

    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=["data"]),
    )
    response = await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert captured["breakdown"]["scope_params"]["work_categories"] == ["data"]
    assert "investment_area" in captured["breakdown"]["scope_filter"]
    assert "work_categories" in captured["breakdown"]["scope_filter"]
    assert " AND investment_area IN %(work_categories)s" in captured["breakdown"]["scope_filter"]
    assert captured["edges"]["scope_params"]["work_categories"] == ["data"]
    assert " AND investment_area IN %(work_categories)s" in captured["edges"]["scope_filter"]
    assert response.categories[0].key == "data"


@pytest.mark.asyncio
async def test_sankey_investment_applies_work_category_filter(monkeypatch):
    captured = {}

    async def _fake_tables_present(*_args, **_kwargs):
        return True

    async def _fake_columns_present(*_args, **_kwargs):
        return True

    async def _fake_flow_items(
        _client, *, start_day, end_day, scope_filter, scope_params, limit
    ):
        captured["flow"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
            "start_day": start_day,
            "end_day": end_day,
            "limit": limit,
        }
        return [{"source": "data", "target": "etl", "value": 3}]

    async def _fake_repo_scope_filter(*_args, **_kwargs):
        return "", {}

    monkeypatch.setattr(sankey_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(sankey_service, "_tables_present", _fake_tables_present)
    monkeypatch.setattr(sankey_service, "_columns_present", _fake_columns_present)
    monkeypatch.setattr(sankey_service, "fetch_investment_flow_items", _fake_flow_items)
    monkeypatch.setattr(sankey_service, "_repo_scope_filter", _fake_repo_scope_filter)

    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=["data"]),
    )
    response = await sankey_service.build_sankey_response(
        db_url="clickhouse://",
        mode="investment",
        filters=filters,
    )

    assert captured["flow"]["scope_params"]["work_categories"] == ["data"]
    assert "investment_area" in captured["flow"]["scope_filter"]
    assert "work_categories" in captured["flow"]["scope_filter"]
    assert " AND investment_area IN %(work_categories)s" in captured["flow"]["scope_filter"]
    assert response.nodes


@pytest.mark.asyncio
async def test_investment_response_without_work_category_filter(monkeypatch):
    """Test that when work_category filter is absent, no filter is applied."""
    captured = {}

    async def _fake_breakdown(
        _client, *, start_day, end_day, scope_filter, scope_params
    ):
        captured["breakdown"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
        }
        return [
            {
                "investment_area": "data",
                "project_stream": "etl",
                "delivery_units": 2,
            }
        ]

    async def _fake_edges(_client, *, start_day, end_day, scope_filter, scope_params):
        captured["edges"] = {
            "scope_filter": scope_filter,
            "scope_params": scope_params,
        }
        return [{"source": "data", "target": "etl", "value": 2}]

    monkeypatch.setattr(investment_service, "clickhouse_client", _fake_clickhouse_client)
    monkeypatch.setattr(investment_service, "fetch_investment_breakdown", _fake_breakdown)
    monkeypatch.setattr(investment_service, "fetch_investment_edges", _fake_edges)

    # Test with None work_category
    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=None),
    )
    await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert "work_categories" not in captured["breakdown"]["scope_params"]
    assert "investment_area" not in captured["breakdown"]["scope_filter"]
    assert "work_categories" not in captured["breakdown"]["scope_filter"]

    # Test with empty list
    filters = MetricFilter(
        time=TimeFilter(range_days=7, compare_days=7),
        scope=ScopeFilter(level="team", ids=["team-a"]),
        why=WhyFilter(work_category=[]),
    )
    await investment_service.build_investment_response(
        db_url="clickhouse://", filters=filters
    )

    assert "work_categories" not in captured["breakdown"]["scope_params"]
    assert "investment_area" not in captured["breakdown"]["scope_filter"]
