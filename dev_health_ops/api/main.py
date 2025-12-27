from __future__ import annotations

import os
from typing import Optional

from datetime import date, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .models.schemas import (
    DrilldownResponse,
    ExplainResponse,
    HealthResponse,
    HomeResponse,
    InvestmentResponse,
    OpportunitiesResponse,
)
from .queries.client import clickhouse_client, query_dicts
from .queries.drilldown import fetch_issues, fetch_pull_requests
from .queries.scopes import build_scope_filter, resolve_repo_id
from .services.cache import TTLCache
from .services.explain import build_explain_response
from .services.home import build_home_response
from .services.investment import build_investment_response
from .services.opportunities import build_opportunities_response

HOME_CACHE = TTLCache(ttl_seconds=60)
EXPLAIN_CACHE = TTLCache(ttl_seconds=120)


def _db_url() -> str:
    return (
        os.getenv("CLICKHOUSE_DSN")
        or os.getenv("DB_CONN_STRING")
        or "clickhouse://localhost:8123/default"
    )


def _window(range_days: int):
    end_day = date.today() + timedelta(days=1)
    start_day = end_day - timedelta(days=range_days)
    return start_day, end_day


app = FastAPI(
    title="Dev Health Ops API",
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/api/v1/health", response_model=HealthResponse)
async def health() -> HealthResponse | JSONResponse:
    services = {}
    try:
        async with clickhouse_client(_db_url()) as client:
            rows = await query_dicts(client, "SELECT 1 AS ok", {})
        services["clickhouse"] = "ok" if rows else "down"
    except Exception:
        services["clickhouse"] = "down"

    status = "ok" if all(state == "ok" for state in services.values()) else "down"
    response = HealthResponse(status=status, services=services)
    if status != "ok":
        content = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response.dict()
        )
        return JSONResponse(status_code=503, content=content)
    return response


@app.get("/api/v1/home", response_model=HomeResponse)
async def home(
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> HomeResponse:
    try:
        return await build_home_response(
            db_url=_db_url(),
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
            compare_days=compare_days,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/explain", response_model=ExplainResponse)
async def explain(
    metric: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> ExplainResponse:
    try:
        return await build_explain_response(
            db_url=_db_url(),
            metric=metric,
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
            compare_days=compare_days,
            cache=EXPLAIN_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/prs", response_model=DrilldownResponse)
async def drilldown_prs(
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
) -> DrilldownResponse:
    try:
        start_day, end_day = _window(range_days)
        async with clickhouse_client(_db_url()) as client:
            scope_value: Optional[str] = scope_id
            if scope_type == "repo" and scope_id:
                resolved = await resolve_repo_id(client, scope_id)
                if resolved:
                    scope_value = resolved

            if scope_type == "repo":
                scope_filter, scope_params = build_scope_filter(
                    scope_type, scope_value or "", repo_column="repo_id"
                )
            else:
                scope_filter, scope_params = "", {}
            items = await fetch_pull_requests(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/issues", response_model=DrilldownResponse)
async def drilldown_issues(
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
) -> DrilldownResponse:
    try:
        start_day, end_day = _window(range_days)
        async with clickhouse_client(_db_url()) as client:
            if scope_type == "team":
                scope_filter, scope_params = build_scope_filter(
                    scope_type, scope_id, team_column="team_id"
                )
            else:
                scope_filter, scope_params = "", {}
            items = await fetch_issues(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/opportunities", response_model=OpportunitiesResponse)
async def opportunities(
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> OpportunitiesResponse:
    try:
        return await build_opportunities_response(
            db_url=_db_url(),
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
            compare_days=compare_days,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/investment", response_model=InvestmentResponse)
async def investment(
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
) -> InvestmentResponse:
    try:
        return await build_investment_response(
            db_url=_db_url(),
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc
