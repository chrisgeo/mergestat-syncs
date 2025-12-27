from __future__ import annotations

import os


from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .models.filters import (
    DrilldownRequest,
    ExplainRequest,
    FilterOptionsResponse,
    HomeRequest,
    MetricFilter,
    ScopeFilter,
    TimeFilter,
)
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
from .queries.filters import fetch_filter_options
from .services.cache import TTLCache
from .services.explain import build_explain_response
from .services.filtering import scope_filter_for_metric, time_window
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


def _filters_from_query(
    scope_type: str,
    scope_id: str,
    range_days: int,
    compare_days: int,
) -> MetricFilter:
    return MetricFilter(
        time=TimeFilter(range_days=range_days, compare_days=compare_days),
        scope=ScopeFilter(level=scope_type, ids=[scope_id] if scope_id else []),
    )


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


@app.post("/api/v1/home", response_model=HomeResponse)
async def home_post(payload: HomeRequest) -> HomeResponse:
    try:
        return await build_home_response(
            db_url=_db_url(),
            filters=payload.filters,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/home", response_model=HomeResponse)
async def home(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> HomeResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, compare_days)
        result = await build_home_response(
            db_url=_db_url(),
            filters=filters,
            cache=HOME_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/explain", response_model=ExplainResponse)
async def explain_post(payload: ExplainRequest) -> ExplainResponse:
    try:
        return await build_explain_response(
            db_url=_db_url(),
            metric=payload.metric,
            filters=payload.filters,
            cache=EXPLAIN_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/explain", response_model=ExplainResponse)
async def explain(
    response: Response,
    metric: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> ExplainResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, compare_days)
        result = await build_explain_response(
            db_url=_db_url(),
            metric=metric,
            filters=filters,
            cache=EXPLAIN_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/drilldown/prs", response_model=DrilldownResponse)
async def drilldown_prs_post(payload: DrilldownRequest) -> DrilldownResponse:
    try:
        start_day, end_day, _, _ = time_window(payload.filters)
        async with clickhouse_client(_db_url()) as client:
            scope_filter, scope_params = await scope_filter_for_metric(
                client, metric_scope="repo", filters=payload.filters
            )
            items = await fetch_pull_requests(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=payload.limit or 50,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/prs", response_model=DrilldownResponse)
async def drilldown_prs(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
) -> DrilldownResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, range_days)
        start_day, end_day, _, _ = time_window(filters)
        async with clickhouse_client(_db_url()) as client:
            scope_filter, scope_params = await scope_filter_for_metric(
                client, metric_scope="repo", filters=filters
            )
            items = await fetch_pull_requests(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/drilldown/issues", response_model=DrilldownResponse)
async def drilldown_issues_post(payload: DrilldownRequest) -> DrilldownResponse:
    try:
        start_day, end_day, _, _ = time_window(payload.filters)
        async with clickhouse_client(_db_url()) as client:
            scope_filter, scope_params = await scope_filter_for_metric(
                client, metric_scope="team", filters=payload.filters
            )
            items = await fetch_issues(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=payload.limit or 50,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/issues", response_model=DrilldownResponse)
async def drilldown_issues(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
) -> DrilldownResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, range_days)
        start_day, end_day, _, _ = time_window(filters)
        async with clickhouse_client(_db_url()) as client:
            scope_filter, scope_params = await scope_filter_for_metric(
                client, metric_scope="team", filters=filters
            )
            items = await fetch_issues(
                client,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/opportunities", response_model=OpportunitiesResponse)
async def opportunities(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
) -> OpportunitiesResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, compare_days)
        result = await build_opportunities_response(
            db_url=_db_url(),
            filters=filters,
            cache=HOME_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/opportunities", response_model=OpportunitiesResponse)
async def opportunities_post(payload: HomeRequest) -> OpportunitiesResponse:
    try:
        return await build_opportunities_response(
            db_url=_db_url(),
            filters=payload.filters,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/investment", response_model=InvestmentResponse)
async def investment(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
) -> InvestmentResponse:
    try:
        filters = _filters_from_query(scope_type, scope_id, range_days, range_days)
        result = await build_investment_response(db_url=_db_url(), filters=filters)
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/investment", response_model=InvestmentResponse)
async def investment_post(payload: HomeRequest) -> InvestmentResponse:
    try:
        return await build_investment_response(
            db_url=_db_url(), filters=payload.filters
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/filters/options", response_model=FilterOptionsResponse)
async def filter_options() -> FilterOptionsResponse:
    try:
        async with clickhouse_client(_db_url()) as client:
            options = await fetch_filter_options(client)
        return FilterOptionsResponse(**options)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc
