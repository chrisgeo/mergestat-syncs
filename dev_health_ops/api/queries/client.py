from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

import clickhouse_connect


def _rows_to_dicts(result: Any) -> List[Dict[str, Any]]:
    col_names = list(getattr(result, "column_names", []) or [])
    rows = list(getattr(result, "result_rows", []) or [])
    if not col_names or not rows:
        return []
    return [dict(zip(col_names, row)) for row in rows]


@asynccontextmanager
async def clickhouse_client(dsn: str) -> AsyncIterator[Any]:
    if hasattr(clickhouse_connect, "get_async_client"):
        client = await clickhouse_connect.get_async_client(dsn=dsn)
    else:
        client = clickhouse_connect.get_client(dsn=dsn)
    try:
        yield client
    finally:
        close = getattr(client, "close", None)
        if close is not None:
            if inspect.iscoroutinefunction(close):
                await close()
            else:
                close()


async def query_dicts(client: Any, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = client.query(query, parameters=params)
    if inspect.isawaitable(result):
        result = await result
    return _rows_to_dicts(result)
