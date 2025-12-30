from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from .client import query_dicts


async def fetch_investment_flow_items(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            inv.artifact_id AS artifact_id,
            any(inv.investment_area) AS investment_area,
            any(inv.project_stream) AS project_stream,
            any(wi.type) AS issue_type,
            any(wi.title) AS title,
            count() AS item_count
        FROM investment_classifications_daily AS inv
        LEFT JOIN work_items AS wi
            ON wi.work_item_id = inv.artifact_id
            AND (inv.repo_id = wi.repo_id OR inv.repo_id IS NULL)
        WHERE inv.day >= %(start_day)s AND inv.day < %(end_day)s
            AND inv.artifact_type = 'work_item'
            {scope_filter}
        GROUP BY inv.artifact_id
        ORDER BY item_count DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    return await query_dicts(client, query, params)


async def fetch_expense_counts(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    query = f"""
        WITH
            lowerUTF8(ifNull(type, '')) AS type_lower,
            lowerUTF8(ifNull(status, '')) AS status_lower,
            lowerUTF8(ifNull(status_raw, '')) AS status_raw_lower,
            arrayMap(x -> lowerUTF8(x), labels) AS labels_lower,
            (
                type_lower LIKE '%bug%'
                OR type_lower LIKE '%incident%'
                OR type_lower LIKE '%support%'
                OR type_lower LIKE '%interrupt%'
                OR arrayExists(
                    x -> x LIKE '%bug%'
                        OR x LIKE '%incident%'
                        OR x LIKE '%hotfix%'
                        OR x LIKE '%support%'
                        OR x LIKE '%unplanned%',
                    labels_lower
                )
            ) AS is_unplanned,
            (
                type_lower LIKE '%refactor%'
                OR type_lower LIKE '%chore%'
                OR type_lower LIKE '%maintenance%'
                OR arrayExists(
                    x -> x LIKE '%rework%'
                        OR x LIKE '%refactor%'
                        OR x LIKE '%cleanup%'
                        OR x LIKE '%tech debt%'
                        OR x LIKE '%rewrite%',
                    labels_lower
                )
            ) AS is_rework,
            (
                status_lower LIKE '%cancel%'
                OR status_lower LIKE '%wont%'
                OR status_lower LIKE '%abandon%'
                OR status_lower LIKE '%duplicate%'
                OR status_lower LIKE '%invalid%'
                OR status_raw_lower LIKE '%cancel%'
                OR status_raw_lower LIKE '%wont%'
                OR status_raw_lower LIKE '%abandon%'
                OR status_raw_lower LIKE '%duplicate%'
                OR status_raw_lower LIKE '%invalid%'
            ) AS is_abandoned
        SELECT
            count() AS total_items,
            countIf(is_unplanned) AS unplanned_items,
            countIf(is_unplanned AND is_rework) AS rework_items,
            countIf(is_unplanned AND is_rework AND is_abandoned) AS abandoned_items
        FROM work_items
        WHERE created_at < %(end_day)s
            AND (completed_at IS NULL OR completed_at >= %(start_day)s)
            {scope_filter}
    """
    params = {"start_day": start_day, "end_day": end_day}
    params.update(scope_params)
    return await query_dicts(client, query, params)


async def fetch_state_transitions(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    query = f"""
        SELECT
            from_status AS source,
            to_status AS target,
            count() AS value
        FROM work_item_transitions
        WHERE occurred_at >= %(start_day)s AND occurred_at < %(end_day)s
            AND from_status != ''
            AND to_status != ''
            {scope_filter}
        GROUP BY source, target
        ORDER BY value DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    return await query_dicts(client, query, params)


async def fetch_hotspot_rows(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: Dict[str, Any],
    limit: int,
) -> List[Dict[str, Any]]:
    query = f"""
        WITH
            lowerUTF8(ifNull(commits.message, '')) AS message_lower
        SELECT
            repos.repo AS repo,
            if(
                position(stats.file_path, '/') > 0,
                arrayElement(splitByChar('/', stats.file_path), 1),
                '(root)'
            ) AS directory,
            stats.file_path AS file_path,
            multiIf(
                message_lower LIKE '%fix%'
                    OR message_lower LIKE '%bug%'
                    OR message_lower LIKE '%hotfix%'
                    OR message_lower LIKE '%patch%',
                'fix',
                message_lower LIKE '%refactor%'
                    OR message_lower LIKE '%cleanup%'
                    OR message_lower LIKE '%chore%'
                    OR message_lower LIKE '%tech debt%',
                'refactor',
                'feature'
            ) AS change_type,
            sum(stats.additions + stats.deletions) AS churn
        FROM git_commit_stats AS stats
        INNER JOIN git_commits AS commits
            ON commits.repo_id = stats.repo_id
            AND commits.hash = stats.commit_hash
        INNER JOIN repos
            ON repos.id = stats.repo_id
        WHERE commits.author_when >= %(start_day)s AND commits.author_when < %(end_day)s
            AND stats.file_path != ''
            {scope_filter}
        GROUP BY repo, directory, file_path, change_type
        ORDER BY churn DESC
        LIMIT %(limit)s
    """
    params = {"start_day": start_day, "end_day": end_day, "limit": limit}
    params.update(scope_params)
    return await query_dicts(client, query, params)
