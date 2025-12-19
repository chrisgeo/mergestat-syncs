from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from metrics.schemas import (
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from models.work_items import WorkItem
from providers.teams import TeamResolver
import logging


def _utc_day_window(day: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    if percentile <= 0:
        return float(min(values))
    if percentile >= 100:
        return float(max(values))
    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    rank = (len(sorted_vals) - 1) * (float(percentile) / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _resolve_team(
    team_resolver: Optional[TeamResolver], identity: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    if team_resolver is None:
        return None, None
    return team_resolver.resolve(identity)


def compute_work_item_metrics_daily(
    *,
    day: date,
    work_items: Sequence[WorkItem],
    computed_at: datetime,
    team_resolver: Optional[TeamResolver] = None,
) -> Tuple[
    List[WorkItemMetricsDailyRecord],
    List[WorkItemUserMetricsDailyRecord],
    List[WorkItemCycleTimeRecord],
]:
    """
    Compute work tracking metrics for a single UTC day.

    Inputs must be WorkItems with:
    - created_at, updated_at always set
    - started_at/completed_at best-effort derived (may be None)

    Null behavior:
    - cycle-time percentiles ignore items missing started_at or completed_at
    - WIP metrics ignore items missing started_at
    """
    start, end = _utc_day_window(day)
    computed_at_utc = _to_utc(computed_at)

    # Aggregations keyed by (provider, work_scope_id, team_id).
    by_group: Dict[Tuple[str, str, Optional[str]], Dict[str, object]] = {}
    by_user: Dict[Tuple[str, str, str, Optional[str]], Dict[str, object]] = {}

    cycle_time_records: List[WorkItemCycleTimeRecord] = []

    for item in work_items:
        work_scope_id = item.work_scope_id or ""
        created_at = _to_utc(item.created_at)
        started_at = _to_utc(item.started_at) if item.started_at else None
        completed_at = _to_utc(item.completed_at) if item.completed_at else None

        # Ignore items that don't exist yet on this day.
        if created_at >= end:
            continue

        assignee = item.assignees[0] if item.assignees else None
        team_id, team_name = _resolve_team(team_resolver, assignee)
        team_id_norm = team_id or ""
        team_name_norm = team_name or ""

        started_today = started_at is not None and start <= started_at < end
        completed_today = completed_at is not None and start <= completed_at < end
        wip_end_of_day = started_at is not None and started_at < end and (completed_at is None or completed_at >= end)

        # Only emit a bucket for groups/users that have activity for this day.
        if not (started_today or completed_today or wip_end_of_day):
            continue

        group_key = (item.provider, work_scope_id, team_id_norm)
        bucket = by_group.get(group_key)
        if bucket is None:
            bucket = {
                "team_name": team_name_norm,
                "items_started": 0,
                "items_completed": 0,
                "items_started_unassigned": 0,
                "items_completed_unassigned": 0,
                "wip_count": 0,
                "wip_unassigned": 0,
                "cycle_hours": [],
                "lead_hours": [],
                "wip_age_hours": [],
                "bug_completed": 0,
                "story_points_completed": 0.0,
            }
            by_group[group_key] = bucket

        user_identity = assignee or "unassigned"
        # User bucket (primary assignee or 'unassigned').
        if user_identity:
            user_key = (item.provider, work_scope_id, user_identity, team_id_norm)
            ub = by_user.get(user_key)
            if ub is None:
                ub = {
                    "team_name": team_name_norm,
                    "items_started": 0,
                    "items_completed": 0,
                    "wip_count": 0,
                    "cycle_hours": [],
                }
                by_user[user_key] = ub
        else:
            user_key = None
            ub = None

        # Started today.
        if started_today:
            bucket["items_started"] = int(bucket["items_started"]) + 1
            if assignee is None:
                bucket["items_started_unassigned"] = int(bucket["items_started_unassigned"]) + 1
            if ub is not None:
                ub["items_started"] = int(ub["items_started"]) + 1

        # Completed today.
        if completed_today:
            bucket["items_completed"] = int(bucket["items_completed"]) + 1
            if assignee is None:
                bucket["items_completed_unassigned"] = int(bucket["items_completed_unassigned"]) + 1
            if item.type == "bug":
                bucket["bug_completed"] = int(bucket["bug_completed"]) + 1
            if item.story_points is not None:
                try:
                    bucket["story_points_completed"] = float(bucket["story_points_completed"]) + float(item.story_points)
                except Exception:
                    # Ignore invalid story_points values for this work item but log for diagnostics.
                    logging.getLogger(__name__).warning(
                        "Failed to convert story_points for work item %s: %r",
                        getattr(item, "work_item_id", None),
                        item.story_points,
                    )

            if ub is not None:
                ub["items_completed"] = int(ub["items_completed"]) + 1

            lead_hours = (completed_at - created_at).total_seconds() / 3600.0
            bucket["lead_hours"].append(float(lead_hours))

            cycle_hours = None
            if started_at is not None:
                cycle_hours = (completed_at - started_at).total_seconds() / 3600.0
                bucket["cycle_hours"].append(float(cycle_hours))
                if ub is not None:
                    ub["cycle_hours"].append(float(cycle_hours))

            cycle_time_records.append(
                WorkItemCycleTimeRecord(
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    day=completed_at.date(),
                    work_scope_id=work_scope_id,
                    team_id=team_id_norm,
                    team_name=team_name_norm,
                    assignee=assignee,
                    type=item.type,
                    status=item.status,
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=float(cycle_hours) if cycle_hours is not None else None,
                    lead_time_hours=float(lead_hours),
                    computed_at=computed_at_utc,
                )
            )

        # WIP end-of-day (started but not completed by end).
        if wip_end_of_day:
            bucket["wip_count"] = int(bucket["wip_count"]) + 1
            if assignee is None:
                bucket["wip_unassigned"] = int(bucket["wip_unassigned"]) + 1
            age_hours = (end - started_at).total_seconds() / 3600.0
            bucket["wip_age_hours"].append(float(age_hours))
            if ub is not None:
                ub["wip_count"] = int(ub["wip_count"]) + 1

    group_records: List[WorkItemMetricsDailyRecord] = []
    for (provider, work_scope_id, team_id), bucket in sorted(
        by_group.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2] or ""))
    ):
        items_completed = int(bucket["items_completed"])
        bug_completed = int(bucket["bug_completed"])
        bug_ratio = (bug_completed / items_completed) if items_completed else 0.0
        cycle_hours: List[float] = list(bucket["cycle_hours"])
        lead_hours: List[float] = list(bucket["lead_hours"])
        wip_ages: List[float] = list(bucket["wip_age_hours"])

        group_records.append(
            WorkItemMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                team_id=team_id or "",
                team_name=bucket.get("team_name"),  # type: ignore[arg-type]
                items_started=int(bucket["items_started"]),
                items_completed=items_completed,
                items_started_unassigned=int(bucket["items_started_unassigned"]),
                items_completed_unassigned=int(bucket["items_completed_unassigned"]),
                wip_count_end_of_day=int(bucket["wip_count"]),
                wip_unassigned_end_of_day=int(bucket["wip_unassigned"]),
                cycle_time_p50_hours=float(_percentile(cycle_hours, 50.0)) if cycle_hours else None,
                cycle_time_p90_hours=float(_percentile(cycle_hours, 90.0)) if cycle_hours else None,
                lead_time_p50_hours=float(_percentile(lead_hours, 50.0)) if lead_hours else None,
                lead_time_p90_hours=float(_percentile(lead_hours, 90.0)) if lead_hours else None,
                wip_age_p50_hours=float(_percentile(wip_ages, 50.0)) if wip_ages else None,
                wip_age_p90_hours=float(_percentile(wip_ages, 90.0)) if wip_ages else None,
                bug_completed_ratio=float(bug_ratio),
                story_points_completed=float(bucket["story_points_completed"]),
                computed_at=computed_at_utc,
            )
        )

    user_records: List[WorkItemUserMetricsDailyRecord] = []
    for (provider, work_scope_id, user_identity, team_id), bucket in sorted(
        by_user.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], str(kv[0][3] or ""))
    ):
        cycle_hours: List[float] = list(bucket["cycle_hours"])
        user_records.append(
            WorkItemUserMetricsDailyRecord(
                day=day,
                provider=provider,
                work_scope_id=work_scope_id,
                user_identity=user_identity,
                team_id=team_id or "",
                team_name=bucket.get("team_name"),  # type: ignore[arg-type]
                items_started=int(bucket["items_started"]),
                items_completed=int(bucket["items_completed"]),
                wip_count_end_of_day=int(bucket["wip_count"]),
                cycle_time_p50_hours=float(_percentile(cycle_hours, 50.0)) if cycle_hours else None,
                cycle_time_p90_hours=float(_percentile(cycle_hours, 90.0)) if cycle_hours else None,
                computed_at=computed_at_utc,
            )
        )

    return group_records, user_records, cycle_time_records
