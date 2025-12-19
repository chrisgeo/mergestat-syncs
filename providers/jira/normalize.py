from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from models.work_items import WorkItem, WorkItemStatusTransition
from providers.identity import IdentityResolver
from providers.status_mapping import StatusMapping


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        # Jira commonly uses "+0000" offsets (no colon); normalize for fromisoformat.
        raw = raw.replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", raw):
            raw = raw[:-2] + ":" + raw[-2:]
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_field(issue: Any, field_name: str) -> Any:
    if isinstance(issue, dict):
        fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else None
        if fields is None:
            return None
        return fields.get(field_name)
    fields = getattr(issue, "fields", None)
    if fields is None:
        return None
    return getattr(fields, field_name, None)


def _parse_sprint(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Jira sprint fields vary by instance. Best-effort parsing:
    - list of strings with "id=...,name=..."
    - list of dict-like objects
    - single string
    """
    if not value:
        return None, None
    sprint = None
    if isinstance(value, list):
        sprint = value[-1] if value else None
    else:
        sprint = value

    if isinstance(sprint, dict):
        sid = sprint.get("id")
        name = sprint.get("name")
        return str(sid) if sid is not None else None, str(name) if name else None

    raw = str(sprint)
    # Typical Jira string contains "id=123,name=Sprint 1,".
    sid_match = re.search(r"\bid=(\d+)\b", raw)
    name_match = re.search(r"\bname=([^,\\]]+)", raw)
    sid = sid_match.group(1) if sid_match else None
    name = name_match.group(1).strip() if name_match else None
    return sid, name


def jira_issue_to_work_item(
    *,
    issue: Any,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    repo_id: Optional[Any] = None,
    story_points_field: Optional[str] = None,
    sprint_field: Optional[str] = None,
    epic_link_field: Optional[str] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    """
    Normalize a Jira issue into a WorkItem and status transitions.

    `story_points_field`, `sprint_field`, and `epic_link_field` are instance-specific.
    If not provided, environment variables are used:
    - JIRA_STORY_POINTS_FIELD
    - JIRA_SPRINT_FIELD
    - JIRA_EPIC_LINK_FIELD
    """
    story_points_field = story_points_field or os.getenv("JIRA_STORY_POINTS_FIELD")
    sprint_field = sprint_field or os.getenv("JIRA_SPRINT_FIELD") or "customfield_10020"
    epic_link_field = epic_link_field or os.getenv("JIRA_EPIC_LINK_FIELD")

    key = (issue.get("key") if isinstance(issue, dict) else getattr(issue, "key", None)) or ""
    work_item_id = f"jira:{key}"

    project = _get_field(issue, "project")
    if isinstance(project, dict):
        project_key = project.get("key")
        project_id = project.get("id")
    else:
        project_key = getattr(project, "key", None) if project else None
        project_id = getattr(project, "id", None) if project else None

    title = _get_field(issue, "summary") or ""

    status_obj = _get_field(issue, "status")
    if isinstance(status_obj, dict):
        status_raw = status_obj.get("name")
        status_category_key = ((status_obj.get("statusCategory") or {}) if isinstance(status_obj.get("statusCategory"), dict) else {}).get("key")
    else:
        status_raw = getattr(status_obj, "name", None) if status_obj else None
        status_category_key = getattr(getattr(status_obj, "statusCategory", None), "key", None) if status_obj else None

    issue_type_obj = _get_field(issue, "issuetype")
    if isinstance(issue_type_obj, dict):
        type_raw = issue_type_obj.get("name")
    else:
        type_raw = getattr(issue_type_obj, "name", None) if issue_type_obj else None

    labels = list(_get_field(issue, "labels") or [])

    normalized_status = status_mapping.normalize_status(
        provider="jira",
        status_raw=status_raw,
        labels=labels,
    )
    # Jira statusCategory=done is a strong hint that the issue is completed even if the status name is custom.
    if normalized_status in {"unknown", "todo", "in_progress", "in_review", "blocked", "backlog"}:
        if str(status_category_key or "").lower() == "done":
            normalized_status = "done"
    normalized_type = status_mapping.normalize_type(
        provider="jira",
        type_raw=type_raw,
        labels=labels,
    )

    assignees: List[str] = []
    assignee_obj = _get_field(issue, "assignee")
    if assignee_obj is not None:
        assignees.append(
            identity.resolve(
                provider="jira",
                email=getattr(assignee_obj, "emailAddress", None),
                account_id=getattr(assignee_obj, "accountId", None),
                display_name=getattr(assignee_obj, "displayName", None),
            )
        )

    reporter_obj = _get_field(issue, "reporter")
    reporter = None
    if reporter_obj is not None:
        reporter = identity.resolve(
            provider="jira",
            email=getattr(reporter_obj, "emailAddress", None),
            account_id=getattr(reporter_obj, "accountId", None),
            display_name=getattr(reporter_obj, "displayName", None),
        )

    created_at = _parse_datetime(_get_field(issue, "created")) or datetime.now(timezone.utc)
    updated_at = _parse_datetime(_get_field(issue, "updated")) or created_at
    closed_at = _parse_datetime(_get_field(issue, "resolutiondate"))

    url = None
    if isinstance(issue, dict):
        url = issue.get("self")
    elif hasattr(issue, "self"):
        url = getattr(issue, "self", None)

    story_points = None
    if story_points_field:
        raw_points = _get_field(issue, story_points_field)
        try:
            story_points = float(raw_points) if raw_points is not None else None
        except Exception:
            story_points = None

    sprint_id = None
    sprint_name = None
    if sprint_field:
        sprint_id, sprint_name = _parse_sprint(_get_field(issue, sprint_field))

    parent_id = None
    parent_obj = _get_field(issue, "parent")
    if parent_obj is not None:
        parent_id = f"jira:{getattr(parent_obj, 'key', None) or ''}" or None

    epic_id = None
    if epic_link_field:
        epic_val = _get_field(issue, epic_link_field)
        if epic_val:
            epic_id = f"jira:{str(epic_val)}"

    # Changelog transitions for started/completed derivation.
    transitions: List[WorkItemStatusTransition] = []
    if isinstance(issue, dict):
        changelog = issue.get("changelog") if isinstance(issue.get("changelog"), dict) else None
        histories = changelog.get("histories") if changelog else None
    else:
        changelog = getattr(issue, "changelog", None)
        histories = getattr(changelog, "histories", None) if changelog else None
    if histories:
        # Jira returns newest-first sometimes; sort by created timestamp.
        def _hist_dt(h: Any) -> datetime:
            created = h.get("created") if isinstance(h, dict) else getattr(h, "created", None)
            return _parse_datetime(created) or datetime.min.replace(
                tzinfo=timezone.utc
            )

        for hist in sorted(list(histories), key=_hist_dt):
            occurred_at = _parse_datetime(hist.get("created") if isinstance(hist, dict) else getattr(hist, "created", None)) or created_at
            author_obj = hist.get("author") if isinstance(hist, dict) else getattr(hist, "author", None)
            actor = None
            if author_obj is not None:
                actor = identity.resolve(
                    provider="jira",
                    email=author_obj.get("emailAddress") if isinstance(author_obj, dict) else getattr(author_obj, "emailAddress", None),
                    account_id=author_obj.get("accountId") if isinstance(author_obj, dict) else getattr(author_obj, "accountId", None),
                    display_name=author_obj.get("displayName") if isinstance(author_obj, dict) else getattr(author_obj, "displayName", None),
                )
            items = hist.get("items") if isinstance(hist, dict) else getattr(hist, "items", None)
            for item in items or []:
                field_name = item.get("field") if isinstance(item, dict) else getattr(item, "field", "")
                if str(field_name or "").lower() != "status":
                    continue
                from_raw = item.get("fromString") if isinstance(item, dict) else getattr(item, "fromString", None)
                to_raw = item.get("toString") if isinstance(item, dict) else getattr(item, "toString", None)
                from_norm = status_mapping.normalize_status(
                    provider="jira", status_raw=from_raw, labels=labels
                )
                to_norm = status_mapping.normalize_status(
                    provider="jira", status_raw=to_raw, labels=labels
                )
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="jira",
                        occurred_at=occurred_at,
                        from_status_raw=from_raw,
                        to_status_raw=to_raw,
                        from_status=from_norm,
                        to_status=to_norm,
                        actor=actor,
                    )
                )

    started_at = None
    completed_at = None
    for t in transitions:
        if started_at is None and t.to_status == "in_progress":
            started_at = t.occurred_at
        if completed_at is None and t.to_status in {"done", "canceled"}:
            completed_at = t.occurred_at
            break

    # Fallback for closed issues with no changelog.
    if completed_at is None and normalized_status in {"done", "canceled"}:
        completed_at = closed_at or updated_at

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="jira",
        repo_id=repo_id,
        project_key=str(project_key) if project_key else None,
        project_id=str(project_id) if project_id else None,
        title=str(title),
        type=normalized_type,
        status=normalized_status,
        status_raw=str(status_raw) if status_raw else None,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=[str(l) for l in labels if l],
        story_points=story_points,
        sprint_id=sprint_id,
        sprint_name=sprint_name,
        parent_id=parent_id,
        epic_id=epic_id,
        url=url,
    )
    return work_item, transitions
