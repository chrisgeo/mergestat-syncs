from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional, Sequence, Tuple

from models.work_items import WorkItem, WorkItemStatusTransition
from providers.identity import IdentityResolver
from providers.status_mapping import StatusMapping


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def gitlab_issue_to_work_item(
    *,
    issue: Any,
    project_full_path: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    label_events: Optional[Sequence[Any]] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    iid = int(_get(issue, "iid") or 0)
    work_item_id = f"gitlab:{project_full_path}#{iid}"

    title = _get(issue, "title") or ""
    state = _get(issue, "state") or None  # opened/closed
    created_at = _to_utc(_parse_iso(_get(issue, "created_at"))) or datetime.now(timezone.utc)
    updated_at = _to_utc(_parse_iso(_get(issue, "updated_at"))) or created_at
    closed_at = _to_utc(_parse_iso(_get(issue, "closed_at")))

    labels = list(_get(issue, "labels") or [])
    labels = [str(l) for l in labels if l]

    normalized_status = status_mapping.normalize_status(
        provider="gitlab",
        status_raw=None,
        labels=labels,
        state=str(state) if state else None,
    )
    normalized_type = status_mapping.normalize_type(
        provider="gitlab",
        type_raw=None,
        labels=labels,
    )

    assignees: List[str] = []
    for a in _get(issue, "assignees") or []:
        assignees.append(
            identity.resolve(
                provider="gitlab",
                email=_get(a, "email"),
                username=_get(a, "username"),
                display_name=_get(a, "name"),
            )
        )

    author_obj = _get(issue, "author")
    reporter = None
    if author_obj is not None:
        reporter = identity.resolve(
            provider="gitlab",
            email=_get(author_obj, "email"),
            username=_get(author_obj, "username"),
            display_name=_get(author_obj, "name"),
        )

    url = _get(issue, "web_url") or _get(issue, "url")

    # Best-effort transitions from label events + state.
    transitions: List[WorkItemStatusTransition] = []
    started_at = None
    completed_at = None

    if label_events:
        def _ev_dt(ev: Any) -> datetime:
            return _to_utc(_parse_iso(_get(ev, "created_at"))) or datetime.min.replace(tzinfo=timezone.utc)

        prev_status = "unknown"
        for ev in sorted(list(label_events), key=_ev_dt):
            action = str(_get(ev, "action") or "").lower()
            label = _get(ev, "label") or {}
            label_name = _get(label, "name") or _get(ev, "label_name")
            if not label_name:
                continue
            label_name = str(label_name)
            occurred_at = _to_utc(_parse_iso(_get(ev, "created_at"))) or created_at

            if action not in {"add", "remove"}:
                continue
            mapped = status_mapping.normalize_status(
                provider="gitlab",
                status_raw=None,
                labels=[label_name] if action == "add" else (),
                state=None,
            )
            if mapped == "unknown":
                continue
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="gitlab",
                    occurred_at=occurred_at,
                    from_status_raw=None,
                    to_status_raw=label_name,
                    from_status=prev_status,  # type: ignore[arg-type]
                    to_status=mapped,
                    actor=None,
                )
            )
            prev_status = mapped

        for t in transitions:
            if started_at is None and t.to_status == "in_progress":
                started_at = t.occurred_at
            if completed_at is None and t.to_status in {"done", "canceled"}:
                completed_at = t.occurred_at
                break

    if completed_at is None and closed_at is not None:
        completed_at = closed_at

    weight = _get(issue, "weight")
    story_points = float(weight) if weight is not None else None

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="gitlab",
        repo_id=repo_id,
        project_key=None,
        # For work tracking metrics, treat the GitLab project path as the "project" scope.
        project_id=str(project_full_path) if project_full_path else (str(_get(issue, "project_id")) if _get(issue, "project_id") else None),
        title=str(title),
        type=normalized_type,
        status=normalized_status,
        status_raw=str(state) if state else None,
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=labels,
        story_points=story_points,
        sprint_id=str(_get(_get(issue, "milestone"), "id")) if _get(issue, "milestone") else None,
        sprint_name=_get(_get(issue, "milestone"), "title") if _get(issue, "milestone") else None,
        url=url,
    )
    return work_item, transitions
