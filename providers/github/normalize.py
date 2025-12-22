from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
import logging

from models.work_items import WorkItem, WorkItemStatusTransition
from providers.identity import IdentityResolver
from providers.status_mapping import StatusMapping


def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _labels_from_nodes(nodes: Any) -> List[str]:
    labels: List[str] = []
    for node in nodes or []:
        name = (node or {}).get("name") if isinstance(node, dict) else getattr(node, "name", None)
        if name:
            labels.append(str(name))
    return labels


def github_issue_to_work_item(
    *,
    issue: Any,
    repo_full_name: str,
    repo_id: Optional[Any],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    events: Optional[Sequence[Any]] = None,
    project_status_raw: Optional[str] = None,
) -> Tuple[WorkItem, List[WorkItemStatusTransition]]:
    number = int(getattr(issue, "number", 0) or 0)
    work_item_id = f"gh:{repo_full_name}#{number}"

    title = getattr(issue, "title", "") or ""
    state = getattr(issue, "state", None)
    created_at = _to_utc(getattr(issue, "created_at", None)) or datetime.now(timezone.utc)
    updated_at = _to_utc(getattr(issue, "updated_at", None)) or created_at
    closed_at = _to_utc(getattr(issue, "closed_at", None))

    labels = [getattr(l, "name", None) for l in getattr(issue, "labels", []) or []]
    labels = [str(l) for l in labels if l]

    # If the issue is in a Project with a status field, prefer that as status_raw.
    status_raw = project_status_raw
    normalized_status = status_mapping.normalize_status(
        provider="github",
        status_raw=status_raw,
        labels=() if status_raw else labels,
        state=str(state) if state else None,
    )
    normalized_type = status_mapping.normalize_type(
        provider="github",
        type_raw=None,
        labels=labels,
    )

    assignees: List[str] = []
    for a in getattr(issue, "assignees", []) or []:
        assignees.append(
            identity.resolve(
                provider="github",
                email=getattr(a, "email", None),
                username=getattr(a, "login", None),
                display_name=getattr(a, "name", None),
            )
        )

    reporter_obj = getattr(issue, "user", None)
    reporter = None
    if reporter_obj is not None:
        reporter = identity.resolve(
            provider="github",
            email=getattr(reporter_obj, "email", None),
            username=getattr(reporter_obj, "login", None),
            display_name=getattr(reporter_obj, "name", None),
        )

    url = getattr(issue, "html_url", None) or getattr(issue, "url", None)

    # Best-effort transitions from issue events (label add/remove, closed/reopened).
    transitions: List[WorkItemStatusTransition] = []
    started_at = None
    completed_at = None
    if events:
        # Events are returned newest-first by PyGithub; sort by created_at.
        def _ev_dt(ev: Any) -> datetime:
            return _to_utc(getattr(ev, "created_at", None)) or datetime.min.replace(tzinfo=timezone.utc)

        prev_status = "unknown"
        for ev in sorted(list(events), key=_ev_dt):
            event_type = str(getattr(ev, "event", "") or "").lower()
            occurred_at = _to_utc(getattr(ev, "created_at", None)) or created_at

            if event_type in {"closed", "reopened"}:
                to_status = "done" if event_type == "closed" else "todo"
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=work_item_id,
                        provider="github",
                        occurred_at=occurred_at,
                        from_status_raw=None,
                        to_status_raw=event_type,
                        from_status=prev_status,  # type: ignore[arg-type]
                        to_status=to_status,  # type: ignore[arg-type]
                        actor=None,
                    )
                )
                prev_status = to_status  # type: ignore[assignment]
                continue

            if event_type not in {"labeled", "unlabeled"}:
                continue

            label_obj = getattr(ev, "label", None)
            label_name = getattr(label_obj, "name", None)
            if not label_name:
                continue
            label_name = str(label_name)

            mapped = status_mapping.normalize_status(
                provider="github",
                status_raw=None,
                labels=[label_name] if event_type == "labeled" else (),
                state=None,
            )
            if mapped == "unknown":
                continue

            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=work_item_id,
                    provider="github",
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

    # Fallback: closed_at implies done.
    if completed_at is None and closed_at is not None:
        completed_at = closed_at

    work_item = WorkItem(
        work_item_id=work_item_id,
        provider="github",
        repo_id=repo_id,
        project_key=None,
        # For work tracking metrics, treat the repo as the "project" scope.
        project_id=str(repo_full_name) if repo_full_name else None,
        title=str(title),
        type=normalized_type,
        status=normalized_status,
        status_raw=str(status_raw) if status_raw else (str(state) if state else None),
        assignees=[a for a in assignees if a and a != "unknown"],
        reporter=reporter if reporter and reporter != "unknown" else None,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        closed_at=closed_at,
        labels=labels,
        url=url,
    )

    return work_item, transitions


def github_project_v2_item_to_work_item(
    *,
    item_node: Dict[str, Any],
    project_scope_id: Optional[str] = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
) -> Optional[WorkItem]:
    """
    Normalize a Projects v2 item node into a WorkItem.

    This is best-effort and intentionally does not attempt to reconstruct status history.
    """
    content = item_node.get("content") or {}
    typename = content.get("__typename")

    # Extract a status, iteration, and estimate values from field values.
    status_raw = None
    iteration_title = None
    iteration_id = None
    estimate = None
    
    for fv in (item_node.get("fieldValues") or {}).get("nodes") or []:
        typename = (fv or {}).get("__typename")
        field = (fv or {}).get("field") or {}
        field_name = str(field.get("name") or "").strip().lower()

        if typename == "ProjectV2ItemFieldSingleSelectValue":
            if field_name == "status":
                status_raw = fv.get("name")
        
        elif typename == "ProjectV2ItemFieldIterationValue":
            # GitHub Iterations
            if "iteration" in field_name or "sprint" in field_name:
                iteration_title = fv.get("title")
                iteration_id = fv.get("id") # internal node id
        
        elif typename == "ProjectV2ItemFieldNumberValue":
            # Estimates / Points
            if field_name in {"estimate", "points", "story points", "size"}:
                try:
                    estimate = float(fv.get("number") or 0)
                except (ValueError, TypeError):
                    logging.getLogger(__name__).debug(
                        "Failed to parse numeric estimate from value %r", fv.get("number")
                    )

    if typename == "Issue":
        repo_full_name = ((content.get("repository") or {}).get("nameWithOwner")) or ""
        number = int(content.get("number") or 0)
        work_item_id = f"gh:{repo_full_name}#{number}" if repo_full_name and number else f"ghproj:{item_node.get('id')}"
        labels = _labels_from_nodes(((content.get("labels") or {}).get("nodes")) or [])
        assignees = []
        for a in ((content.get("assignees") or {}).get("nodes")) or []:
            assignees.append(
                identity.resolve(
                    provider="github",
                    email=a.get("email"),
                    username=a.get("login"),
                    display_name=a.get("name"),
                )
            )
        author = content.get("author") or {}
        reporter = identity.resolve(
            provider="github",
            email=author.get("email"),
            username=author.get("login"),
            display_name=author.get("name"),
        )
        created_at = _to_utc(_parse_iso(content.get("createdAt"))) or datetime.now(timezone.utc)
        updated_at = _to_utc(_parse_iso(content.get("updatedAt"))) or created_at
        closed_at = _to_utc(_parse_iso(content.get("closedAt")))
        state = content.get("state")

        normalized_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(status_raw) if status_raw else None,
            labels=() if status_raw else labels,
            state=str(state) if state else None,
        )
        normalized_type = status_mapping.normalize_type(
            provider="github",
            type_raw=None,
            labels=labels,
        )
        completed_at = closed_at if closed_at else None

        return WorkItem(
            work_item_id=work_item_id,
            provider="github",
            repo_id=None,
            project_key=None,
            project_id=str(project_scope_id or repo_full_name) if (project_scope_id or repo_full_name) else None,
            title=str(content.get("title") or ""),
            type=normalized_type,
            status=normalized_status,
            status_raw=str(status_raw) if status_raw else (str(state) if state else None),
            assignees=[a for a in assignees if a and a != "unknown"],
            reporter=reporter if reporter and reporter != "unknown" else None,
            created_at=created_at,
            updated_at=updated_at,
            started_at=None,
            completed_at=completed_at,
            closed_at=closed_at,
            labels=labels,
            story_points=estimate,
            sprint_id=iteration_id,
            sprint_name=iteration_title,
            url=content.get("url"),
        )

    if typename == "DraftIssue":
        created_at = _to_utc(_parse_iso(content.get("createdAt"))) or datetime.now(timezone.utc)
        updated_at = _to_utc(_parse_iso(content.get("updatedAt"))) or created_at
        normalized_status = status_mapping.normalize_status(
            provider="github",
            status_raw=str(status_raw) if status_raw else None,
            labels=(),
            state=None,
        )
        return WorkItem(
            work_item_id=f"ghproj:{item_node.get('id')}",
            provider="github",
            repo_id=None,
            project_key=None,
            project_id=str(project_scope_id) if project_scope_id else None,
            title=str(content.get("title") or ""),
            type="issue",
            status=normalized_status,
            status_raw=str(status_raw) if status_raw else None,
            assignees=[],
            reporter=None,
            created_at=created_at,
            updated_at=updated_at,
            started_at=None,
            completed_at=None,
            closed_at=None,
            labels=[],
            story_points=estimate,
            sprint_id=iteration_id,
            sprint_name=iteration_title,
            url=None,
        )

    return None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # GitHub returns RFC3339 with Z.
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
