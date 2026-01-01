from __future__ import annotations

from datetime import datetime, timezone

import pytest

from models.work_items import WorkItemStatusTransition
from providers.identity import IdentityResolver
from providers.jira.normalize import (
    _normalize_relationship_type,
    _service_class_from_priority,
    detect_reopen_events,
    extract_jira_issue_dependencies,
    jira_comment_to_interaction_event,
    jira_sprint_payload_to_model,
)


@pytest.mark.parametrize(
    ("priority_raw", "expected"),
    [
        ("Highest", "expedite"),
        ("Critical", "expedite"),
        ("Blocker", "expedite"),
        ("Urgent", "expedite"),
        ("P0", "expedite"),
        ("P1", "expedite"),
        ("Low", "background"),
        ("Lowest", "background"),
        ("P4", "background"),
        ("P5", "background"),
        ("Medium", "standard"),
        (None, "standard"),
        # False positive prevention tests
        ("P10", "standard"),  # Should not match "P1"
        ("P100", "standard"),  # Should not match "P1" or "P0"
        ("Below", "standard"),  # Should not match "low"
        ("Following", "standard"),  # Should not match "low"
        ("P3", "standard"),  # Not in any marker list
    ],
)
def test_service_class_from_priority(priority_raw: str | None, expected: str) -> None:
    assert _service_class_from_priority(priority_raw) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("blocks", "blocks"),
        ("Blocks", "blocks"),
        ("is blocked by", "blocked_by"),
        ("blocked by", "blocked_by"),
        ("relates to", "relates"),
        ("duplicates", "duplicates"),
        ("other", "other"),
        (None, "other"),
        # False positive prevention tests
        ("blocker", "other"),  # Should not match "blocks"
        ("blocking", "other"),  # Should not match "blocks"
        ("blocks all", "blocks"),  # Should match "blocks" with word boundary
        ("is blocked by all", "blocked_by"),  # Should match "blocked by"
    ],
)
def test_normalize_relationship_type(raw_value: str | None, expected: str) -> None:
    assert _normalize_relationship_type(raw_value) == expected


def test_extract_jira_issue_dependencies() -> None:
    issue = {
        "key": "ABC-1",
        "fields": {
            "issuelinks": [
                {
                    "type": {
                        "name": "Blocks",
                        "outward": "blocks",
                        "inward": "is blocked by",
                    },
                    "outwardIssue": {"key": "ABC-2"},
                },
                {
                    "type": {"name": "Relates", "outward": "relates to"},
                    "inwardIssue": {"key": "ABC-3"},
                },
                {
                    "type": {"name": "Duplicate", "outward": "duplicates"},
                    "outwardIssue": {"key": "ABC-4"},
                },
                {
                    "type": {"name": "Custom", "inward": "required by"},
                    "inwardIssue": {"key": "ABC-5"},
                },
            ]
        },
    }

    deps = extract_jira_issue_dependencies(issue=issue, work_item_id="jira:ABC-1")
    assert len(deps) == 4

    dep_map = {(d.source_work_item_id, d.target_work_item_id): d.relationship_type for d in deps}
    assert dep_map[("jira:ABC-1", "jira:ABC-2")] == "blocks"
    assert dep_map[("jira:ABC-3", "jira:ABC-1")] == "relates"
    assert dep_map[("jira:ABC-1", "jira:ABC-4")] == "duplicates"
    assert dep_map[("jira:ABC-5", "jira:ABC-1")] == "other"


def test_detect_reopen_events() -> None:
    transitions = [
        WorkItemStatusTransition(
            work_item_id="jira:ABC-1",
            provider="jira",
            occurred_at=datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc),
            from_status_raw="To Do",
            to_status_raw="Done",
            from_status="todo",
            to_status="done",
            actor="a",
        ),
        WorkItemStatusTransition(
            work_item_id="jira:ABC-1",
            provider="jira",
            occurred_at=datetime(2025, 1, 2, 9, 0, tzinfo=timezone.utc),
            from_status_raw="Done",
            to_status_raw="In Progress",
            from_status="done",
            to_status="in_progress",
            actor="b",
        ),
        WorkItemStatusTransition(
            work_item_id="jira:ABC-1",
            provider="jira",
            occurred_at=datetime(2025, 1, 3, 9, 0, tzinfo=timezone.utc),
            from_status_raw="Canceled",
            to_status_raw="To Do",
            from_status="canceled",
            to_status="todo",
            actor="c",
        ),
    ]

    events = detect_reopen_events(work_item_id="jira:ABC-1", transitions=transitions)
    assert len(events) == 2
    assert events[0].occurred_at == datetime(2025, 1, 2, 9, 0, tzinfo=timezone.utc)
    assert events[1].occurred_at == datetime(2025, 1, 3, 9, 0, tzinfo=timezone.utc)


def test_jira_comment_to_interaction_event() -> None:
    identity = IdentityResolver(alias_to_canonical={})
    comment = {
        "created": "2025-01-02T03:04:05.000+0000",
        "author": {"accountId": "acct-123", "displayName": "Alice"},
        "body": "Hello",
    }

    event = jira_comment_to_interaction_event(
        work_item_id="jira:ABC-1",
        comment=comment,
        identity=identity,
    )

    assert event is not None
    assert event.work_item_id == "jira:ABC-1"
    assert event.provider == "jira"
    assert event.interaction_type == "comment"
    assert event.body_length == 5
    assert event.actor == "Alice"


def test_jira_sprint_payload_to_model() -> None:
    payload = {
        "id": 123,
        "name": "Sprint 7",
        "state": "active",
        "startDate": "2025-02-01T10:00:00.000+0000",
        "endDate": "2025-02-15T10:00:00.000+0000",
        "completeDate": "2025-02-14T10:00:00.000+0000",
    }

    sprint = jira_sprint_payload_to_model(payload)
    assert sprint is not None
    assert sprint.provider == "jira"
    assert sprint.sprint_id == "123"
    assert sprint.name == "Sprint 7"
    assert sprint.state == "active"
    assert sprint.started_at == datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc)
    assert sprint.ended_at == datetime(2025, 2, 15, 10, 0, tzinfo=timezone.utc)
    assert sprint.completed_at == datetime(2025, 2, 14, 10, 0, tzinfo=timezone.utc)
