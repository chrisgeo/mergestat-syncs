from datetime import datetime, timezone

from dev_health_ops.api.models.schemas import (
    FlameFrame,
    FlameTimeline,
    HeatmapAxes,
    HeatmapCell,
    HeatmapLegend,
    HeatmapResponse,
)
from dev_health_ops.api.services.flame import validate_flame_frames
from dev_health_ops.api.services.heatmap import HEATMAP_METRICS, WEEKDAY_LABELS, _hour_labels


def test_heatmap_schema_shape():
    response = HeatmapResponse(
        axes=HeatmapAxes(x=["00"], y=["Mon"]),
        cells=[HeatmapCell(x="00", y="Mon", value=2.0)],
        legend=HeatmapLegend(unit="hours", scale="linear"),
        evidence=[{"id": "sample"}],
    )
    assert response.axes.x == ["00"]


def test_heatmap_no_person_matrix():
    assert all(
        not (metric.x_axis == "person" and metric.y_axis == "person")
        for metric in HEATMAP_METRICS
    )


def test_heatmap_axis_label_snapshot():
    assert WEEKDAY_LABELS == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    assert _hour_labels() == [f"{hour:02d}" for hour in range(24)]


def test_heatmap_legend_units_snapshot():
    expected = {
        "review_wait_density": "hours",
        "repo_touchpoints": "commits",
        "hotspot_risk": "hotspot score",
        "active_hours": "commits",
    }
    assert {metric.metric: metric.unit for metric in HEATMAP_METRICS} == expected


def test_flame_frames_cover_lifecycle():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    timeline = FlameTimeline(start=start, end=end)
    frames = [
        FlameFrame(
            id="root",
            parent_id=None,
            label="Lifecycle",
            start=start,
            end=end,
            state="active",
            category="planned",
        ),
        FlameFrame(
            id="child",
            parent_id="root",
            label="Phase",
            start=start,
            end=end,
            state="active",
            category="planned",
        ),
    ]
    assert validate_flame_frames(timeline, frames)


def test_flame_frames_gap_detection():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)
    timeline = FlameTimeline(start=start, end=end)
    frames = [
        FlameFrame(
            id="root-a",
            parent_id=None,
            label="Phase A",
            start=start,
            end=datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
            state="active",
            category="planned",
        ),
        FlameFrame(
            id="root-b",
            parent_id=None,
            label="Phase B",
            start=datetime(2024, 1, 1, 13, tzinfo=timezone.utc),
            end=end,
            state="active",
            category="planned",
        ),
    ]
    assert not validate_flame_frames(timeline, frames)
