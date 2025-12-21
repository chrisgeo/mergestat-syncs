from __future__ import annotations

import math
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Sequence

from metrics.schemas import CommitStatRow, FileMetricsRecord


def compute_file_hotspots(
    *,
    repo_id: uuid.UUID,
    day: date,
    window_stats: Sequence[CommitStatRow],
    computed_at: datetime,
) -> List[FileMetricsRecord]:
    """
    Compute file hotspot scores based on a window of commit stats.

    Formula (from docs 1.3.2):
    hotspot_raw = α*log(1 + churn_f) + β*contributors_f + γ*commit_count_f
    Weights: α=0.4, β=0.3, γ=0.3
    """
    file_map: Dict[str, Dict[str, Any]] = {}

    for row in window_stats:
        if row["repo_id"] != repo_id:
            continue

        path = row.get("file_path")
        if not path:
            continue

        if path not in file_map:
            file_map[path] = {
                "churn": 0,
                "authors": set(),
                "commits": set(),
            }

        stats = file_map[path]
        additions = max(0, int(row.get("additions") or 0))
        deletions = max(0, int(row.get("deletions") or 0))
        stats["churn"] += additions + deletions

        author = (
            row.get("author_email") or row.get("author_name") or "unknown"
        ).strip()
        stats["authors"].add(author)
        stats["commits"].add(row["commit_hash"])

    records: List[FileMetricsRecord] = []
    alpha, beta, gamma = 0.4, 0.3, 0.3

    for path, stats in file_map.items():
        churn = stats["churn"]
        contributors = len(stats["authors"])
        commits_count = len(stats["commits"])

        # Formula from docs 1.3.2
        hotspot_score = (
            (alpha * math.log1p(churn))
            + (beta * contributors)
            + (gamma * commits_count)
        )

        records.append(
            FileMetricsRecord(
                repo_id=repo_id,
                day=day,
                path=path,
                churn=churn,
                contributors=contributors,
                commits_count=commits_count,
                hotspot_score=float(hotspot_score),
                computed_at=computed_at,
            )
        )

    # Sort by hotspot score descending
    return sorted(records, key=lambda r: r.hotspot_score, reverse=True)
