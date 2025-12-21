import math
import uuid
from datetime import date, datetime, timezone

from metrics.hotspots import compute_file_hotspots
from metrics.schemas import CommitStatRow


def test_file_hotspots_formula():
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    computed_at = datetime(2025, 2, 2, tzinfo=timezone.utc)

    # Formula parameters
    alpha, beta, gamma = 0.4, 0.3, 0.3

    # Scenario: 1 file, 2 commits, 2 authors, total churn 100
    # churn = 100
    # contributors = 2
    # commits = 2
    # expected = 0.4 * log(101) + 0.3 * 2 + 0.3 * 2
    #          = 0.4 * 4.615... + 0.6 + 0.6
    #          = 1.846 + 1.2 = 3.046...

    stats: list[CommitStatRow] = [
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "file_path": "monitor.py",
            "additions": 60,
            "deletions": 20,
            "author_email": "alice@example.com",
            "author_name": "Alice",
            "committer_when": computed_at,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
        {
            "repo_id": repo_id,
            "commit_hash": "c2",
            "file_path": "monitor.py",
            "additions": 10,
            "deletions": 10,
            "author_email": "bob@example.com",
            "author_name": "Bob",
            "committer_when": computed_at,
            "old_file_mode": "100644",
            "new_file_mode": "100644",
        },
    ]

    results = compute_file_hotspots(
        repo_id=repo_id,
        day=day,
        window_stats=stats,
        computed_at=computed_at,
    )

    assert len(results) == 1
    rec = results[0]
    assert rec.path == "monitor.py"
    assert rec.churn == 100
    assert rec.contributors == 2
    assert rec.commits_count == 2

    expected_score = (alpha * math.log1p(100)) + (beta * 2) + (gamma * 2)
    assert abs(rec.hotspot_score - expected_score) < 1e-6


def test_file_hotspots_multiple_files_sorting():
    repo_id = uuid.uuid4()
    day = date(2025, 2, 1)
    computed_at = datetime(2025, 2, 2, tzinfo=timezone.utc)

    stats: list[CommitStatRow] = [
        # active.py: high churn, many authors
        {
            "repo_id": repo_id,
            "commit_hash": "c1",
            "file_path": "active.py",
            "additions": 1000,
            "deletions": 0,
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": computed_at,
            "old_file_mode": "",
            "new_file_mode": "",
        },
        {
            "repo_id": repo_id,
            "commit_hash": "c2",
            "file_path": "active.py",
            "additions": 0,
            "deletions": 0,
            "author_email": "b@ex.com",
            "author_name": "B",
            "committer_when": computed_at,
            "old_file_mode": "",
            "new_file_mode": "",
        },
        # quiet.py: low churn
        {
            "repo_id": repo_id,
            "commit_hash": "c3",
            "file_path": "quiet.py",
            "additions": 1,
            "deletions": 0,
            "author_email": "a@ex.com",
            "author_name": "A",
            "committer_when": computed_at,
            "old_file_mode": "",
            "new_file_mode": "",
        },
    ]

    results = compute_file_hotspots(
        repo_id=repo_id,
        day=day,
        window_stats=stats,
        computed_at=computed_at,
    )

    assert len(results) == 2
    assert results[0].path == "active.py"
    assert results[1].path == "quiet.py"
    assert results[0].hotspot_score > results[1].hotspot_score
