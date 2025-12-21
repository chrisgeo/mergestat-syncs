import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from models.git import (
    Repo,
    GitCommit,
    GitCommitStat,
    GitPullRequest,
    GitPullRequestReview,
    GitFile,
    CiPipelineRun,
    Deployment,
    Incident,
)
from models.work_items import WorkItem, WorkItemStatusTransition, WorkItemType
from metrics.schemas import (
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    FileMetricsRecord,
)


class SyntheticDataGenerator:
    def __init__(
        self,
        repo_name: str = "acme/demo-app",
        repo_id: Optional[uuid.UUID] = None,
        provider: str = "synthetic",
    ):
        self.repo_name = repo_name
        if repo_id:
            self.repo_id = repo_id
        else:
            # Deterministic UUID based on repo name
            namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
            self.repo_id = uuid.uuid5(namespace, repo_name)
        self.provider = provider
        self.authors = [
            ("Alice Smith", "alice@example.com"),
            ("Bob Jones", "bob@example.com"),
            ("Charlie Brown", "charlie@example.com"),
            ("David White", "david@example.com"),
            ("Eve Black", "eve@example.com"),
        ]
        self.files = [
            "src/main.py",
            "src/utils.py",
            "src/models.py",
            "src/api/routes.py",
            "src/api/auth.py",
            "tests/test_main.py",
            "README.md",
            "docker-compose.yml",
            ".github/workflows/ci.yml",
        ]

    def generate_repo(self) -> Repo:
        return Repo(
            id=self.repo_id,
            repo=self.repo_name,
            ref="main",
            settings={
                "source": "synthetic",
                "repo_id": str(self.repo_id),
            },
            tags=["demo", "synthetic"],
        )

    def generate_commits(
        self, days: int = 30, commits_per_day: int = 5
    ) -> List[GitCommit]:
        commits = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(1, commits_per_day * 2)
            for _ in range(daily_count):
                author_name, author_email = random.choice(self.authors)
                commit_time = current_date + timedelta(seconds=random.randint(0, 86400))
                if commit_time > end_date:
                    continue

                commit_hash = uuid.uuid4().hex
                commits.append(
                    GitCommit(
                        repo_id=self.repo_id,
                        hash=commit_hash,
                        message=f"Synthetic commit: {random.choice(['fix typo', 'add feature', 'update docs', 'refactor code'])}",
                        author_name=author_name,
                        author_email=author_email,
                        author_when=commit_time,
                        committer_name=author_name,
                        committer_email=author_email,
                        committer_when=commit_time,
                        parents=1,
                    )
                )
            current_date += timedelta(days=1)

        return commits

    def generate_commit_stats(self, commits: List[GitCommit]) -> List[GitCommitStat]:
        stats = []
        for commit in commits:
            # Each commit touches 1-3 files
            files_to_touch = random.sample(self.files, random.randint(1, 3))
            for file_path in files_to_touch:
                additions = random.randint(1, 100)
                deletions = random.randint(0, additions)
                stats.append(
                    GitCommitStat(
                        repo_id=self.repo_id,
                        commit_hash=commit.hash,
                        file_path=file_path,
                        additions=additions,
                        deletions=deletions,
                    )
                )
        return stats

    def generate_prs(self, count: int = 20) -> List[Dict[str, Any]]:
        prs = []
        end_date = datetime.now(timezone.utc)

        for i in range(1, count + 1):
            author_name, author_email = random.choice(self.authors)
            # PRs created over the last 60 days
            created_at = end_date - timedelta(
                days=random.randint(0, 60), hours=random.randint(0, 23)
            )

            # Simulated lifecycle
            state = random.choice(["merged", "merged", "merged", "open", "closed"])
            merged_at = None
            closed_at = None

            first_review_at = None
            first_comment_at = None
            reviews_count = 0
            comments_count = random.randint(0, 10)

            if comments_count > 0:
                first_comment_at = created_at + timedelta(
                    minutes=random.randint(5, 120)
                )

            # Review stats
            has_review = random.random() > 0.2
            if has_review:
                first_review_at = created_at + timedelta(hours=random.randint(1, 48))
                reviews_count = random.randint(1, 5)

            if state == "merged":
                merged_at = created_at + timedelta(days=random.randint(1, 7))
                closed_at = merged_at
            elif state == "closed":
                closed_at = created_at + timedelta(days=random.randint(1, 14))

            prs.append({
                "pr": GitPullRequest(
                    repo_id=self.repo_id,
                    number=i,
                    title=f"Synthetic PR #{i}: {random.choice(['Feature X', 'Fix Bug Y', 'Cleanup Z'])}",
                    state=state,
                    author_name=author_name,
                    author_email=author_email,
                    created_at=created_at,
                    merged_at=merged_at,
                    closed_at=closed_at,
                    head_branch=f"feature/{i}",
                    base_branch="main",
                    additions=random.randint(10, 500),
                    deletions=random.randint(5, 200),
                    changed_files=random.randint(1, 10),
                    first_review_at=first_review_at,
                    first_comment_at=first_comment_at,
                    reviews_count=reviews_count,
                    comments_count=comments_count,
                    changes_requested_count=random.randint(0, 2),
                ),
                "reviews": self._generate_pr_reviews(i, first_review_at, reviews_count)
                if first_review_at
                else [],
            })
        return prs

    def generate_ci_pipeline_runs(
        self, days: int = 30, runs_per_day: int = 3
    ) -> List[CiPipelineRun]:
        runs = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        run_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(1, max(1, runs_per_day * 2))
            for _ in range(daily_count):
                queued_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 12)
                )
                started_at = queued_at + timedelta(minutes=random.randint(1, 30))
                duration_minutes = random.randint(5, 60)
                finished_at = started_at + timedelta(minutes=duration_minutes)
                status = random.choice(["success", "success", "failed"])

                run_index += 1
                runs.append(
                    CiPipelineRun(
                        repo_id=self.repo_id,
                        run_id=f"synth-run-{run_index}",
                        status=status,
                        queued_at=queued_at,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                )
            current_date += timedelta(days=1)
        return runs

    def generate_deployments(
        self, days: int = 30, deployments_per_day: int = 2, pr_numbers: Optional[List[int]] = None
    ) -> List[Deployment]:
        deployments = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        deploy_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(0, max(1, deployments_per_day * 2))
            for _ in range(daily_count):
                started_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 20)
                )
                duration_minutes = random.randint(5, 90)
                finished_at = started_at + timedelta(minutes=duration_minutes)
                deployed_at = finished_at + timedelta(minutes=random.randint(0, 15))
                status = random.choice(["success", "success", "failed"])
                environment = random.choice(["production", "staging"])
                merged_at = started_at - timedelta(hours=random.randint(1, 72))
                pr_number = None
                if pr_numbers:
                    pr_number = random.choice(pr_numbers)

                deploy_index += 1
                deployments.append(
                    Deployment(
                        repo_id=self.repo_id,
                        deployment_id=f"synth-deploy-{deploy_index}",
                        status=status,
                        environment=environment,
                        started_at=started_at,
                        finished_at=finished_at,
                        deployed_at=deployed_at,
                        merged_at=merged_at,
                        pull_request_number=pr_number,
                    )
                )
            current_date += timedelta(days=1)
        return deployments

    def generate_incidents(
        self, days: int = 30, incidents_per_day: int = 1
    ) -> List[Incident]:
        incidents = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        incident_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(0, max(1, incidents_per_day * 2))
            for _ in range(daily_count):
                started_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 20)
                )
                resolved_at = started_at + timedelta(hours=random.randint(1, 12))
                status = random.choice(["resolved", "resolved", "open"])
                if status == "open":
                    resolved_at = None

                incident_index += 1
                incidents.append(
                    Incident(
                        repo_id=self.repo_id,
                        incident_id=f"synth-incident-{incident_index}",
                        status=status,
                        started_at=started_at,
                        resolved_at=resolved_at,
                    )
                )
            current_date += timedelta(days=1)
        return incidents

    def _generate_pr_reviews(
        self, pr_number: int, first_review_at: datetime, count: int
    ) -> List[GitPullRequestReview]:
        reviews = []
        for i in range(count):
            reviewer_name, reviewer_email = random.choice(self.authors)
            review_time = first_review_at + timedelta(hours=random.randint(0, 24) * i)
            state = (
                "APPROVED"
                if i == count - 1
                else random.choice(["COMMENTED", "CHANGES_REQUESTED", "APPROVED"])
            )
            reviews.append(
                GitPullRequestReview(
                    repo_id=self.repo_id,
                    number=pr_number,
                    review_id=f"rev_{pr_number}_{i}",
                    reviewer=reviewer_email,
                    state=state,
                    submitted_at=review_time,
                )
            )
        return reviews

    def generate_files(self) -> List[GitFile]:
        return [
            GitFile(repo_id=self.repo_id, path=f, executable=False) for f in self.files
        ]

    def generate_work_item_metrics(
        self, days: int = 30
    ) -> List[WorkItemMetricsDailyRecord]:
        records = []
        end_date = datetime.now(timezone.utc).date()
        for i in range(days):
            day = end_date - timedelta(days=i)
            records.append(
                WorkItemMetricsDailyRecord(
                    day=day,
                    provider=self.provider,
                    work_scope_id=self.repo_name,
                    team_id="alpha",
                    team_name="Alpha Team",
                    items_started=random.randint(2, 8),
                    items_completed=random.randint(1, 6),
                    items_started_unassigned=random.randint(0, 2),
                    items_completed_unassigned=random.randint(0, 1),
                    wip_count_end_of_day=random.randint(5, 15),
                    wip_unassigned_end_of_day=random.randint(1, 3),
                    cycle_time_p50_hours=float(random.randint(24, 72)),
                    cycle_time_p90_hours=float(random.randint(72, 120)),
                    lead_time_p50_hours=float(random.randint(48, 96)),
                    lead_time_p90_hours=float(random.randint(96, 240)),
                    wip_age_p50_hours=float(random.randint(12, 48)),
                    wip_age_p90_hours=float(random.randint(48, 168)),
                    bug_completed_ratio=random.uniform(0.1, 0.4),
                    story_points_completed=float(random.randint(10, 50)),
                    # Phase 2 metrics
                    new_bugs_count=random.randint(0, 3),
                    new_items_count=random.randint(3, 10),
                    defect_intro_rate=random.uniform(0.0, 0.3),
                    wip_congestion_ratio=random.uniform(0.5, 2.0),
                    computed_at=datetime.now(timezone.utc),
                )
            )
        return records

    def generate_work_item_cycle_times(
        self, count: int = 50
    ) -> List[WorkItemCycleTimeRecord]:
        records = []
        end_date = datetime.now(timezone.utc)
        for i in range(count):
            created_at = end_date - timedelta(days=random.randint(0, 60))
            started_at = created_at + timedelta(hours=random.randint(4, 48))
            completed_at = started_at + timedelta(hours=random.randint(24, 168))

            cycle_time = (completed_at - started_at).total_seconds() / 3600
            
            # Simulate flow efficiency (typically 10-40%)
            efficiency = random.uniform(0.1, 0.6)
            active_hours = cycle_time * efficiency
            wait_hours = cycle_time * (1.0 - efficiency)

            records.append(
                WorkItemCycleTimeRecord(
                    work_item_id=f"synth:{self.repo_name}#{i}",
                    provider=self.provider,
                    day=completed_at.date(),
                    work_scope_id=self.repo_name,
                    team_id="alpha",
                    team_name="Alpha Team",
                    assignee=random.choice(self.authors)[0],
                    type=random.choice(["story", "bug", "task"]),
                    status="done",
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    cycle_time_hours=cycle_time,
                    lead_time_hours=(completed_at - created_at).total_seconds() / 3600,
                    active_time_hours=active_hours,
                    wait_time_hours=wait_hours,
                    flow_efficiency=efficiency,
                    computed_at=datetime.now(timezone.utc),
                )
            )
        return records

    def generate_work_items(self, days: int = 30) -> List[WorkItem]:
        items = []
        end_date = datetime.now(timezone.utc)

        # Focus on bugs for MTTR
        for i in range(days * 2):
            author_name, author_email = random.choice(self.authors)
            created_at = end_date - timedelta(
                days=random.randint(0, days), hours=random.randint(0, 23)
            )

            # Simulated lifecycle
            is_bug = random.random() > 0.5
            item_type: WorkItemType = (
                "bug" if is_bug else random.choice(["story", "task"])
            )

            is_done = random.random() > 0.2
            started_at = None
            completed_at = None
            status = "done" if is_done else "in_progress"

            if is_done or random.random() > 0.5:
                started_at = created_at + timedelta(hours=random.randint(1, 48))
                if is_done:
                    completed_at = started_at + timedelta(hours=random.randint(24, 168))

            items.append(
                WorkItem(
                    work_item_id=f"synth:{self.repo_name}#{i}",
                    provider=self.provider,
                    title=f"Synthetic {item_type} {i}",
                    type=item_type,
                    status=status,
                    status_raw=status,
                    repo_id=self.repo_id,
                    project_id=self.repo_name,
                    created_at=created_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    closed_at=completed_at,
                    reporter=author_email,
                    assignees=[author_email] if random.random() > 0.3 else [],
                )
            )
        return items

    def generate_work_item_transitions(
        self, items: List[WorkItem]
    ) -> List[WorkItemStatusTransition]:
        transitions = []
        for item in items:
            # Simple transition from todo -> in_progress -> done
            transitions.append(
                WorkItemStatusTransition(
                    work_item_id=item.work_item_id,
                    provider=item.provider,
                    occurred_at=item.created_at,
                    from_status_raw=None,
                    to_status_raw="todo",
                    from_status="backlog",
                    to_status="todo",
                )
            )
            if item.started_at:
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.started_at,
                        from_status_raw="todo",
                        to_status_raw="in_progress",
                        from_status="todo",
                        to_status="in_progress",
                    )
                )

                # Randomly inject a wait state (blocked) between start and complete
                if item.completed_at and random.random() > 0.5:
                    duration = (item.completed_at - item.started_at).total_seconds()
                    if duration > 7200:  # If duration > 2 hours
                        blocked_at = item.started_at + timedelta(
                            seconds=random.randint(3600, int(duration * 0.4))
                        )
                        unblocked_at = blocked_at + timedelta(
                            seconds=random.randint(1800, int(duration * 0.4))
                        )

                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=blocked_at,
                                from_status_raw="in_progress",
                                to_status_raw="blocked",
                                from_status="in_progress",
                                to_status="blocked",
                            )
                        )
                        transitions.append(
                            WorkItemStatusTransition(
                                work_item_id=item.work_item_id,
                                provider=item.provider,
                                occurred_at=unblocked_at,
                                from_status_raw="blocked",
                                to_status_raw="in_progress",
                                from_status="blocked",
                                to_status="in_progress",
                            )
                        )

            if item.completed_at:
                # Need to determine the 'from' status
                # Ideally we track current status, but for now assuming we return to 'in_progress' before done
                transitions.append(
                    WorkItemStatusTransition(
                        work_item_id=item.work_item_id,
                        provider=item.provider,
                        occurred_at=item.completed_at,
                        from_status_raw="in_progress",
                        to_status_raw="done",
                        from_status="in_progress",
                        to_status="done",
                    )
                )
        return transitions
