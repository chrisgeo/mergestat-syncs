from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

try:
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v1 fallback
    ConfigDict = None


class Coverage(BaseModel):
    repos_covered_pct: float
    prs_linked_to_issues_pct: float
    issues_with_cycle_states_pct: float


class Freshness(BaseModel):
    last_ingested_at: Optional[datetime]
    sources: Dict[str, str]
    coverage: Coverage


class SparkPoint(BaseModel):
    ts: datetime
    value: float


class MetricDelta(BaseModel):
    metric: str
    label: str
    value: float
    unit: str
    delta_pct: float
    spark: List[SparkPoint]


class SummarySentence(BaseModel):
    id: str
    text: str
    evidence_link: str


class ConstraintEvidence(BaseModel):
    label: str
    link: str


class ConstraintCard(BaseModel):
    title: str
    claim: str
    evidence: List[ConstraintEvidence]
    experiments: List[str]


class EventItem(BaseModel):
    ts: datetime
    type: str
    text: str
    link: str


class HomeResponse(BaseModel):
    freshness: Freshness
    deltas: List[MetricDelta]
    summary: List[SummarySentence]
    tiles: Dict[str, Any]
    constraint: ConstraintCard
    events: List[EventItem]


class Contributor(BaseModel):
    id: str
    label: str
    value: float
    delta_pct: float
    evidence_link: str


class ExplainResponse(BaseModel):
    metric: str
    label: str
    unit: str
    value: float
    delta_pct: float
    drivers: List[Contributor]
    contributors: List[Contributor]
    drilldown_links: Dict[str, str]


class PullRequestRow(BaseModel):
    repo_id: str
    number: int
    title: Optional[str]
    author: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]
    first_review_at: Optional[datetime]
    review_latency_hours: Optional[float]
    link: Optional[str]


class IssueRow(BaseModel):
    work_item_id: str
    provider: str
    status: str
    team_id: Optional[str]
    cycle_time_hours: Optional[float]
    lead_time_hours: Optional[float]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    link: Optional[str]


class DrilldownResponse(BaseModel):
    items: List[Any]


class OpportunityCard(BaseModel):
    id: str
    title: str
    rationale: str
    evidence_links: List[str]
    suggested_experiments: List[str]


class OpportunitiesResponse(BaseModel):
    items: List[OpportunityCard]


class HealthResponse(BaseModel):
    status: str
    services: Dict[str, str]


class InvestmentCategory(BaseModel):
    key: str
    name: str
    value: float


class InvestmentSubtype(BaseModel):
    name: str
    value: float
    parent_key: str = Field(alias="parentKey")

    if ConfigDict is not None:
        model_config = ConfigDict(validate_by_name=True)
    else:
        class Config:
            allow_population_by_field_name = True


class InvestmentResponse(BaseModel):
    categories: List[InvestmentCategory]
    subtypes: List[InvestmentSubtype]
    edges: Optional[List[Dict[str, Any]]] = None
