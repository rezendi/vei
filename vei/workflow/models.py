from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EvaluationLevel(str, Enum):
    DESCRIPTIVE = "descriptive"
    LABELED = "labeled"
    RUBRIC_EVALUABLE = "rubric_evaluable"
    CONTRACT_EVALUABLE = "contract_evaluable"
    RL_PACKAGED = "rl_packaged"


_EVALUATION_LEVEL_RANK = {
    EvaluationLevel.DESCRIPTIVE: 0,
    EvaluationLevel.LABELED: 1,
    EvaluationLevel.RUBRIC_EVALUABLE: 2,
    EvaluationLevel.CONTRACT_EVALUABLE: 3,
    EvaluationLevel.RL_PACKAGED: 4,
}


def evaluation_level_rank(level: EvaluationLevel | str) -> int:
    return _EVALUATION_LEVEL_RANK[EvaluationLevel(level)]


def evaluation_level_at_least(
    level: EvaluationLevel | str,
    required: EvaluationLevel | str,
) -> bool:
    return evaluation_level_rank(level) >= evaluation_level_rank(required)


class BusinessTaskStatus(str, Enum):
    DRAFT = "draft"
    REVIEWED = "reviewed"
    ACTIVE = "active"
    RETIRED = "retired"


class WorkflowLabelKind(str, Enum):
    GOOD_EXAMPLE = "good_example"
    BAD_EXAMPLE = "bad_example"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    NEEDS_HUMAN = "needs_human"
    MISSING_CONTEXT = "missing_context"
    POLICY_RISK = "policy_risk"
    TOOL_FAILURE = "tool_failure"
    UNCLEAR_OUTCOME = "unclear_outcome"


class WorkflowLabel(BaseModel):
    label_id: str
    candidate_id: str | None = None
    task_id: str | None = None
    label: WorkflowLabelKind
    note: str = ""
    event_ids: list[str] = Field(default_factory=list)
    example_ids: list[str] = Field(default_factory=list)
    created_at: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowEvidenceRef(BaseModel):
    event_id: str
    case_id: str | None = None
    ts_ms: int | None = None
    surface: str = ""
    kind: str = ""
    actor_id: str = ""
    object_refs: list[str] = Field(default_factory=list)
    snippet: str = ""


class WorkflowReferencePath(BaseModel):
    path_id: str
    title: str
    description: str = ""
    event_ids: list[str] = Field(default_factory=list)
    case_ids: list[str] = Field(default_factory=list)
    evidence_refs: list[WorkflowEvidenceRef] = Field(default_factory=list)
    labels: list[WorkflowLabel] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowObservedExample(BaseModel):
    example_id: str
    case_id: str | None = None
    thread_ref: str = ""
    summary: str = ""
    event_ids: list[str] = Field(default_factory=list)
    surfaces: list[str] = Field(default_factory=list)
    actor_ids: list[str] = Field(default_factory=list)
    object_refs: list[str] = Field(default_factory=list)
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    labels: list[WorkflowLabel] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BusinessTaskSpec(BaseModel):
    """Declarative, evidence-backed description of business work.

    This intentionally has no required ordered steps. Ordered step graphs and
    deterministic contracts are downstream specializations.
    """

    model_config = ConfigDict(extra="forbid")

    task_id: str
    title: str
    company_name: str = ""
    company_domain: str = ""
    objective: str = ""
    business_context: str = ""
    context_requirements: list[str] = Field(default_factory=list)
    required_evidence: list[str] = Field(default_factory=list)
    permitted_tools: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    policies: list[str] = Field(default_factory=list)
    acceptable_outputs: list[str] = Field(default_factory=list)
    accept_reject_criteria: list[str] = Field(default_factory=list)
    evaluation_rubric: list[str] = Field(default_factory=list)
    escalation_paths: list[str] = Field(default_factory=list)
    observed_examples: list[WorkflowObservedExample] = Field(default_factory=list)
    reference_paths: list[WorkflowReferencePath] = Field(default_factory=list)
    source_event_ids: list[str] = Field(default_factory=list)
    source_case_ids: list[str] = Field(default_factory=list)
    labels: list[WorkflowLabel] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)
    spec_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    status: BusinessTaskStatus = BusinessTaskStatus.DRAFT
    evaluation_level: EvaluationLevel = EvaluationLevel.DESCRIPTIVE
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_rl_ladder(self) -> "BusinessTaskSpec":
        if (
            self.evaluation_level == EvaluationLevel.RL_PACKAGED
            and self.status
            not in {BusinessTaskStatus.REVIEWED, BusinessTaskStatus.ACTIVE}
        ):
            raise ValueError("rl_packaged specs must be reviewed or active")
        return self


class WorkflowCandidate(BaseModel):
    candidate_id: str
    title: str
    company_name: str = ""
    company_domain: str = ""
    group_key: str
    source_case_ids: list[str] = Field(default_factory=list)
    source_event_ids: list[str] = Field(default_factory=list)
    thread_refs: list[str] = Field(default_factory=list)
    surfaces: list[str] = Field(default_factory=list)
    event_kinds: list[str] = Field(default_factory=list)
    actor_ids: list[str] = Field(default_factory=list)
    object_refs: list[str] = Field(default_factory=list)
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    repetition_count: int = 0
    evidence_density: float = 0.0
    cross_surface_score: float = 0.0
    escalation_score: float = 0.0
    labelability_score: float = 0.0
    rank_score: float = 0.0
    summary: str = ""
    snippets: list[str] = Field(default_factory=list)
    draft_task_spec: BusinessTaskSpec
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowMiningResult(BaseModel):
    schema_version: int = 1
    source_dir: str
    company_name: str = ""
    company_domain: str = ""
    event_count: int = 0
    candidate_count: int = 0
    candidates: list[WorkflowCandidate] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowRefreshReport(BaseModel):
    source_dir: str
    workspace: str
    output: str
    new_candidate_ids: list[str] = Field(default_factory=list)
    changed_candidate_ids: list[str] = Field(default_factory=list)
    retired_candidate_ids: list[str] = Field(default_factory=list)
    unchanged_candidate_ids: list[str] = Field(default_factory=list)
    label_count: int = 0
    wiki_refresh_status: str = "not_requested"
    skillmap_refresh_status: str = "not_requested"
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowEnvironmentPackageManifest(BaseModel):
    schema_version: int = 1
    package_id: str
    task_id: str
    evaluation_level: EvaluationLevel
    output_root: str
    files: list[str] = Field(default_factory=list)
    claim_boundaries: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
