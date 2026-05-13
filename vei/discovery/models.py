from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class DiscoverySourceRole(str, Enum):
    HISTORICAL = "historical"
    LIVE = "live"
    SNAPSHOT = "snapshot"


class DiscoverySourceInput(BaseModel):
    path: str
    role: DiscoverySourceRole = DiscoverySourceRole.SNAPSHOT
    label: str = ""


class DiscoveryEvidenceRef(BaseModel):
    event_id: str
    source_id: str
    source_role: DiscoverySourceRole
    case_id: str = ""
    timestamp: str = ""
    ts_ms: int = 0
    surface: str = ""
    provider: str = ""
    kind: str = ""
    actor_id: str = ""
    subject: str = ""
    snippet: str = ""


class CompanyLineageSource(BaseModel):
    source_id: str
    path: str
    role: DiscoverySourceRole
    label: str = ""
    organization_name: str = ""
    organization_domain: str = ""
    captured_at: str = ""
    source_providers: list[str] = Field(default_factory=list)
    event_count: int = 0
    case_count: int = 0
    surface_counts: dict[str, int] = Field(default_factory=dict)


class CompanyLineage(BaseModel):
    canonical_name: str
    canonical_domain: str = ""
    aliases: list[str] = Field(default_factory=list)
    sources: list[CompanyLineageSource] = Field(default_factory=list)
    source_providers: list[str] = Field(default_factory=list)
    event_count: int = 0
    case_count: int = 0
    surface_counts: dict[str, int] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkflowExample(BaseModel):
    example_id: str
    case_id: str = ""
    title: str = ""
    source_id: str = ""
    source_role: DiscoverySourceRole
    event_ids: list[str] = Field(default_factory=list)
    surfaces: list[str] = Field(default_factory=list)
    start_ts_ms: int = 0
    end_ts_ms: int = 0
    evidence_refs: list[DiscoveryEvidenceRef] = Field(default_factory=list)


class WorkflowFamily(BaseModel):
    workflow_id: str
    title: str
    objective: str
    trigger: str = ""
    domain: str = ""
    canonical_steps: list[str] = Field(default_factory=list)
    variants: list[str] = Field(default_factory=list)
    supported_skill_ids: list[str] = Field(default_factory=list)
    source_roles: list[DiscoverySourceRole] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    source_providers: list[str] = Field(default_factory=list)
    surfaces: list[str] = Field(default_factory=list)
    actor_ids: list[str] = Field(default_factory=list)
    case_count: int = 0
    event_count: int = 0
    repetition_score: float = 0.0
    business_value_score: float = 0.0
    confidence: float = 0.0
    examples: list[WorkflowExample] = Field(default_factory=list)
    evidence_refs: list[DiscoveryEvidenceRef] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeneratedSkill(BaseModel):
    skill_id: str
    title: str
    summary: str
    status: Literal["draft", "generated"] = "generated"
    activation: str
    goal: str
    supported_workflow_ids: list[str] = Field(default_factory=list)
    prerequisites: list[str] = Field(default_factory=list)
    steps: list[str] = Field(default_factory=list)
    output_artifacts: list[str] = Field(default_factory=list)
    verification_checks: list[str] = Field(default_factory=list)
    allowed_actions: list[str] = Field(default_factory=list)
    blocked_actions: list[str] = Field(default_factory=list)
    usefulness_score: float = 0.0
    confidence: float = 0.0
    evidence_refs: list[DiscoveryEvidenceRef] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    skill_path: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeneratedSkillMap(BaseModel):
    schema_version: Literal["generated_skill_map_v1"] = "generated_skill_map_v1"
    organization_name: str
    organization_domain: str = ""
    generated_at: str
    source_ref: str
    skill_count: int = 0
    skills: list[GeneratedSkill] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryManifest(BaseModel):
    schema_version: Literal["company_discovery_manifest_v1"] = (
        "company_discovery_manifest_v1"
    )
    generated_at: str
    backend: str = "deterministic_semantic_v1"
    workflow_count: int = 0
    skill_count: int = 0
    source_count: int = 0
    event_count: int = 0
    rejected_event_count: int = 0
    caveats: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryResult(BaseModel):
    schema_version: Literal["company_discovery_result_v1"] = (
        "company_discovery_result_v1"
    )
    lineage: CompanyLineage
    workflows: list[WorkflowFamily] = Field(default_factory=list)
    skill_map: GeneratedSkillMap
    manifest: DiscoveryManifest
    output_dir: str = ""
