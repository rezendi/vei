from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

BUS_FACTOR_REPORT_VERSION: Literal["bus_factor_report_v1"] = "bus_factor_report_v1"

SoleOwnedKind = Literal["skill", "workflow_promoted", "workflow_good_example"]


class SoleOwnedSkill(BaseModel):
    skill_id: str
    title: str
    summary: str = ""
    evidence_event_ids: list[str] = Field(default_factory=list)
    evidence_event_count: int = 0


class SoleOwnedWorkflow(BaseModel):
    candidate_id: str
    title: str
    label: Literal["promoted", "good_example", "candidate"]
    activity_share: float
    sent_event_count: int
    total_event_count: int


class ActorRiskProfile(BaseModel):
    actor_id: str
    display_name: str = ""
    email: str = ""
    last_active_at: str = ""
    last_active_ts_ms: int = 0
    sent_event_count_in_window: int = 0
    sole_owned_skills: list[SoleOwnedSkill] = Field(default_factory=list)
    sole_owned_workflows: list[SoleOwnedWorkflow] = Field(default_factory=list)
    flag_predicates: list[str] = Field(default_factory=list)


class BusFactorReport(BaseModel):
    version: Literal["bus_factor_report_v1"] = BUS_FACTOR_REPORT_VERSION
    tenant_id: str
    generated_at: str
    snapshot_path: str
    snapshot_hash: str = ""
    window_days: int
    window_start_ts_ms: int
    window_end_ts_ms: int
    corpus_first_event_ts_ms: int = 0
    corpus_last_event_ts_ms: int = 0
    total_events_in_corpus: int = 0
    total_events_in_window: int = 0
    sole_owner_definition: str = (
        "An actor is the sole owner of a skill if it is the only actor "
        "appearing as `actor_ref` (i.e. the sender) on the skill's cited "
        "evidence events that fall within the analysis window."
    )
    activity_share_threshold: float = 0.80
    skill_map_path: str = ""
    workflow_candidates_path: str = ""
    workflow_labels_path: str = ""
    actor_profiles: list[ActorRiskProfile] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
