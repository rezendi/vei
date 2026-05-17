from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import vei.skillmap.skill_pipeline as skill_pipeline
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)
from vei.ingest.agent_activity.agent_activity_jsonl import AgentActivityJsonlAdapter
from vei.ingest.agent_activity.api import ingest_agent_activity
from vei.skillmap.api import (
    CompanySkill,
    CompanySkillMap,
    SkillEvidenceRef,
    SkillStep,
    SkillTrigger,
    build_company_skill_map_from_context_path,
    build_company_skill_map_from_workspace,
    enrich_skill_map_with_world_model_opportunities,
    render_company_skill_map_markdown,
    render_skill_refresh_report,
    validate_company_skill_map,
)


def test_skill_map_builds_shadow_skills_from_context_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    progress_messages: list[str] = []

    skill_map = build_company_skill_map_from_context_path(
        snapshot_path,
        limit=8,
        progress=progress_messages.append,
    )

    assert skill_map.schema_version == "company_skill_map_v1"
    assert skill_map.organization_name == "Acme Ops"
    assert skill_map.canonical_event_count >= 3
    assert skill_map.metadata["skill_extractor"] == "llm"
    assert skill_map.metadata["llm_provider"] == "codex"
    assert skill_map.metadata["llm_reasoning_effort"] == "low"
    assert skill_map.metadata["llm_accepted_skill_count"] == 1
    assert any("Skill map LLM shard" in message for message in progress_messages)
    assert skill_map.validation.ok is True
    assert all(skill.status == "draft" for skill in skill_map.skills)
    assert all(skill.evidence_refs for skill in skill_map.skills)
    replayed = [skill for skill in skill_map.skills if skill.replay_results]
    assert replayed
    assert replayed[0].replay_score > 0
    assert replayed[0].deployment_readiness in {
        "needs_review",
        "shadow_ready",
        "activation_candidate",
    }
    assert replayed[0].replay_results[0].counterfactual_prompt
    assert any(skill.domain == "renewal_ops" for skill in skill_map.skills)
    assert any(
        not step.read_only and step.requires_approval
        for skill in skill_map.skills
        for step in skill.steps
    )
    serialized = json.dumps(skill_map.model_dump(mode="json"))
    for forbidden_default_world_value in [
        "checkout_v2",
        "PD-9001",
        "Q2 Territory Plan",
        "REQ-8801",
        "TCK-42",
    ]:
        assert forbidden_default_world_value not in serialized
    assert all(
        evidence.ref_type != "graph_plan"
        for skill in skill_map.skills
        for evidence in skill.evidence_refs
    )
    assert (
        skill_map.metadata["graph_plan_skills"]
        == "omitted_for_context_bundle_to_avoid_template_leakage"
    )

    markdown = render_company_skill_map_markdown(skill_map)
    assert "Company Skill Map: Acme Ops" in markdown
    assert "Evidence:" in markdown


def test_skill_map_refresh_uses_workspace_control_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_control_skillmap_llm(monkeypatch)
    workspace = _write_skillmap_workspace_with_control_activity(tmp_path)

    skill_map = build_company_skill_map_from_workspace(
        workspace,
        limit=4,
        include_replay=False,
    )

    assert skill_map.metadata["builder"] == "workspace_control"
    assert skill_map.metadata["skill_source_policy"] == (
        "workspace_context_plus_control_spine"
    )
    assert skill_map.metadata["control_event_count"] == 1
    assert "agent_activity_jsonl" in skill_map.source_providers
    assert skill_map.canonical_event_count >= 4
    assert skill_map.validation.ok is True
    control_event_id = skill_map.metadata["control_event_ids_sample"][0]
    assert any(
        evidence.ref_type == "event" and evidence.ref_id == control_event_id
        for skill in skill_map.skills
        for evidence in skill.evidence_refs
    )
    assert any(
        "observed captured agent behavior" in skill.summary
        for skill in skill_map.skills
    )


def test_skill_map_requires_llm_credentials_when_building_context_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    monkeypatch.setattr(
        "vei.skillmap.skill_pipeline._llm_available", lambda provider: False
    )

    with pytest.raises(
        RuntimeError, match="deterministic skill extraction is disabled"
    ):
        build_company_skill_map_from_context_path(snapshot_path, limit=4)


def test_skill_map_uses_codex_gpt_55_without_default_downgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models: list[str] = []

    async def fake_plan_once_with_usage(**kwargs: object) -> SimpleNamespace:
        model = str(kwargs["model"])
        models.append(model)
        payload = json.loads(str(kwargs["user"]))
        if "evidence_catalog" in payload:
            return SimpleNamespace(
                plan={
                    "tool": "skillmap.cluster",
                    "args": {
                        "clusters": [
                            {
                                "title": "Sensitive data release gate",
                                "summary": "Authentication reports show repeated sensitive logging risk.",
                                "candidate_type": "flagship_skill",
                                "domain": "security_privacy",
                                "positive_triggers": ["OTP or CVV appears in logs"],
                                "negative_triggers": ["No auth or payment fields"],
                                "reuse_pattern": "Sensitive auth/payment fields require redaction review.",
                                "evidence_ids": ["event:evt-1"],
                                "allowed_actions": ["audit_logs"],
                                "blocked_actions": ["live_write_without_approval"],
                                "output_artifacts": [
                                    {
                                        "artifact_id": "redaction_audit",
                                        "title": "Redaction audit",
                                        "kind": "table",
                                        "schema_hint": "field, risk, mitigation",
                                    }
                                ],
                                "replay_checks": [
                                    "Replay seeded auth data and confirm tokens are absent."
                                ],
                                "usefulness_scores": {
                                    "company_specificity": 0.9,
                                    "repeat_frequency": 0.7,
                                    "business_consequence": 0.9,
                                    "actionability": 0.8,
                                    "evidence_coverage": 0.8,
                                    "risk_if_wrong": 0.9,
                                    "replay_testability": 0.8,
                                },
                                "usefulness_rationale": "Prevents repeated sensitive-data exposure.",
                                "confidence": 0.83,
                            }
                        ]
                    },
                },
                usage=_usage(model),
            )
        return SimpleNamespace(
            plan={
                "tool": "skillmap.propose",
                "args": {
                    "skills": [
                        {
                            "title": "Audit auth/payment log redaction",
                            "summary": "Use cited auth and payment evidence before shipping logging changes.",
                            "candidate_type": "flagship_skill",
                            "domain": "security_privacy",
                            "trigger": {
                                "description": "Auth or payment flows touch logs.",
                                "signals": ["OTP", "CVV", "PII"],
                            },
                            "negative_triggers": ["No sensitive fields touched."],
                            "goal": "Prevent sensitive fields from entering logs or local stores.",
                            "reuse_pattern": "Sensitive fields need redaction audit before release.",
                            "evidence_ids": ["event:evt-1"],
                            "steps": [
                                {
                                    "instruction": "Review cited sensitive-data report.",
                                    "tool": "event.search",
                                    "read_only": True,
                                }
                            ],
                            "output_artifacts": [
                                {
                                    "artifact_id": "redaction_audit",
                                    "title": "Redaction audit",
                                    "kind": "table",
                                    "schema_hint": "field, risk, mitigation",
                                }
                            ],
                            "replay_checks": [
                                "Replay seeded auth data and confirm tokens are absent."
                            ],
                            "allowed_actions": ["audit_logs"],
                            "blocked_actions": ["live_write_without_approval"],
                            "execution_mode": "shadow",
                            "tags": ["privacy", "auth"],
                            "usefulness_scores": {
                                "company_specificity": 0.9,
                                "repeat_frequency": 0.7,
                                "business_consequence": 0.9,
                                "actionability": 0.8,
                                "evidence_coverage": 0.8,
                                "risk_if_wrong": 0.9,
                                "replay_testability": 0.8,
                            },
                            "usefulness_rationale": "Prevents repeated sensitive-data exposure.",
                            "confidence": 0.83,
                        }
                    ]
                },
            },
            usage=_usage(model),
        )

    monkeypatch.delenv("VEI_SKILLMAP_LLM_FALLBACKS", raising=False)
    monkeypatch.setattr(
        skill_pipeline, "plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr(skill_pipeline, "_llm_available", lambda provider: True)

    skills, metadata, gaps = skill_pipeline._extract_context_skills_with_llm(
        organization_name="Py Insights",
        organization_domain="py-insights.com",
        source_providers=["teams"],
        canonical_event_count=1,
        evidence_catalog=[
            {
                "evidence_id": "event:evt-1",
                "ref_type": "event",
                "title": "OTP is logged",
                "source": "teams",
                "surface": "teams",
                "timestamp": "2026-05-15T00:00:00Z",
                "snippet": "OTP is being captured in logs.",
                "summary": "OTP is being captured in logs.",
                "facts": {},
                "evidence_ref": {
                    "ref_type": "event",
                    "ref_id": "evt-1",
                    "source": "teams",
                    "surface": "teams",
                    "title": "OTP is logged",
                    "timestamp": "2026-05-15T00:00:00Z",
                    "snippet": "OTP is being captured in logs.",
                },
            }
        ],
        limit=4,
        provider=None,
        model=None,
        timeout_s=30,
        catalog_shard_size=80,
    )

    assert set(models) == {"gpt-5.5"}
    assert skills
    assert metadata["llm_fallback_used"] is False
    assert metadata["llm_model"] == "gpt-5.5"
    assert not any(gap.severity == "error" for gap in gaps)


def test_skill_map_fallback_env_overrides_default_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "VEI_SKILLMAP_LLM_FALLBACKS",
        "openai:gpt-5-mini",
    )

    assert skill_pipeline._skillmap_llm_attempts("codex", "gpt-5.5") == [
        ("codex", "gpt-5.5"),
        ("openai", "gpt-5-mini"),
    ]


def test_skill_map_fallback_env_can_disable_default_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VEI_SKILLMAP_LLM_FALLBACKS", "none")

    assert skill_pipeline._skillmap_llm_attempts("codex", "gpt-5.5") == [
        ("codex", "gpt-5.5")
    ]


def _usage(model: str) -> SimpleNamespace:
    return SimpleNamespace(
        provider="codex",
        model=model,
        prompt_tokens=10,
        completion_tokens=20,
        total_tokens=30,
        estimated_cost_usd=None,
    )


def test_skill_map_adds_cited_world_model_opportunities(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    strategic_path = tmp_path / "strategic_state_point_results.csv"
    strategic_path.write_text(
        "\n".join(
            [
                (
                    "decision_point,candidate_label,candidate_type,"
                    "counterfactual_action,supported_target_score,"
                    "ranking_basis,saturation_guard_trusted_for_ranking,"
                    "success_observable,failure_observable"
                ),
                (
                    "Renewal legal decision,Legal review gate,focused_pilot,"
                    "Run legal review before the renewal customer reply,0.82,"
                    "supported_curated_targets,true,"
                    "Legal review is complete before reply,"
                    "Customer reply is sent before legal review"
                ),
                (
                    "Renewal finance decision,Finance confirmation gate,focused_pilot,"
                    "Require finance confirmation before any external customer reply,0.78,"
                    "supported_curated_targets,true,"
                    "Finance confirms before external reply,"
                    "External reply goes out before finance confirms"
                ),
                (
                    "Uncited narrative decision,Narrative reset,commercial_reset,"
                    "Rewrite the market narrative around category leadership,0.9,"
                    "supported_curated_targets,true,"
                    "Narrative is clearer,"
                    "Narrative remains vague"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    skill_map = CompanySkillMap(
        organization_name="Acme Ops",
        organization_domain="acme.example",
        generated_at="2026-01-03T00:00:00Z",
        source_ref=str(snapshot_path),
        skills=[
            CompanySkill(
                skill_id="skill:legal-review",
                title="Renewal legal review gate",
                summary="Use legal review for renewal risk before customer replies.",
                trigger=SkillTrigger(description="Renewal risk needs legal review."),
                goal="Prevent customer replies before legal review is complete.",
                steps=[
                    SkillStep(
                        step_id="collect",
                        instruction="Collect renewal risk and legal review evidence.",
                    )
                ],
                confidence=0.7,
            )
        ],
    )

    enriched = enrich_skill_map_with_world_model_opportunities(
        skill_map,
        context_path=snapshot_path,
        world_model_report_path=strategic_path,
        max_opportunities=4,
    )

    meta = enriched.metadata["world_model_skill_opportunities"]
    assert meta["opportunities_added"] == 2
    assert meta["skill_upgrade_count"] == 1
    assert meta["missing_skill_gap_count"] == 1
    assert meta["rows_skipped_uncited"] == 1
    gap = next(
        gap
        for gap in enriched.gaps
        if gap.metadata.get("opportunity_source") == "world_model_skill_opportunity_v1"
    )
    assert "Finance confirmation gate" in gap.title
    assert gap.evidence_refs
    assert all(ref.ref_type == "event" for ref in gap.evidence_refs)
    upgrades = enriched.skills[0].metadata["world_model_upgrade_opportunities"]
    assert upgrades[0]["candidate_label"] == "Legal review gate"
    assert upgrades[0]["supporting_evidence_ids"]


def test_skill_map_processes_entire_evidence_catalog_across_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)

    skill_map = build_company_skill_map_from_context_path(
        snapshot_path,
        limit=8,
        include_replay=False,
        catalog_shard_size=2,
    )

    assert skill_map.metadata["evidence_catalog_count"] > 2
    assert skill_map.metadata["llm_shard_count"] > 1
    assert (
        skill_map.metadata["evidence_catalog_processed_count"]
        == skill_map.metadata["evidence_catalog_count"]
    )
    assert (
        skill_map.metadata["evidence_catalog_policy"]
        == "full_normalized_bundle_sharded_then_globalized"
    )
    assert skill_map.metadata["llm_pipeline"] == "evidence_clusters_then_global_skills"
    assert skill_map.metadata["llm_accepted_cluster_count"] >= 2
    assert skill_map.metadata["llm_selected_cluster_count"] >= 1


def test_skill_map_refresh_preserves_review_state_and_retires_missing_skills(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_llm(monkeypatch)
    snapshot_path = _write_skillmap_snapshot(tmp_path)
    first_map = build_company_skill_map_from_context_path(snapshot_path, limit=4)
    previous_map = first_map.model_copy(deep=True)
    previous_map.skills[0].owner = "maya@acme.example"
    previous_map.skills[0].reviewer = "legal@acme.example"
    previous_map.skills[0].review_status = "reviewed"
    previous_map.skills.append(
        CompanySkill(
            skill_id="skill:legacy-retired",
            title="Legacy removed skill",
            summary="A previously reviewed skill no longer grounded in the bundle.",
            domain="legacy",
            trigger=SkillTrigger(description="Legacy trigger."),
            goal="Retire if no refreshed evidence supports it.",
            steps=[
                SkillStep(
                    step_id="legacy-review",
                    instruction="Review the legacy map entry.",
                    read_only=True,
                )
            ],
            evidence_refs=[
                SkillEvidenceRef(
                    ref_type="source",
                    ref_id="legacy-map",
                    title="Legacy skill map",
                )
            ],
        )
    )
    previous_path = tmp_path / "previous_skill_map.json"
    previous_path.write_text(
        json.dumps(previous_map.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )

    refreshed = build_company_skill_map_from_context_path(
        snapshot_path,
        limit=4,
        previous_map_path=previous_path,
    )

    preserved = next(
        skill
        for skill in refreshed.skills
        if skill.metadata.get("refresh_status") == "preserved"
    )
    assert preserved.owner == "maya@acme.example"
    assert preserved.reviewer == "legal@acme.example"
    assert preserved.review_status == "reviewed"
    retired = [
        skill
        for skill in refreshed.skills
        if skill.metadata.get("refresh_status") == "retired"
    ]
    assert retired and retired[0].status == "retired"
    assert refreshed.metadata["refresh"]["preserved_skill_count"] == 1
    assert refreshed.metadata["refresh"]["retired_skill_count"] == 1
    assert "Retired Skills" in render_skill_refresh_report(refreshed)


def test_skill_map_validation_rejects_unreviewed_active_skill() -> None:
    skill = CompanySkill(
        skill_id="skill:test",
        title="Send renewal update",
        summary="Demo active skill.",
        status="active",
        domain="comm_graph",
        trigger=SkillTrigger(description="A renewal update is due."),
        goal="Send a renewal update.",
        steps=[],
        evidence_refs=[],
        execution_mode="approval_gated",
    )
    skill_map = CompanySkillMap(
        organization_name="Acme Ops",
        generated_at="2026-01-01T00:00:00Z",
        source_ref="unit-test",
        skills=[skill],
    )

    validation = validate_company_skill_map(skill_map)

    assert validation.ok is False
    assert validation.error_count >= 3
    assert {issue.code for issue in validation.issues if issue.severity == "error"} >= {
        "skill_without_evidence",
        "skill_without_steps",
        "active_skill_missing_owner_or_reviewer",
        "active_skill_not_reviewed",
        "active_skill_missing_replay_proof",
    }


def _patch_skillmap_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_plan_once_with_usage(**kwargs: object) -> SimpleNamespace:
        payload = json.loads(str(kwargs["user"]))
        if "evidence_catalog" in payload:
            evidence_ids = [
                str(item["evidence_id"])
                for item in payload["evidence_catalog"]
                if str(item.get("evidence_id") or "").startswith(
                    ("case:", "event:", "knowledge_asset:")
                )
            ]
            return SimpleNamespace(
                plan={
                    "tool": "skillmap.cluster",
                    "args": {
                        "clusters": [
                            {
                                "title": "CASE-123 renewal risk operating cluster",
                                "summary": "Acme renewal risks repeat across customer mail, internal coordination, and legal SOP evidence.",
                                "candidate_type": "flagship_skill",
                                "domain": "renewal_ops",
                                "positive_triggers": ["CASE-123 renewal risk"],
                                "negative_triggers": [
                                    "Routine renewal without legal or finance blocker"
                                ],
                                "reuse_pattern": "Renewal risks need legal and finance review before customer response.",
                                "evidence_ids": evidence_ids[:5],
                                "allowed_actions": ["review_case_timeline"],
                                "blocked_actions": ["live_write_without_approval"],
                                "output_artifacts": [
                                    {
                                        "artifact_id": "renewal_update",
                                        "title": "Internal renewal update",
                                        "kind": "markdown",
                                        "schema_hint": "case, blocker, owner, next action",
                                    }
                                ],
                                "replay_checks": [
                                    "Future events should show legal or finance review before customer-facing response."
                                ],
                                "usefulness_scores": {
                                    "company_specificity": 0.9,
                                    "repeat_frequency": 0.7,
                                    "business_consequence": 0.8,
                                    "actionability": 0.9,
                                    "evidence_coverage": 0.8,
                                    "risk_if_wrong": 0.7,
                                    "replay_testability": 0.8,
                                },
                                "usefulness_rationale": "This is a high-risk repeatable renewal decision gate.",
                                "confidence": 0.82,
                            }
                        ]
                    },
                },
                usage=SimpleNamespace(
                    provider="openai",
                    model="test-model",
                    prompt_tokens=10,
                    completion_tokens=20,
                    total_tokens=30,
                    estimated_cost_usd=0.01,
                ),
            )
        evidence_ids = [
            evidence_id
            for cluster in payload["candidate_clusters"]
            for evidence_id in cluster.get("evidence_ids", [])
        ]
        return SimpleNamespace(
            plan={
                "tool": "skillmap.propose",
                "args": {
                    "skills": [
                        {
                            "title": "Coordinate CASE-123 renewal risk review",
                            "summary": "Use Acme's renewal thread, legal response, and SOP before drafting the customer update.",
                            "candidate_type": "flagship_skill",
                            "domain": "renewal_ops",
                            "trigger": {
                                "description": "A renewal risk case appears across mail, Slack, and the renewal SOP.",
                                "signals": ["CASE-123", "#renewals", "legal review"],
                            },
                            "negative_triggers": [
                                "Routine renewal with no legal or finance blocker."
                            ],
                            "goal": "Prepare a grounded internal renewal update before any customer-facing response.",
                            "reuse_pattern": "Acme renewal risks repeat across customer mail, internal coordination, and legal SOP evidence.",
                            "evidence_ids": evidence_ids[:5],
                            "steps": [
                                {
                                    "instruction": "Review the cited CASE-123 timeline and linked renewal SOP.",
                                    "tool": "vei.structure_view",
                                    "read_only": True,
                                },
                                {
                                    "instruction": "Draft an internal renewal update for human approval.",
                                    "tool": "knowledge.compose_artifact",
                                    "graph_domain": "knowledge_graph",
                                    "graph_action": "compose_artifact",
                                    "args": {"target": "internal_update"},
                                    "read_only": False,
                                    "requires_approval": True,
                                },
                            ],
                            "output_artifacts": [
                                {
                                    "artifact_id": "renewal_update",
                                    "title": "Internal renewal update",
                                    "kind": "markdown",
                                    "schema_hint": "case, blocker, owner, next action",
                                }
                            ],
                            "replay_checks": [
                                "Future events should show legal or finance review before customer-facing response."
                            ],
                            "allowed_actions": [
                                "review_case_timeline",
                                "compose_shadow_draft",
                            ],
                            "blocked_actions": ["live_write_without_approval"],
                            "execution_mode": "shadow",
                            "tags": ["renewal", "legal_review", "CASE-123"],
                            "usefulness_scores": {
                                "company_specificity": 0.9,
                                "repeat_frequency": 0.7,
                                "business_consequence": 0.8,
                                "actionability": 0.9,
                                "evidence_coverage": 0.8,
                                "risk_if_wrong": 0.7,
                                "replay_testability": 0.8,
                            },
                            "usefulness_rationale": "This is a high-risk repeatable renewal decision gate.",
                            "confidence": 0.82,
                        }
                    ]
                },
            },
            usage=SimpleNamespace(
                provider="openai",
                model="test-model",
                prompt_tokens=10,
                completion_tokens=20,
                total_tokens=30,
                estimated_cost_usd=0.01,
            ),
        )

    monkeypatch.setattr(
        "vei.skillmap.skill_pipeline.plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr(
        "vei.skillmap.skill_pipeline._llm_available", lambda provider: True
    )


def _patch_control_skillmap_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_plan_once_with_usage(**kwargs: object) -> SimpleNamespace:
        payload = json.loads(str(kwargs["user"]))
        if "evidence_catalog" in payload:
            evidence_ids = [
                str(item["evidence_id"])
                for item in payload["evidence_catalog"]
                if str(item.get("evidence_id") or "").startswith("event:")
                and str(item.get("summary") or "").find("tool=docs.create") >= 0
            ]
            assert evidence_ids, payload["evidence_catalog"]
            return SimpleNamespace(
                plan={
                    "tool": "skillmap.cluster",
                    "args": {
                        "clusters": [
                            {
                                "title": "Captured renewal draft behavior",
                                "summary": "A workspace agent created a renewal draft from the captured CASE-123 evidence.",
                                "candidate_type": "workflow",
                                "domain": "renewal_ops",
                                "positive_triggers": [
                                    "CASE-123 renewal risk draft is needed"
                                ],
                                "negative_triggers": [
                                    "No captured agent draft or case evidence exists"
                                ],
                                "reuse_pattern": "Renewal-risk drafts should follow the captured agent behavior and stay approval-gated.",
                                "evidence_ids": evidence_ids[:1],
                                "allowed_actions": ["compose_shadow_draft"],
                                "blocked_actions": ["send_without_approval"],
                                "output_artifacts": [
                                    {
                                        "artifact_id": "renewal_control_draft",
                                        "title": "Renewal control draft",
                                        "kind": "markdown",
                                        "schema_hint": "case, evidence, draft owner, approval state",
                                    }
                                ],
                                "replay_checks": [
                                    "The agent draft should cite the same case before any external reply."
                                ],
                                "usefulness_scores": {
                                    "company_specificity": 0.9,
                                    "repeat_frequency": 0.6,
                                    "business_consequence": 0.8,
                                    "actionability": 0.9,
                                    "evidence_coverage": 0.8,
                                    "risk_if_wrong": 0.7,
                                    "replay_testability": 0.7,
                                },
                                "confidence": 0.84,
                            }
                        ]
                    },
                },
                usage=SimpleNamespace(
                    provider="openai",
                    model="test-model",
                    prompt_tokens=9,
                    completion_tokens=18,
                    total_tokens=27,
                    estimated_cost_usd=0.01,
                ),
            )
        evidence_ids = [
            evidence_id
            for cluster in payload["candidate_clusters"]
            for evidence_id in cluster.get("evidence_ids", [])
        ]
        return SimpleNamespace(
            plan={
                "tool": "skillmap.propose",
                "args": {
                    "skills": [
                        {
                            "title": "Refresh renewal draft from captured agent behavior",
                            "summary": "Use observed captured agent behavior and the company case context to produce an approval-gated renewal draft.",
                            "candidate_type": "workflow",
                            "domain": "renewal_ops",
                            "trigger": {
                                "description": "CASE-123 needs a renewal-risk draft and Control shows an agent already created one.",
                                "signals": ["CASE-123", "docs.create", "renewal draft"],
                            },
                            "negative_triggers": [
                                "The workspace has no captured agent draft behavior."
                            ],
                            "goal": "Create a grounded internal renewal draft that remains approval-gated.",
                            "reuse_pattern": "Renewal-risk drafts should follow the captured agent behavior and stay approval-gated.",
                            "evidence_ids": evidence_ids[:1],
                            "steps": [
                                {
                                    "instruction": "Review the cited Control event and the linked CASE-123 company context.",
                                    "tool": "vei.provenance.evidence_pack",
                                    "read_only": True,
                                },
                                {
                                    "instruction": "Draft the internal renewal update for human approval.",
                                    "tool": "knowledge.compose_artifact",
                                    "read_only": False,
                                    "requires_approval": True,
                                },
                            ],
                            "output_artifacts": [
                                {
                                    "artifact_id": "renewal_control_draft",
                                    "title": "Renewal control draft",
                                    "kind": "markdown",
                                    "schema_hint": "case, evidence, draft owner, approval state",
                                }
                            ],
                            "replay_checks": [
                                "The agent draft should cite the same case before any external reply."
                            ],
                            "allowed_actions": ["compose_shadow_draft"],
                            "blocked_actions": ["send_without_approval"],
                            "execution_mode": "shadow",
                            "tags": ["control", "renewal", "CASE-123"],
                            "usefulness_scores": {
                                "company_specificity": 0.9,
                                "repeat_frequency": 0.6,
                                "business_consequence": 0.8,
                                "actionability": 0.9,
                                "evidence_coverage": 0.8,
                                "risk_if_wrong": 0.7,
                                "replay_testability": 0.7,
                            },
                            "confidence": 0.84,
                        }
                    ]
                },
            },
            usage=SimpleNamespace(
                provider="openai",
                model="test-model",
                prompt_tokens=9,
                completion_tokens=18,
                total_tokens=27,
                estimated_cost_usd=0.01,
            ),
        )

    monkeypatch.setattr(
        "vei.skillmap.skill_pipeline.plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr(
        "vei.skillmap.skill_pipeline._llm_available", lambda provider: True
    )


def _write_skillmap_workspace_with_control_activity(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    snapshot_path = _write_skillmap_snapshot(workspace)
    activity_path = tmp_path / "agent_activity.jsonl"
    activity_path.write_text(
        json.dumps(
            {
                "id": "agent-draft-1",
                "ts_ms": 1767272400000,
                "case_id": "case:CASE-123",
                "actor_id": "renewal.agent",
                "actor_display_name": "Renewal Agent",
                "tool": "docs.create",
                "args": {
                    "doc_id": "DOC-CASE-123-DRAFT",
                    "title": "CASE-123 renewal control draft",
                },
                "response": {
                    "doc_id": "DOC-CASE-123-DRAFT",
                    "title": "CASE-123 renewal control draft",
                },
                "status": "completed",
                "source_granularity": "per_call",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    ingest_agent_activity(
        adapter=AgentActivityJsonlAdapter(activity_path, tenant_id="acme.example"),
        workspace=snapshot_path.parent,
    )
    return workspace


def _write_skillmap_snapshot(tmp_path: Path) -> Path:
    snapshot = ContextSnapshot(
        organization_name="Acme Ops",
        organization_domain="acme.example",
        captured_at="2026-01-03T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"threads": 1, "messages": 1},
                data={
                    "threads": [
                        {
                            "thread_id": "thr-CASE-123",
                            "subject": "CASE-123 renewal risk",
                            "messages": [
                                {
                                    "from": "maya@acme.example",
                                    "to": ["legal@acme.example"],
                                    "date": "2026-01-01T10:00:00Z",
                                    "subject": "CASE-123 renewal risk",
                                    "body_text": (
                                        "CASE-123 renewal risk needs legal review "
                                        "before the customer reply."
                                    ),
                                },
                                {
                                    "from": "legal@acme.example",
                                    "to": ["maya@acme.example"],
                                    "date": "2026-01-01T10:30:00Z",
                                    "subject": "CASE-123 renewal risk",
                                    "body_text": (
                                        "Legal review complete. Keep the first "
                                        "reply internal until finance confirms."
                                    ),
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="slack",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"channels": 1, "messages": 1},
                data={
                    "channels": [
                        {
                            "channel": "#renewals",
                            "messages": [
                                {
                                    "user": "maya@acme.example",
                                    "ts": "2026-01-01T11:00:00Z",
                                    "text": (
                                        "CASE-123 renewal risk has a customer "
                                        "thread and legal review."
                                    ),
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="notion",
                captured_at="2026-01-03T00:00:00Z",
                record_counts={"pages": 1},
                data={
                    "pages": [
                        {
                            "page_id": "page-1",
                            "title": "CASE-123 renewal SOP",
                            "body": "Use legal review for renewal risk.",
                            "tags": ["renewal", "sop"],
                            "linked_object_refs": ["case:CASE-123"],
                            "updated_at": "2026-01-01T12:00:00Z",
                        }
                    ]
                },
            ),
        ],
    )
    snapshot_path = tmp_path / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(snapshot.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    write_canonical_history_sidecars(snapshot, snapshot_path)
    return snapshot_path
