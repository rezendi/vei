from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from vei.contract.api import build_contract_from_workflow
from vei.events.api import ActorRef, EventDomain, ObjectRef, build_event
from vei.scenario_engine.api import WorkflowScenarioSpec
from vei.workflow.api import (
    BusinessTaskSpec,
    BusinessTaskStatus,
    EvaluationLevel,
    WorkflowObservedExample,
    business_task_from_workflow,
    contract_from_business_task,
    mine_workflows,
    package_workflow_environment,
)

REPO_EXAMPLES_ROOT = Path("docs/examples")
CLEARWATER_WALKTHROUGH_CONTEXT = (
    REPO_EXAMPLES_ROOT
    / "clearwater-technician-no-show"
    / "workspace"
    / "context_snapshot.json"
)
WORKFLOW_WALKTHROUGH_SPEC = (
    REPO_EXAMPLES_ROOT
    / "workflow-intelligence-walkthrough"
    / "reviewed_service_ops_task_spec.json"
)


def _write_context(
    root: Path, *, name: str = "Acme", domain: str = "acme.test"
) -> Path:
    path = root / "context_snapshot.json"
    path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": name,
                "organization_domain": domain,
                "sources": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _write_events(root: Path) -> list[str]:
    actors = [
        ActorRef(actor_id="customer@example.com", display_name="Customer"),
        ActorRef(actor_id="ops@acme.test", display_name="Ops"),
    ]
    events = [
        build_event(
            event_id="evt-1",
            tenant_id="acme.test",
            case_id="case-pilot",
            ts_ms=1_700_000_000_000,
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=actors[0],
            participants=[actors[1]],
            object_refs=[
                ObjectRef(
                    object_id="thread-1",
                    domain="comm_graph",
                    kind="mail",
                    label="Pilot proposal from Acme",
                )
            ],
            delta_data={
                "surface": "mail",
                "thread_ref": "mail:thread-1",
                "subject": "Pilot proposal from Acme",
                "snippet": "Customer asks for a partner pilot proposal and deadline.",
            },
        ).with_hash(),
        build_event(
            event_id="evt-2",
            tenant_id="acme.test",
            case_id="case-pilot",
            ts_ms=1_700_000_060_000,
            domain=EventDomain.WORK_GRAPH,
            kind="ticket.created",
            actor_ref=actors[1],
            object_refs=[
                ObjectRef(
                    object_id="T-1",
                    domain="work_graph",
                    kind="ticket",
                    label="Prepare partner pilot proposal",
                )
            ],
            delta_data={
                "surface": "tickets",
                "title": "Prepare partner pilot proposal",
                "snippet": "Create proposal, confirm approval, and capture evidence.",
            },
        ).with_hash(),
        build_event(
            event_id="evt-3",
            tenant_id="acme.test",
            case_id="case-renewal",
            ts_ms=1_700_010_000_000,
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=actors[0],
            participants=[actors[1]],
            object_refs=[
                ObjectRef(
                    object_id="thread-2",
                    domain="comm_graph",
                    kind="mail",
                    label="Renewal invoice question",
                )
            ],
            delta_data={
                "surface": "mail",
                "thread_ref": "mail:thread-2",
                "subject": "Renewal invoice question",
                "snippet": "Customer asks whether the renewal invoice is accepted.",
            },
        ).with_hash(),
        build_event(
            event_id="evt-4",
            tenant_id="acme.test",
            case_id="case-renewal-2",
            ts_ms=1_700_020_000_000,
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=actors[0],
            participants=[actors[1]],
            object_refs=[
                ObjectRef(
                    object_id="thread-3",
                    domain="comm_graph",
                    kind="mail",
                    label="Renewal invoice question",
                )
            ],
            delta_data={
                "surface": "mail",
                "thread_ref": "mail:thread-3",
                "subject": "Renewal invoice question",
                "snippet": "Customer asks whether a renewal invoice has been accepted.",
            },
        ).with_hash(),
    ]
    event_path = root / "canonical_events.jsonl"
    event_path.write_text(
        "".join(event.model_dump_json() + "\n" for event in events),
        encoding="utf-8",
    )
    return [event.event_id for event in events]


def _source_root(tmp_path: Path) -> tuple[Path, Path, list[str]]:
    root = tmp_path / "source"
    root.mkdir()
    context_path = _write_context(root)
    event_ids = _write_events(root)
    return root, context_path, event_ids


def _write_company_skill_map(root: Path, event_ids: list[str]) -> Path:
    skill_map_dir = root / "skill_map"
    skill_map_dir.mkdir()
    path = skill_map_dir / "company_skill_map.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "company_skill_map_v1",
                "organization_name": "Acme",
                "organization_domain": "acme.test",
                "generated_at": "2026-05-13T00:00:00Z",
                "source_ref": "test",
                "canonical_event_count": len(event_ids),
                "skill_count": 2,
                "skills": [
                    {
                        "skill_id": "skill:release-to-qa",
                        "title": "Release-To-QA Feedback And Handoff Workflow",
                        "summary": (
                            "Turns support, proposal, and ticket evidence into "
                            "an owner-backed QA handoff."
                        ),
                        "status": "draft",
                        "domain": "product_ops",
                        "candidate_type": "workflow",
                        "usefulness_score": 0.91,
                        "usefulness_rationale": "High consequence and repeatable.",
                        "trigger": {
                            "description": (
                                "Use when a customer-facing change needs QA "
                                "handoff, evidence, and owner signoff."
                            ),
                            "signals": ["proposal", "ticket", "approval"],
                        },
                        "goal": (
                            "Create a cited QA handoff before changing the "
                            "customer-facing workflow."
                        ),
                        "prerequisites": ["Thread or case history"],
                        "steps": [
                            {
                                "step_id": "step-1",
                                "instruction": "Collect cited events and owner.",
                                "tool": "event.search",
                                "read_only": True,
                                "requires_approval": False,
                            },
                            {
                                "step_id": "step-2",
                                "instruction": "Write the QA handoff checklist.",
                                "tool": "artifact.write",
                                "read_only": False,
                                "requires_approval": True,
                            },
                        ],
                        "output_artifacts": [
                            {
                                "artifact_id": "qa-handoff",
                                "title": "QA handoff checklist",
                                "kind": "checklist",
                            }
                        ],
                        "evidence_refs": [
                            {
                                "ref_type": "event",
                                "ref_id": event_ids[0],
                                "source": "test",
                                "surface": "mail",
                                "title": "Pilot proposal from Acme",
                            },
                            {
                                "ref_type": "event",
                                "ref_id": event_ids[1],
                                "source": "test",
                                "surface": "tickets",
                                "title": "Prepare partner pilot proposal",
                            },
                            {
                                "ref_type": "event",
                                "ref_id": event_ids[1],
                                "source": "test",
                                "surface": "tickets",
                                "title": "hi",
                            },
                            {
                                "ref_type": "event",
                                "ref_id": event_ids[1],
                                "source": "test",
                                "surface": "tickets",
                                "title": (
                                    "mysql+pymysql://user:"
                                    "password@db.example.test/app"
                                ),
                            },
                        ],
                        "allowed_actions": ["create_handoff_checklist"],
                        "blocked_actions": ["ship_without_approval"],
                        "deployment_readiness": "activation_candidate",
                        "confidence": 0.9,
                        "execution_mode": "approval_gated",
                    },
                    {
                        "skill_id": "skill:junk",
                        "title": "Hi",
                        "summary": "Greeting cluster.",
                        "status": "draft",
                        "candidate_type": "workflow",
                        "usefulness_score": 1.0,
                        "trigger": {"description": "Use on greetings."},
                        "goal": "Do nothing.",
                        "steps": [
                            {
                                "step_id": "step-junk",
                                "instruction": "Say hi.",
                            }
                        ],
                        "evidence_refs": [
                            {
                                "ref_type": "event",
                                "ref_id": event_ids[2],
                                "title": "Hi",
                            }
                        ],
                        "confidence": 1.0,
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _workflow_spec() -> WorkflowScenarioSpec:
    return WorkflowScenarioSpec.model_validate(
        {
            "name": "customer_pilot_review",
            "objective": {
                "statement": "Review a customer pilot request with evidence.",
                "success": ["Pilot response cites the requested evidence."],
            },
            "world": {"catalog": "service_ops"},
            "constraints": [
                {
                    "name": "approval_boundary",
                    "description": "Do not send commercial terms without approval.",
                }
            ],
            "approvals": [
                {
                    "stage": "commercial",
                    "approver": "commercial@acme.test",
                }
            ],
            "steps": [
                {
                    "step_id": "draft",
                    "description": "Draft response",
                    "tool": "mail.send",
                }
            ],
            "success_assertions": [
                {
                    "kind": "result_contains",
                    "field": "body",
                    "contains": "evidence",
                    "description": "Response cites evidence.",
                }
            ],
        }
    )


def test_business_task_spec_is_declarative_and_gates_downstream_levels() -> None:
    spec = BusinessTaskSpec(
        task_id="bts-demo",
        title="Demo task",
        evaluation_level=EvaluationLevel.DESCRIPTIVE,
    )

    assert spec.evaluation_level == EvaluationLevel.DESCRIPTIVE
    assert "steps" not in BusinessTaskSpec.model_fields
    with pytest.raises(ValidationError):
        BusinessTaskSpec(task_id="bts-bad", title="Bad", steps=[])  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        BusinessTaskSpec(
            task_id="bts-rl",
            title="RL draft",
            evaluation_level=EvaluationLevel.RL_PACKAGED,
            status=BusinessTaskStatus.DRAFT,
        )


def test_workflow_and_contract_adapter_round_trips_contract_gate() -> None:
    workflow = _workflow_spec()
    contract = build_contract_from_workflow(workflow)

    spec = business_task_from_workflow(
        workflow,
        contract=contract,
        company_name="Acme",
        company_domain="acme.test",
    )

    assert spec.evaluation_level == EvaluationLevel.CONTRACT_EVALUABLE
    assert spec.permitted_tools == ["mail.send"]
    assert spec.reference_paths[0].metadata["example_step_ids"] == ["draft"]
    assert contract_from_business_task(spec).name == contract.name

    descriptive = spec.model_copy(update={"evaluation_level": EvaluationLevel.LABELED})
    with pytest.raises(ValueError, match="not contract-evaluable"):
        contract_from_business_task(descriptive)


def test_mining_requires_a_skill_map(tmp_path: Path) -> None:
    _root, context_path, _event_ids = _source_root(tmp_path)
    with pytest.raises(FileNotFoundError, match="requires a company skill map"):
        mine_workflows(context_path, output=tmp_path / "workflow", limit=5)


def test_mining_prefers_skill_backed_workflows_when_skill_map_exists(
    tmp_path: Path,
) -> None:
    root, context_path, event_ids = _source_root(tmp_path)
    _write_company_skill_map(root, event_ids)
    output = tmp_path / "semantic_workflow"

    result = mine_workflows(context_path, output=output, limit=5)

    assert result.metadata["selected_backend"] == "semantic"
    assert result.metadata["semantic_candidate_count"] == 1
    assert result.candidate_count == 1
    candidate = result.candidates[0]
    assert candidate.title == "Release-To-QA Feedback And Handoff Workflow"
    assert candidate.metadata["generated_by"] == "skillmap_semantic_v1"
    assert (
        candidate.draft_task_spec.evaluation_level == EvaluationLevel.RUBRIC_EVALUABLE
    )
    assert "ship_without_approval" in " ".join(candidate.draft_task_spec.constraints)
    assert "hi" not in {snippet.lower() for snippet in candidate.snippets}
    assert all("mysql+pymysql://" not in snippet for snippet in candidate.snippets)
    assert "[REDACTED_CONNECTION_STRING]" in candidate.snippets
    assert "Hi" not in {item.title for item in result.candidates}
    assert (output / "workflow_mining_manifest.json").is_file()


def test_mining_annotates_skill_workflows_with_world_model_alignment(
    tmp_path: Path,
) -> None:
    root, context_path, event_ids = _source_root(tmp_path)
    skill_map_path = _write_company_skill_map(root, event_ids)
    world_model_path = tmp_path / "strategic_state_point_results.csv"
    world_model_path.write_text(
        "\n".join(
            [
                "decision_point,candidate_label,candidate_type,counterfactual_action,supported_target_score",
                (
                    "QA release gate,QA handoff,focused_pilot,"
                    "Create a QA handoff checklist before customer-facing release,0.71"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = mine_workflows(
        context_path,
        limit=5,
        skill_map_path=skill_map_path,
        world_model_report_path=world_model_path,
    )

    alignment = result.candidates[0].metadata["world_model_alignment"]
    assert alignment["score"] > 0
    assert alignment["matches"][0]["candidate_label"] == "QA handoff"


def test_mining_surfaces_cited_world_model_skill_opportunities(
    tmp_path: Path,
) -> None:
    root, context_path, event_ids = _source_root(tmp_path)
    skill_map_path = _write_company_skill_map(root, event_ids)
    payload = json.loads(skill_map_path.read_text(encoding="utf-8"))
    payload["gaps"] = [
        {
            "gap_id": "gap:finance-confirmation",
            "title": "World-model opportunity: Finance confirmation gate",
            "severity": "warning",
            "reason": "Counterfactual search highlights finance confirmation.",
            "recommendation": (
                "Draft a skill that requires finance confirmation before an "
                "external customer reply."
            ),
            "evidence_refs": [
                {
                    "ref_type": "event",
                    "ref_id": event_ids[0],
                    "title": "Pilot proposal from Acme",
                    "snippet": (
                        "Customer proposal needs approval and finance "
                        "confirmation before external reply."
                    ),
                }
            ],
            "metadata": {
                "opportunity_source": "world_model_skill_opportunity_v1",
                "priority_score": 0.93,
                "existing_skill_coverage_score": 0.04,
                "candidate_label": "Finance confirmation gate",
                "candidate_type": "focused_pilot",
                "counterfactual_action": (
                    "Require finance confirmation before any external customer reply."
                ),
                "decision_point": "Renewal finance decision",
                "success_observable": "Finance confirms before reply.",
                "supporting_evidence_ids": [f"event:{event_ids[0]}"],
            },
        }
    ]
    skill_map_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    output = tmp_path / "semantic_workflow"

    result = mine_workflows(
        context_path,
        output=output,
        limit=5,
        skill_map_path=skill_map_path,
    )

    opportunity = next(
        candidate
        for candidate in result.candidates
        if candidate.metadata.get("generated_by") == "world_model_skill_opportunity_v1"
    )
    assert opportunity.source_event_ids == [event_ids[0]]
    assert opportunity.rank_score > 90
    assert (
        opportunity.draft_task_spec.evaluation_level == EvaluationLevel.RUBRIC_EVALUABLE
    )
    assert "Finance confirmation gate" in opportunity.title
    manifest = json.loads((output / "workflow_mining_manifest.json").read_text())
    assert manifest["world_model_opportunity_candidate_count"] == 1


def test_package_env_rejects_descriptive_specs_and_writes_rl_package(
    tmp_path: Path,
) -> None:
    root, context_path, event_ids = _source_root(tmp_path)
    workflow = _workflow_spec()
    contract = build_contract_from_workflow(workflow)
    spec = business_task_from_workflow(
        workflow,
        contract=contract,
        company_name="Acme",
        company_domain="acme.test",
    )
    descriptive_path = tmp_path / "descriptive.json"
    descriptive_path.write_text(
        spec.model_copy(
            update={"evaluation_level": EvaluationLevel.DESCRIPTIVE}
        ).model_dump_json(indent=2)
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requires evaluation_level=rl_packaged"):
        package_workflow_environment(
            spec_path=descriptive_path,
            source_dir=context_path,
            output=tmp_path / "bad_pkg",
        )

    rl_spec = spec.model_copy(
        update={
            "evaluation_level": EvaluationLevel.RL_PACKAGED,
            "status": BusinessTaskStatus.REVIEWED,
            "source_event_ids": event_ids[:3],
            "source_case_ids": ["case-pilot", "case-renewal"],
            "observed_examples": [
                WorkflowObservedExample(
                    example_id="example-1",
                    case_id="case-pilot",
                    event_ids=event_ids[:2],
                    summary="Pilot request example",
                ),
                WorkflowObservedExample(
                    example_id="example-2",
                    case_id="case-renewal",
                    event_ids=[event_ids[2]],
                    summary="Renewal request example",
                ),
            ],
        },
        deep=True,
    )
    rl_path = tmp_path / "rl_spec.json"
    rl_path.write_text(rl_spec.model_dump_json(indent=2) + "\n", encoding="utf-8")

    manifest = package_workflow_environment(
        spec_path=rl_path,
        source_dir=root,
        output=tmp_path / "env",
    )

    assert manifest.evaluation_level == EvaluationLevel.RL_PACKAGED
    assert {
        "environment_manifest.json",
        "task_spec.json",
        "contract.json",
        "observation_schema.json",
        "action_schema.json",
        "reward_spec.json",
        "reset_cases.jsonl",
        "example_traces.jsonl",
        "splits.json",
        "README.md",
    }.issubset(set(manifest.files))
    assert (
        (tmp_path / "env" / "README.md")
        .read_text(encoding="utf-8")
        .startswith("# Customer Pilot Review")
    )
    traces = [
        json.loads(line)
        for line in (tmp_path / "env" / "example_traces.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert [event["event_id"] for event in traces[0]["events"]] == event_ids[:2]
    assert [event["event_id"] for event in traces[1]["events"]] == [event_ids[2]]


def test_workflow_default_authoring_path_does_not_import_openai_sdk() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(Path("vei/workflow").glob("*.py"))
    ).lower()
    assert "from openai" not in source
    assert "import openai" not in source
    assert "openai." not in source


DISPATCH_CONTEXT = Path("_vei_out/datasets/dispatch_real/context_snapshot.json")
POWR_CONTEXT = Path("_vei_out/datasets/powrofyou/context_snapshot.json")


@pytest.mark.skipif(
    not DISPATCH_CONTEXT.exists(),
    reason="private Dispatch fixture not present",
)
def test_dispatch_private_fixture_mines_real_workflows_without_seed_leakage() -> None:
    result = mine_workflows(DISPATCH_CONTEXT, limit=8)

    assert result.company_name == "Dispatch"
    assert result.company_domain == "thedispatch.ai"
    assert result.candidate_count >= 1
    assert any(candidate.source_event_ids for candidate in result.candidates)
    assert (
        min(
            candidate.start_ts_ms
            for candidate in result.candidates
            if candidate.start_ts_ms is not None
        )
        > 946_684_800_000
    )
    payload = result.model_dump_json().lower()
    assert "clearwater facility services" not in payload
    assert "cfs.example.com" not in payload

    candidate = result.candidates[0]
    promoted = candidate.draft_task_spec
    assert promoted.company_name == "Dispatch"
    assert promoted.evaluation_level in {
        EvaluationLevel.DESCRIPTIVE,
        EvaluationLevel.LABELED,
        EvaluationLevel.RUBRIC_EVALUABLE,
    }


@pytest.mark.skipif(
    not POWR_CONTEXT.exists(),
    reason="private Powr of You fixture not present",
)
def test_powrofyou_private_fixture_mines_source_backed_specs_without_cross_leakage() -> (
    None
):
    result = mine_workflows(POWR_CONTEXT, limit=12)

    assert result.company_name == "Powr of You"
    assert result.company_domain == "powrofyou.com"
    assert result.candidate_count >= 1
    assert any(candidate.source_event_ids for candidate in result.candidates)
    assert any(
        term in candidate.model_dump_json().lower()
        for candidate in result.candidates
        for term in ("application", "partner", "deal", "customer", "pilot")
    )
    payload = result.model_dump_json().lower()
    assert "thedispatch.ai" not in payload
    assert "clearwater facility services" not in payload
    assert "enron.com" not in payload

    promoted = result.candidates[0].draft_task_spec
    assert promoted.company_name == "Powr of You"
    assert promoted.source_event_ids
    assert promoted.evaluation_level in {
        EvaluationLevel.DESCRIPTIVE,
        EvaluationLevel.LABELED,
        EvaluationLevel.RUBRIC_EVALUABLE,
    }
