from __future__ import annotations

import json
from pathlib import Path

import pytest
import typer.testing
from pydantic import ValidationError

from vei.benchmark import get_benchmark_family_workflow_spec
from vei.cli.vei import app as vei_cli_app
from vei.cli.vei_workflow import app as workflow_cli_app
from vei.contract.api import build_contract_from_workflow
from vei.events.api import ActorRef, EventDomain, ObjectRef, build_event
from vei.scenario_engine.api import WorkflowScenarioSpec
from vei.workflow.api import (
    BusinessTaskSpec,
    BusinessTaskStatus,
    EvaluationLevel,
    WorkflowLabelKind,
    WorkflowObservedExample,
    add_workflow_label,
    business_task_from_workflow,
    contract_from_business_task,
    load_workflow_labels,
    mine_workflows,
    package_workflow_environment,
    promote_workflow_candidate,
    refresh_workflows,
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


def test_mining_is_stable_and_promotes_labeled_specs(tmp_path: Path) -> None:
    root, context_path, event_ids = _source_root(tmp_path)
    output = tmp_path / "workflow"

    first = mine_workflows(context_path, output=output, limit=5)
    second = mine_workflows(root, limit=5)

    assert first.candidate_count >= 1
    assert first.candidates[0].candidate_id == second.candidates[0].candidate_id
    assert first.candidates[0].source_event_ids
    assert any(candidate.repetition_count >= 2 for candidate in first.candidates)
    assert any(
        candidate.metadata.get("event_count", 0) > candidate.repetition_count
        for candidate in first.candidates
    )
    assert first.candidates[0].draft_task_spec.evaluation_level == (
        EvaluationLevel.DESCRIPTIVE
    )

    candidate_id = first.candidates[0].candidate_id
    add_workflow_label(
        output,
        candidate_id=candidate_id,
        label=WorkflowLabelKind.GOOD_EXAMPLE,
        note="clean evidence path",
        event_ids=[event_ids[0]],
    )
    report = refresh_workflows(
        source_dir=context_path,
        workspace=root,
        output=output,
        limit=5,
    )
    labels = load_workflow_labels(output)
    promoted = promote_workflow_candidate(
        output,
        candidate_id=candidate_id,
        output=tmp_path / "task_spec.json",
    )

    assert report.label_count == 1
    assert labels[0].label == WorkflowLabelKind.GOOD_EXAMPLE
    assert promoted.evaluation_level == EvaluationLevel.LABELED
    assert promoted.labels[0].note == "clean evidence path"


def test_workflow_cli_mines_labels_and_promotes_specs(tmp_path: Path) -> None:
    _root, context_path, event_ids = _source_root(tmp_path)
    output = tmp_path / "workflow_cli"
    runner = typer.testing.CliRunner()

    mine_result = runner.invoke(
        workflow_cli_app,
        [
            "mine",
            "--source-dir",
            str(context_path),
            "--output",
            str(output),
            "--limit",
            "3",
        ],
    )
    assert mine_result.exit_code == 0, mine_result.output
    candidate_id = json.loads(mine_result.output)["candidates"][0]["candidate_id"]

    label_result = runner.invoke(
        workflow_cli_app,
        [
            "label",
            "--root",
            str(output),
            "--candidate-id",
            candidate_id,
            "--label",
            "good_example",
            "--note",
            "CLI label",
            "--event-id",
            event_ids[0],
        ],
    )
    assert label_result.exit_code == 0, label_result.output

    spec_path = tmp_path / "task_spec.json"
    promote_result = runner.invoke(
        workflow_cli_app,
        [
            "promote",
            "--root",
            str(output),
            "--candidate-id",
            candidate_id,
            "--output",
            str(spec_path),
        ],
    )
    assert promote_result.exit_code == 0, promote_result.output
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    assert payload["evaluation_level"] == "labeled"
    assert payload["labels"][0]["note"] == "CLI label"


def test_repo_owned_workflow_walkthrough_cli_ladder(tmp_path: Path) -> None:
    output = tmp_path / "workflows"
    runner = typer.testing.CliRunner()

    mine_result = runner.invoke(
        vei_cli_app,
        [
            "workflow",
            "mine",
            "--source-dir",
            str(CLEARWATER_WALKTHROUGH_CONTEXT),
            "--output",
            str(output),
            "--limit",
            "3",
        ],
    )
    assert mine_result.exit_code == 0, mine_result.output
    mined_payload = json.loads(mine_result.output)
    candidate = mined_payload["candidates"][0]
    candidate_id = candidate["candidate_id"]
    event_id = candidate["source_event_ids"][0]

    assert mined_payload["company_name"] == "Clearwater Field Services"
    assert mined_payload["company_domain"] == "cfs.example.com"
    assert mined_payload["event_count"] == 45
    assert candidate_id == "wfc_140309889b0c4241"
    assert candidate["title"] == "Morning Dispatch Board"
    assert candidate["draft_task_spec"]["evaluation_level"] == "descriptive"
    assert candidate["draft_task_spec"]["metadata"]["claim_boundary"] == (
        "descriptive evidence summary, not a deterministic workflow"
    )

    label_result = runner.invoke(
        vei_cli_app,
        [
            "workflow",
            "label",
            "--root",
            str(output),
            "--candidate-id",
            candidate_id,
            "--label",
            "good_example",
            "--note",
            "walkthrough evidence path",
            "--event-id",
            event_id,
        ],
    )
    assert label_result.exit_code == 0, label_result.output
    label_payload = json.loads(label_result.output)
    assert label_payload["label"] == "good_example"
    assert label_payload["event_ids"] == [event_id]

    spec_path = tmp_path / "task_spec.json"
    promote_result = runner.invoke(
        vei_cli_app,
        [
            "workflow",
            "promote",
            "--root",
            str(output),
            "--candidate-id",
            candidate_id,
            "--output",
            str(spec_path),
        ],
    )
    assert promote_result.exit_code == 0, promote_result.output
    promoted_payload = json.loads(spec_path.read_text(encoding="utf-8"))
    assert promoted_payload["evaluation_level"] == "labeled"
    assert promoted_payload["status"] == "draft"
    assert promoted_payload["labels"][0]["note"] == "walkthrough evidence path"

    package_rejected = runner.invoke(
        vei_cli_app,
        [
            "workflow",
            "package-env",
            "--spec",
            str(spec_path),
            "--source-dir",
            str(CLEARWATER_WALKTHROUGH_CONTEXT),
            "--output",
            str(tmp_path / "rejected_env"),
        ],
    )
    assert package_rejected.exit_code != 0
    assert "requires evaluation_level=rl_packaged" in package_rejected.output

    reviewed_payload = json.loads(WORKFLOW_WALKTHROUGH_SPEC.read_text(encoding="utf-8"))
    current_contract = build_contract_from_workflow(
        get_benchmark_family_workflow_spec(
            "service_ops",
            variant_name="technician_no_show",
        )
    )

    assert reviewed_payload["metadata"]["reviewed_from_candidate_id"] == candidate_id
    assert reviewed_payload["title"] == candidate["title"]
    assert reviewed_payload["source_event_ids"] == candidate["source_event_ids"]
    assert reviewed_payload["source_case_ids"] == candidate["source_case_ids"]
    assert reviewed_payload["evaluation_level"] == "rl_packaged"
    assert reviewed_payload["status"] == "reviewed"
    assert reviewed_payload["metadata"]["contract"] == current_contract.model_dump(
        mode="json"
    )

    package_result = runner.invoke(
        vei_cli_app,
        [
            "workflow",
            "package-env",
            "--spec",
            str(WORKFLOW_WALKTHROUGH_SPEC),
            "--source-dir",
            str(CLEARWATER_WALKTHROUGH_CONTEXT),
            "--output",
            str(tmp_path / "env"),
        ],
    )
    assert package_result.exit_code == 0, package_result.output
    package_payload = json.loads(package_result.output)
    assert package_payload["evaluation_level"] == "rl_packaged"
    assert package_payload["metadata"]["contract_name"] == "service_ops.contract"
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
    }.issubset(set(package_payload["files"]))
    assert package_payload["claim_boundaries"] == [
        "Rewards are deterministic process/compliance predicates.",
        "The package does not claim to optimize business outcomes.",
        "Held-out splits are stable by case/thread hash.",
    ]


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
