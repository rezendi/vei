from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

import vei.pyinsights.daily_refresh as daily_refresh_module
from vei.cli.vei import app as vei_app
from vei.context.api import ContextSourceResult
from vei.pyinsights.daily_refresh import (
    DailyRefreshCheck,
    FreshContextCaptureResult,
    run_validated_daily_refresh,
)


def test_validated_daily_refresh_emits_report_only_after_gates_pass(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=True)

    manifest = run_validated_daily_refresh(
        as_of="2026-05-13",
        previous="none",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=fixture["workflow"],
        skill_map_path=fixture["skill_map"],
        refresh_workflows=False,
    )

    assert manifest.status == "validated"
    assert manifest.validation_passed is True
    assert "ceo_report" in manifest.artifacts
    assert Path(manifest.artifacts["ceo_report"]).is_file()
    assert "validation_failure_note" not in manifest.artifacts


def test_validated_daily_refresh_demotes_activation_candidate_without_owner_reviewer(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=True)
    skill_map_path = fixture["skill_map"]
    payload = json.loads(skill_map_path.read_text(encoding="utf-8"))
    payload["skills"][0]["deployment_readiness"] = "activation_candidate"
    payload["skills"][0]["owner"] = ""
    payload["skills"][0]["reviewer"] = ""
    skill_map_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    manifest = run_validated_daily_refresh(
        as_of="2026-05-13",
        previous="none",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=fixture["workflow"],
        skill_map_path=skill_map_path,
        refresh_workflows=False,
    )

    assert manifest.status == "validated"
    rendered_skill_map = json.loads(
        Path(manifest.artifacts["skill_map"]).read_text(encoding="utf-8")
    )
    assert rendered_skill_map["skills"][0]["deployment_readiness"] == "shadow_ready"


def test_validated_daily_refresh_blocks_report_when_guard_fails(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=False)

    manifest = run_validated_daily_refresh(
        as_of="2026-05-13",
        previous="none",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=fixture["workflow"],
        skill_map_path=fixture["skill_map"],
        refresh_workflows=False,
    )

    assert manifest.status == "not_validated"
    assert manifest.validation_passed is False
    assert "ceo_report" not in manifest.artifacts
    failure_note = Path(manifest.artifacts["validation_failure_note"])
    assert failure_note.is_file()
    assert "strategic.saturation_guard_passed" in failure_note.read_text()


def test_validated_daily_refresh_uses_fresh_capture_when_bundle_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=True)
    fresh_context = _copy_context_with_capture_date(
        fixture["context"],
        tmp_path / "fresh_context",
        captured_at="2026-05-14T12:00:00+00:00",
    )

    def fake_capture(**kwargs) -> FreshContextCaptureResult:
        assert kwargs["existing_context_path"] == fixture["context"].resolve()
        assert kwargs["as_of_date"].isoformat() == "2026-05-14"
        return FreshContextCaptureResult(
            context_path=fresh_context,
            checks=[
                DailyRefreshCheck(
                    code="source.fresh_capture_for_as_of",
                    passed=True,
                    detail=str(fresh_context),
                )
            ],
            references={"source_capture_context": str(fresh_context)},
            notes=["test fresh capture used"],
        )

    monkeypatch.setattr(
        daily_refresh_module,
        "_capture_fresh_context_for_as_of",
        fake_capture,
    )

    manifest = daily_refresh_module.run_validated_daily_refresh(
        as_of="2026-05-14",
        previous="none",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=fixture["workflow"],
        refresh_workflows=False,
    )

    assert manifest.status == "validated"
    assert manifest.references["context_bundle"] == str(fresh_context)
    assert manifest.references["source_capture_context"] == str(fresh_context)
    assert any(
        check.code == "source.fresh_capture_for_as_of" and check.passed
        for check in manifest.checks
    )


def test_fresh_capture_prunes_only_unconfigured_pipeshub_providers(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PIPESHUB_BASE_URL", raising=False)
    monkeypatch.delenv("PIPESHUB_BEARER_AUTH", raising=False)

    expected, skipped = daily_refresh_module._prune_expected_providers_for_env(
        {"onedrive", "outlook", "teams"}
    )

    assert expected == {"teams"}
    assert skipped == {"onedrive", "outlook"}


def test_fresh_capture_keeps_pipeshub_providers_when_token_exists(monkeypatch) -> None:
    monkeypatch.setenv("PIPESHUB_BEARER_AUTH", "token")

    expected, skipped = daily_refresh_module._prune_expected_providers_for_env(
        {"onedrive", "outlook", "teams"}
    )

    assert expected == {"onedrive", "outlook", "teams"}
    assert skipped == set()


def test_fresh_capture_runs_pipeshub_per_connector(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_capture(**kwargs):
        connectors = tuple(kwargs["connectors"])
        calls.append(connectors)
        provider = daily_refresh_module._normalize_provider_name(connectors[0])
        snapshot = daily_refresh_module.ContextSnapshot(
            organization_name=kwargs["organization_name"],
            organization_domain=kwargs["organization_domain"],
            captured_at="2026-05-15T12:00:00Z",
            sources=[
                ContextSourceResult(
                    provider=provider,
                    captured_at="2026-05-15T12:00:00Z",
                    record_counts={"records": 1},
                    data={"records": [{"id": f"{provider}-1"}]},
                )
            ],
        )
        return SimpleNamespace(snapshot=snapshot)

    monkeypatch.setattr(daily_refresh_module, "_load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(
        daily_refresh_module, "_pipeshub_capture_configured", lambda: True
    )
    monkeypatch.setattr(
        daily_refresh_module, "_teams_graph_capture_configured", lambda: False
    )
    monkeypatch.setattr(
        daily_refresh_module,
        "capture_pipeshub_context_from_env",
        fake_capture,
    )
    monkeypatch.setattr(
        daily_refresh_module,
        "write_pipeshub_capture_outputs",
        lambda *args, **kwargs: None,
    )

    workspace = tmp_path / "capture"
    workspace.mkdir()
    result = daily_refresh_module._run_live_context_capture(
        workspace=workspace,
        existing_context_payload={
            "organization_name": "Py Insights",
            "organization_domain": "py-insights.com",
        },
        as_of_date=daily_refresh_module.date(2026, 5, 15),
        expected_providers={"onedrive", "outlook"},
        capture_connectors=None,
        limit=5000,
        timeout_s=30,
        include_content=False,
    )

    assert calls == [("onedrive",), ("outlook",)]
    assert result.context_path is not None
    assert result.references["source_capture_pipeshub_onedrive_report"]
    assert result.references["source_capture_pipeshub_outlook_report"]
    provider_check = next(
        check
        for check in result.checks
        if check.code == "source.fresh_capture_expected_providers_present"
    )
    assert provider_check.passed is True


def test_latest_valid_ignores_current_run_for_skill_map_resolution(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=True)
    current_run = tmp_path / "daily" / "pyinsights_daily_20260513"
    current_skill_map = current_run / "skill_map" / "company_skill_map.json"
    current_skill_map.parent.mkdir(parents=True)
    current_skill_map.write_text(
        json.dumps(
            {
                "schema_version": "company_skill_map_v1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "generated_at": "2026-05-13T12:00:00+00:00",
                "source_ref": "current-run",
                "skill_count": 1,
                "skills": [
                    {
                        "skill_id": "skill-current",
                        "title": "Current run stale skill",
                        "summary": "Invalid current-run citation.",
                        "status": "draft",
                        "candidate_type": "workflow",
                        "trigger": {"description": "When release is planned."},
                        "goal": "Avoid using the current run as previous input.",
                        "evidence_refs": [
                            {"ref_type": "event", "ref_id": "missing-current-event"}
                        ],
                        "deployment_readiness": "shadow_ready",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (current_run / "validation_manifest.json").write_text(
        json.dumps({"status": "validated", "as_of": "2026-05-13"}) + "\n",
        encoding="utf-8",
    )

    manifest = run_validated_daily_refresh(
        as_of="2026-05-13",
        previous="latest-valid",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=fixture["workflow"],
        refresh_workflows=False,
        fresh_capture=False,
    )

    assert manifest.status == "validated"
    assert manifest.references["source_skill_map"] == str(
        fixture["skill_map"].resolve()
    )


def test_pyinsights_daily_refresh_cli_returns_nonzero_on_unvalidated_run(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=False)

    result = CliRunner().invoke(
        vei_app,
        [
            "pyinsights",
            "daily-refresh",
            "--as-of",
            "2026-05-13",
            "--previous",
            "none",
            "--context-bundle",
            str(fixture["context"]),
            "--output-root",
            str(tmp_path / "daily"),
            "--model-run-root",
            str(fixture["model"]),
            "--strategic-run-root",
            str(fixture["strategic"]),
            "--workflow-output",
            str(fixture["workflow"]),
            "--skill-map",
            str(fixture["skill_map"]),
            "--reuse-workflows",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "not_validated"
    assert "ceo_report" not in payload["artifacts"]


def test_validated_daily_refresh_rejects_one_off_descriptive_workflow_queue(
    tmp_path: Path,
) -> None:
    fixture = _write_daily_fixture(tmp_path, guard_passed=True)
    workflow_root = fixture["workflow"]
    (workflow_root / "workflow_candidates.json").write_text(
        json.dumps(
            {
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": "wfc-noisy",
                        "title": "Hi",
                        "source_event_ids": ["evt-1"],
                        "snippets": ["Release review evidence"],
                        "draft_task_spec": {
                            "task_id": "task-noisy",
                            "title": "Hi",
                            "evaluation_level": "descriptive",
                        },
                        "metadata": {"generated_by": "legacy_cluster_v1"},
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (workflow_root / "workflow_mining_manifest.json").write_text(
        json.dumps(
            {
                "selected_backend": "semantic",
                "semantic_candidate_count": 1,
                "published_candidate_count": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = run_validated_daily_refresh(
        as_of="2026-05-13",
        previous="none",
        context_bundle=fixture["context"],
        output_root=tmp_path / "daily",
        model_run_root=fixture["model"],
        strategic_run_root=fixture["strategic"],
        workflow_output=workflow_root,
        skill_map_path=fixture["skill_map"],
        refresh_workflows=False,
    )

    assert manifest.status == "not_validated"
    failed_codes = {
        check.code
        for check in manifest.checks
        if not check.passed and check.severity == "error"
    }
    assert "workflow.discovery_queue_has_multiple_candidates" in failed_codes
    assert "workflow.only_semantic_discovery_sources" in failed_codes
    assert "workflow.all_candidates_have_rubric_evaluable_task_specs" in failed_codes
    assert "workflow.workflow_titles_are_operating_patterns" in failed_codes


def _write_daily_fixture(tmp_path: Path, *, guard_passed: bool) -> dict[str, Path]:
    context_root = tmp_path / "context"
    context_root.mkdir()
    as_of = "2026-05-13T12:00:00+00:00"
    context_path = context_root / "context_snapshot.json"
    context_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "captured_at": as_of,
                "sources": [
                    {
                        "provider": "teams",
                        "captured_at": as_of,
                        "status": "ok",
                        "record_counts": {"messages": 2},
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    rows = [
        {
            "event_id": "evt-1",
            "timestamp": as_of,
            "ts_ms": 1715601600000,
            "surface": "teams",
            "provider": "teams",
            "kind": "chat_message",
            "domain": "comm_graph",
            "subject": "clients-web",
            "snippet": "clients-web",
        },
        {
            "event_id": "evt-2",
            "timestamp": as_of,
            "ts_ms": 1715601601000,
            "surface": "teams",
            "provider": "teams",
            "kind": "chat_message",
            "domain": "comm_graph",
            "subject": "clients-web",
            "snippet": "Follow-up",
        },
    ]
    (context_root / "canonical_event_index.json").write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "captured_at": as_of,
                "event_count": 2,
                "case_count": 0,
                "surface_counts": {"teams": 2},
                "rows": rows,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (context_root / "canonical_events.jsonl").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": "evt-1",
                "tenant_id": "pyinsights",
                "ts_ms": 1715601600000,
                "domain": "comm_graph",
                "kind": "chat_message",
            }
        )
        + "\n"
        + json.dumps(
            {
                "schema_version": 1,
                "event_id": "evt-2",
                "tenant_id": "pyinsights",
                "ts_ms": 1715601601000,
                "domain": "comm_graph",
                "kind": "chat_message",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (context_root / "context_verify_20260513.json").write_text(
        json.dumps({"ok": True, "checks": []}) + "\n",
        encoding="utf-8",
    )
    skill_map = context_root / "skill_map" / "company_skill_map.json"
    skill_map.parent.mkdir()
    skill_map.write_text(
        json.dumps(
            {
                "schema_version": "company_skill_map_v1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "generated_at": as_of,
                "source_ref": "test",
                "skill_count": 1,
                "skills": [
                    {
                        "skill_id": "skill-1",
                        "title": "Release gate",
                        "summary": "A cited release gate.",
                        "status": "draft",
                        "candidate_type": "workflow",
                        "trigger": {"description": "When release is planned."},
                        "goal": "Ship only after review.",
                        "evidence_refs": [{"ref_type": "event", "ref_id": "evt-1"}],
                        "deployment_readiness": "shadow_ready",
                        "owner": "ops",
                        "reviewer": "lead",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    workflow = tmp_path / "workflow"
    workflow.mkdir()
    (workflow / "workflow_candidates.json").write_text(
        json.dumps(
            {
                "candidate_count": 3,
                "candidates": [
                    _workflow_candidate(
                        "wfc-1",
                        "Release gate",
                        ["evt-1"],
                        level="rubric_evaluable",
                    ),
                    _workflow_candidate(
                        "wfc-2",
                        "Evidence-backed privacy review",
                        ["evt-2"],
                        level="rubric_evaluable",
                    ),
                    _workflow_candidate(
                        "wfc-3",
                        "Study handoff checklist",
                        ["evt-1", "evt-2"],
                        level="rubric_evaluable",
                    ),
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (workflow / "workflow_mining_manifest.json").write_text(
        json.dumps(
            {
                "selected_backend": "semantic",
                "semantic_candidate_count": 3,
                "published_candidate_count": 3,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    model = tmp_path / "model"
    (model / "dataset").mkdir(parents=True)
    (model / "model_runs" / "jepa_latent").mkdir(parents=True)
    (model / "target_manifests").mkdir()
    target_manifest = model / "target_manifests" / "pyinsights.json"
    target_manifest.write_text(
        json.dumps(
            {
                "supported_heads": ["cycle_time_ms"],
                "experimental_heads": ["release_readiness"],
                "unsupported_heads": ["liquidity_stress"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (model / "dataset" / "dataset_manifest.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "target_layer": {
                        "target_manifest_paths": {
                            "pyinsights": str(target_manifest.resolve())
                        },
                        "ranking_policy": (
                            "supported_structural_and_curated_heads_only"
                        ),
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (model / "model_runs" / "jepa_latent" / "model.pt").write_text(
        "stub", encoding="utf-8"
    )
    (model / "model_runs" / "jepa_latent" / "train_result.json").write_text(
        json.dumps({"train_loss": 1.0, "validation_loss": 1.1}) + "\n",
        encoding="utf-8",
    )
    (model / "model_runs" / "jepa_latent" / "eval_result.json").write_text(
        json.dumps(
            {"observed_metrics": {"target_family_mae": {"curated_semantic": 0.1}}}
        )
        + "\n",
        encoding="utf-8",
    )

    strategic = tmp_path / "strategic"
    strategic.mkdir()
    trusted = "true" if guard_passed else "false"
    (strategic / "strategic_state_point_results.csv").write_text(
        "case_id,ranking_basis,saturation_guard_trusted_for_ranking\n"
        f"case-1,supported_curated_targets,{trusted}\n",
        encoding="utf-8",
    )
    guard = {
        "status": "passed" if guard_passed else "failed",
        "trusted_for_daily_advice": guard_passed,
        "score_spread": 0.02 if guard_passed else 0.001,
        "reasons": [] if guard_passed else ["score spread too low"],
    }
    (strategic / "strategic_state_point_saturation_guard.json").write_text(
        json.dumps(guard) + "\n",
        encoding="utf-8",
    )
    (strategic / "strategic_state_point_results.json").write_text(
        json.dumps({"saturation_guard": guard}) + "\n",
        encoding="utf-8",
    )
    (strategic / "pyinsights_ceo_counterfactual_report_20260513.md").write_text(
        "# CEO report\n", encoding="utf-8"
    )

    return {
        "context": context_path,
        "skill_map": skill_map,
        "workflow": workflow,
        "model": model,
        "strategic": strategic,
    }


def _copy_context_with_capture_date(
    source_context: Path,
    destination_root: Path,
    *,
    captured_at: str,
) -> Path:
    shutil.copytree(source_context.parent, destination_root)
    context_path = destination_root / "context_snapshot.json"
    context_payload = json.loads(context_path.read_text(encoding="utf-8"))
    context_payload["captured_at"] = captured_at
    for source in context_payload.get("sources", []) or []:
        if isinstance(source, dict):
            source["captured_at"] = captured_at
    context_path.write_text(json.dumps(context_payload) + "\n", encoding="utf-8")

    index_path = destination_root / "canonical_event_index.json"
    index_payload = json.loads(index_path.read_text(encoding="utf-8"))
    index_payload["captured_at"] = captured_at
    index_path.write_text(json.dumps(index_payload) + "\n", encoding="utf-8")
    (destination_root / "context_verify_20260514.json").write_text(
        json.dumps({"ok": True, "checks": []}) + "\n",
        encoding="utf-8",
    )
    return context_path


def _workflow_candidate(
    candidate_id: str,
    title: str,
    event_ids: list[str],
    *,
    level: str,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "title": title,
        "source_event_ids": event_ids,
        "source_case_ids": [f"case-{candidate_id}"],
        "rank_score": 90.0,
        "snippets": [f"{title} cited evidence"],
        "draft_task_spec": {
            "task_id": f"task-{candidate_id}",
            "title": title,
            "objective": f"Review {title.lower()} with cited evidence.",
            "evaluation_level": level,
            "metadata": {
                "claim_boundary": (
                    "semantic workflow candidate synthesized from citation-backed "
                    "skill evidence; requires human review before activation"
                )
            },
        },
        "metadata": {
            "generated_by": "skillmap_semantic_v1",
            "deployment_readiness": "shadow_ready",
        },
    }
