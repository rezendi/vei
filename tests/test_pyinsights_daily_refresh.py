from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei import app as vei_app
from vei.pyinsights.daily_refresh import run_validated_daily_refresh


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
        {"event_id": "evt-1", "surface": "teams", "provider": "teams"},
        {"event_id": "evt-2", "surface": "teams", "provider": "teams"},
    ]
    (context_root / "canonical_event_index.json").write_text(
        json.dumps(
            {
                "version": "1",
                "captured_at": as_of,
                "event_count": 2,
                "surface_counts": {"teams": 2},
                "rows": rows,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (context_root / "canonical_events.jsonl").write_text(
        '{"event_id":"evt-1"}\n{"event_id":"evt-2"}\n',
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
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": "wfc-1",
                        "title": "Release gate",
                        "source_event_ids": ["evt-1"],
                        "snippets": ["Release review evidence"],
                        "metadata": {"generated_by": "skillmap_semantic_v1"},
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (workflow / "workflow_mining_manifest.json").write_text(
        json.dumps(
            {
                "semantic_candidate_count": 1,
                "structural_candidate_count": 2,
                "raw_structural_clusters_are_diagnostics": True,
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
