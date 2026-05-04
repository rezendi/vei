from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from vei.cli.vei_skillmap import app
from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.ingest.agent_activity.agent_activity_jsonl import AgentActivityJsonlAdapter
from vei.ingest.agent_activity.api import ingest_agent_activity
from vei.skillmap.api import CompanySkillMap


def test_skillmap_cli_builds_outputs_and_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_cli_llm(monkeypatch)
    dotenv_calls: list[bool] = []
    monkeypatch.setattr(
        "vei.cli.vei_skillmap.load_dotenv",
        lambda *args, **kwargs: dotenv_calls.append(bool(kwargs.get("override"))),
    )
    snapshot_path = _write_cli_snapshot(tmp_path)
    output_dir = tmp_path / "skillmap"
    runner = CliRunner()

    build_result = runner.invoke(
        app,
        [
            "build",
            "--source-dir",
            str(snapshot_path),
            "--output",
            str(output_dir),
            "--limit",
            "6",
        ],
    )

    assert build_result.exit_code == 0, build_result.output
    assert dotenv_calls == [False]
    map_path = output_dir / "company_skill_map.json"
    assert map_path.exists()
    assert (output_dir / "company_skills.md").exists()
    assert (output_dir / "skill_evidence_report.md").exists()
    assert (output_dir / "skill_replay_report.md").exists()
    assert (output_dir / "skill_refresh_report.md").exists()
    assert (output_dir / "skill_gap_report.md").exists()

    validate_result = runner.invoke(app, ["validate", "--map", str(map_path)])

    assert validate_result.exit_code == 0, validate_result.output
    payload = json.loads(validate_result.output)
    assert payload["ok"] is True
    assert payload["draft_skill_count"] >= 1


def test_skillmap_cli_reports_llm_failures_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_path = _write_cli_snapshot(tmp_path)
    monkeypatch.chdir(tmp_path)

    def fail_build(*_args: object, **_kwargs: object) -> CompanySkillMap:
        raise TimeoutError("codex call timed out")

    monkeypatch.setattr(
        "vei.cli.vei_skillmap.build_company_skill_map_from_context_path",
        fail_build,
    )

    result = CliRunner().invoke(
        app,
        [
            "build",
            "--source-dir",
            str(snapshot_path),
            "--output",
            str(tmp_path / "skillmap"),
        ],
    )

    assert result.exit_code == 1
    assert "Skill map build failed: codex call timed out" in result.output
    assert "--provider codex --model gpt-5.3-codex-spark" in result.output
    assert "Traceback" not in result.output
    assert (tmp_path / ".artifacts" / "skillmap_build_error.txt").exists()


def test_skillmap_cli_refreshes_workspace_control_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_skillmap_cli_llm(monkeypatch)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _write_cli_snapshot(workspace)
    activity_path = tmp_path / "agent_activity.jsonl"
    activity_path.write_text(
        json.dumps(
            {
                "id": "agent-draft-456",
                "ts_ms": 1767272400000,
                "case_id": "case:CASE-456",
                "actor_id": "renewal.agent",
                "tool": "docs.create",
                "args": {"doc_id": "DOC-CASE-456-DRAFT"},
                "response": {"doc_id": "DOC-CASE-456-DRAFT"},
                "status": "completed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    ingest_agent_activity(
        adapter=AgentActivityJsonlAdapter(activity_path, tenant_id="acme.example"),
        workspace=workspace,
    )
    output_dir = tmp_path / "skillmap"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "refresh",
            "--workspace",
            str(workspace),
            "--output",
            str(output_dir),
            "--no-replay",
            "--limit",
            "4",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "from 1 Control event(s)" in result.output
    map_path = output_dir / "company_skill_map.json"
    assert map_path.exists()
    assert (output_dir / "control_evidence_pack.json").exists()
    payload = json.loads(map_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["builder"] == "workspace_control"
    assert payload["metadata"]["control_event_count"] == 1


def test_skillmap_cli_validate_exits_nonzero_for_invalid_map(tmp_path: Path) -> None:
    map_path = tmp_path / "company_skill_map.json"
    skill_map = CompanySkillMap(
        organization_name="Acme Ops",
        generated_at="2026-01-01T00:00:00Z",
        source_ref="unit-test",
    )
    map_path.write_text(
        json.dumps(skill_map.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(app, ["validate", "--map", str(map_path)])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["error_count"] >= 1


def _patch_skillmap_cli_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_plan_once_with_usage(**kwargs: object) -> SimpleNamespace:
        payload = json.loads(str(kwargs["user"]))
        if "evidence_catalog" in payload:
            evidence_ids = [
                str(item["evidence_id"])
                for item in payload["evidence_catalog"]
                if str(item.get("evidence_id") or "")
            ]
            return SimpleNamespace(
                plan={
                    "tool": "skillmap.cluster",
                    "args": {
                        "clusters": [
                            {
                                "title": "CASE-456 renewal review cluster",
                                "summary": "The same renewal risk appears across mail, Slack, and docs.",
                                "candidate_type": "flagship_skill",
                                "domain": "renewal_ops",
                                "positive_triggers": ["CASE-456 renewal risk"],
                                "negative_triggers": ["Renewal thread with no blocker"],
                                "reuse_pattern": "Renewal risks repeat across mail, Slack, and docs.",
                                "evidence_ids": evidence_ids[:5],
                                "allowed_actions": ["review_case_timeline"],
                                "blocked_actions": ["live_write_without_approval"],
                                "output_artifacts": [
                                    {
                                        "artifact_id": "renewal_review",
                                        "title": "Renewal review memo",
                                        "kind": "markdown",
                                        "schema_hint": "case, evidence, owner, next action",
                                    }
                                ],
                                "replay_checks": [
                                    "Future events should include internal review before external response."
                                ],
                                "usefulness_scores": {
                                    "company_specificity": 0.8,
                                    "repeat_frequency": 0.7,
                                    "business_consequence": 0.8,
                                    "actionability": 0.9,
                                    "evidence_coverage": 0.8,
                                    "risk_if_wrong": 0.6,
                                    "replay_testability": 0.8,
                                },
                                "confidence": 0.8,
                            }
                        ]
                    },
                },
                usage=SimpleNamespace(
                    provider="openai",
                    model="test-model",
                    prompt_tokens=8,
                    completion_tokens=16,
                    total_tokens=24,
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
                            "title": "Coordinate CASE-456 renewal review",
                            "summary": "Use the renewal mail thread, Slack blocker, and SOP before drafting an internal update.",
                            "candidate_type": "flagship_skill",
                            "domain": "renewal_ops",
                            "trigger": {
                                "description": "CASE-456 appears in renewal mail, Slack, and docs.",
                                "signals": ["CASE-456", "#renewals"],
                            },
                            "negative_triggers": ["Renewal thread with no blocker"],
                            "goal": "Prepare a cited internal renewal-risk update.",
                            "reuse_pattern": "Renewal risks repeat across mail, Slack, and docs.",
                            "evidence_ids": evidence_ids[:5],
                            "steps": [
                                {
                                    "instruction": "Read the cited renewal evidence.",
                                    "tool": "vei.structure_view",
                                    "read_only": True,
                                }
                            ],
                            "output_artifacts": [
                                {
                                    "artifact_id": "renewal_review",
                                    "title": "Renewal review memo",
                                    "kind": "markdown",
                                    "schema_hint": "case, evidence, owner, next action",
                                }
                            ],
                            "replay_checks": [
                                "Future events should include internal review before external response."
                            ],
                            "allowed_actions": ["review_case_timeline"],
                            "blocked_actions": ["live_write_without_approval"],
                            "execution_mode": "read_only",
                            "tags": ["renewal", "CASE-456"],
                            "usefulness_scores": {
                                "company_specificity": 0.8,
                                "repeat_frequency": 0.7,
                                "business_consequence": 0.8,
                                "actionability": 0.9,
                                "evidence_coverage": 0.8,
                                "risk_if_wrong": 0.6,
                                "replay_testability": 0.8,
                            },
                            "confidence": 0.8,
                        }
                    ]
                },
            },
            usage=SimpleNamespace(
                provider="openai",
                model="test-model",
                prompt_tokens=8,
                completion_tokens=16,
                total_tokens=24,
                estimated_cost_usd=0.01,
            ),
        )

    monkeypatch.setattr(
        "vei.skillmap.api.plan_once_with_usage", fake_plan_once_with_usage
    )
    monkeypatch.setattr("vei.skillmap.api._llm_available", lambda provider: True)


def _write_cli_snapshot(tmp_path: Path) -> Path:
    snapshot = ContextSnapshot(
        organization_name="Acme Ops",
        organization_domain="acme.example",
        captured_at="2026-01-03T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2026-01-03T00:00:00Z",
                data={
                    "threads": [
                        {
                            "thread_id": "thr-CASE-456",
                            "subject": "CASE-456 renewal risk",
                            "messages": [
                                {
                                    "from": "maya@acme.example",
                                    "to": ["legal@acme.example"],
                                    "date": "2026-01-01T10:00:00Z",
                                    "body_text": "CASE-456 renewal risk needs review.",
                                },
                                {
                                    "from": "legal@acme.example",
                                    "to": ["maya@acme.example"],
                                    "date": "2026-01-01T10:30:00Z",
                                    "body_text": "CASE-456 review is complete.",
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="slack",
                captured_at="2026-01-03T00:00:00Z",
                data={
                    "channels": [
                        {
                            "channel": "#renewals",
                            "messages": [
                                {
                                    "user": "maya@acme.example",
                                    "ts": "2026-01-01T11:00:00Z",
                                    "text": "CASE-456 renewal risk is blocked.",
                                }
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="notion",
                captured_at="2026-01-03T00:00:00Z",
                data={
                    "pages": [
                        {
                            "page_id": "page-456",
                            "title": "CASE-456 renewal SOP",
                            "body": "Review renewal risks with legal.",
                            "linked_object_refs": ["case:CASE-456"],
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
    return snapshot_path
