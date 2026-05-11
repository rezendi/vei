from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import typer.testing

from vei.cli.vei_llm_test import (
    _build_base_prompt,
    _build_common_hints,
    _build_stdio_server_parameters,
    _episode_failure_exit_code,
    _full_flow_progress,
    _is_infrastructure_failure_message,
    _normalize_result,
    _select_progress_action,
    _select_visible_tools,
    _should_use_strict_procurement_flow,
    _should_bypass_strict_planning,
    _strict_full_flow_action,
    _strict_full_flow_complete,
    _tool_progress_text,
    EpisodeFailure,
    app as llm_app,
    run_episode,
)


class _FakeResult:
    def model_dump(self) -> dict:
        return {
            "structuredContent": None,
            "content": [
                {"type": "text", "text": '{"result": {"id": "D-1"}, "ok": true}'}
            ],
            "isError": False,
        }


def test_normalize_result_parses_text_json_payload() -> None:
    normalized = _normalize_result(_FakeResult())
    assert normalized == {"result": {"id": "D-1"}, "ok": True}


def test_select_progress_action_prefers_non_observe_with_args() -> None:
    action_menu = [
        {"tool": "vei.observe", "args": {}},
        {"tool": "browser.click", "args": {"node_id": "CLICK:open_pdp#0"}},
        {"tool": "mail.list", "args": {}},
    ]
    tool, args = _select_progress_action(action_menu) or ("", {})
    assert tool == "browser.click"
    assert args == {"node_id": "CLICK:open_pdp#0"}


def test_task_prompt_uses_generic_system_prompt() -> None:
    prompt = _build_base_prompt("Contain the malicious OAuth app.")

    assert "Contain the malicious OAuth app" in prompt
    assert "MacroBook" not in prompt
    assert "when the task asks for evidence preservation" in prompt


def test_common_hints_are_task_specific() -> None:
    security_hints = _build_common_hints(
        8,
        task=(
            "Contain the malicious OAuth app with evidence preservation and "
            "notification decision."
        ),
    )
    procurement_hints = _build_common_hints(8)

    assert security_hints["google_admin.get_oauth_app"] == {"app_id": "OAUTH-9001"}
    assert security_hints["slack.send_message"]["channel"] == "#security-incident"
    assert "google_admin.get_oauth_app" not in procurement_hints
    assert procurement_hints["slack.send_message"]["channel"] == "#procurement"


def test_generic_task_hints_do_not_include_procurement_examples() -> None:
    hints = _build_common_hints(
        8,
        task="Reduce checkout incident impact and update the revenue war room.",
    )

    assert "slack.send_message" not in hints
    assert "mail.compose" not in hints
    assert "docs.create" not in hints


def test_visible_tools_keep_hinted_tools_when_top_k_is_small() -> None:
    visible = _select_visible_tools(
        available=[
            "vei.observe",
            "google_admin.preserve_oauth_evidence",
            "docs.update",
            "browser.read",
        ],
        action_menu=[],
        search_matches=[],
        baseline=[
            "vei.observe",
            "google_admin.preserve_oauth_evidence",
            "docs.update",
        ],
        top_k=1,
    )

    assert "google_admin.preserve_oauth_evidence" in visible
    assert "docs.update" in visible


def test_visible_tools_preserve_baseline_order_for_stable_prompts() -> None:
    visible = _select_visible_tools(
        available=[
            "docs.update",
            "vei.observe",
            "google_admin.preserve_oauth_evidence",
            "browser.read",
        ],
        action_menu=[],
        search_matches=[],
        baseline=[
            "vei.observe",
            "google_admin.preserve_oauth_evidence",
            "docs.update",
        ],
        top_k=3,
    )

    assert visible == [
        "vei.observe",
        "google_admin.preserve_oauth_evidence",
        "docs.update",
    ]


def test_tool_progress_text_lists_remaining_hinted_tools() -> None:
    progress = _tool_progress_text(
        [
            (
                'action 1: {"tool": "google_admin.preserve_oauth_evidence", '
                '"args": {"app_id": "OAUTH-9001"}}'
            )
        ],
        {
            "google_admin.preserve_oauth_evidence": {},
            "docs.update": {},
            "jira.add_comment": {},
        },
    )

    assert "google_admin.preserve_oauth_evidence x1" in progress
    assert "docs.update" in progress
    assert "jira.add_comment" in progress


def test_tool_progress_text_tracks_workflow_argument_hints() -> None:
    task = "\n".join(
        [
            "Known tool argument hints. Use these IDs:",
            '- vei.graph_action: {"action": "assign_application", "args": {"app_id": "APP-crm", "user_id": "USR-ACQ-1"}, "domain": "identity_graph"}',
            '- vei.graph_action: {"action": "restrict_drive_share", "args": {"doc_id": "GDRIVE-2201", "visibility": "internal"}, "domain": "doc_graph"}',
            '- docs.update: {"doc_id": "CUTOVER-2201", "body": "done"}',
        ]
    )
    progress = _tool_progress_text(
        [
            (
                'action 1: {"tool": "vei.graph_action", "args": {"domain": '
                '"identity_graph", "action": "assign_application", "args": '
                '{"user_id": "USR-ACQ-1", "app_id": "APP-crm"}}}'
            )
        ],
        {"vei.graph_action": {}, "docs.update": {}},
        task=task,
    )

    assert "Remaining workflow argument hints" in progress
    assert "restrict_drive_share" in progress
    assert "CUTOVER-2201" in progress
    assert "assign_application" not in progress


def test_stdio_server_parameters_put_state_under_artifacts(tmp_path: Path) -> None:
    artifacts = tmp_path / "llm"
    params = _build_stdio_server_parameters(
        dataset_path=None, artifacts_dir=str(artifacts)
    )

    assert params.env is not None
    assert params.env["VEI_ARTIFACTS_DIR"] == str(artifacts)
    assert params.env["VEI_STATE_DIR"] == str(artifacts)


def test_strict_full_flow_only_applies_to_procurement_scenarios() -> None:
    assert _should_use_strict_procurement_flow(
        score_success_mode="full",
        task=None,
        scenario_name="multi_channel",
    )
    assert not _should_use_strict_procurement_flow(
        score_success_mode="full",
        task="Contain a malicious OAuth app.",
        scenario_name="oauth_app_containment",
    )
    assert not _should_use_strict_procurement_flow(
        score_success_mode="email",
        task=None,
        scenario_name="multi_channel",
    )


def test_strict_full_flow_action_advances_missing_enterprise_steps() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {
                    "body_text": "Quote is $2999 and ETA is 5 business days.",
                },
            }
        },
    ]
    progress = _full_flow_progress(transcript)
    action = _strict_full_flow_action(progress)
    assert action is not None
    assert action[0] == "docs.create"


def test_strict_full_flow_action_uses_discovered_ids_for_ticket_and_crm() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {"body_text": "Price $2999 ETA 5 days"},
            }
        },
        {"action": {"tool": "docs.create", "args": {}, "result": {"doc_id": "D-1"}}},
        {
            "action": {
                "tool": "tickets.list",
                "args": {},
                "result": {"tickets": [{"ticket_id": "TCK-77"}]},
            }
        },
    ]
    progress = _full_flow_progress(transcript)
    ticket_action = _strict_full_flow_action(progress)
    assert ticket_action is not None
    assert ticket_action[0] == "tickets.update"
    assert ticket_action[1]["ticket_id"] == "TCK-77"

    transcript.append(
        {
            "action": {
                "tool": "tickets.update",
                "args": {"ticket_id": "TCK-77"},
                "result": {"ok": True},
            }
        }
    )
    transcript.append(
        {
            "action": {
                "tool": "crm.list_deals",
                "args": {},
                "result": {"deals": [{"id": "D-301"}]},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_action = _strict_full_flow_action(progress)
    assert crm_action is not None
    assert crm_action[0] == "crm.log_activity"
    assert crm_action[1]["deal_id"] == "D-301"


def test_strict_full_flow_action_uses_default_ids_when_missing() -> None:
    transcript = [
        {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}},
        {
            "action": {
                "tool": "slack.send_message",
                "args": {"text": "Budget $2999 approved"},
                "result": {"ts": "1"},
            }
        },
        {
            "action": {
                "tool": "mail.compose",
                "args": {"to": "sales@macrocompute.example"},
                "result": {"id": "m1"},
            }
        },
        {
            "action": {
                "tool": "mail.open",
                "args": {"id": "m2"},
                "result": {"body_text": "Price $2999 ETA 5 days"},
            }
        },
        {"action": {"tool": "docs.create", "args": {}, "result": {"doc_id": "D-1"}}},
    ]
    progress = _full_flow_progress(transcript)
    ticket_action = _strict_full_flow_action(progress)
    assert ticket_action is not None
    assert ticket_action[0] == "tickets.create"

    transcript.append(
        {
            "action": {
                "tool": "tickets.create",
                "args": {"title": "Quote follow-up"},
                "result": {"ticket_id": "TCK-88"},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    ticket_update_action = _strict_full_flow_action(progress)
    assert ticket_update_action is not None
    assert ticket_update_action[0] == "tickets.update"
    assert ticket_update_action[1]["ticket_id"] == "TCK-88"

    progress = _full_flow_progress(transcript)
    crm_action = _strict_full_flow_action(progress)
    assert crm_action is not None
    assert crm_action[0] == "tickets.update"

    transcript.append(
        {
            "action": {
                "tool": "tickets.update",
                "args": {"ticket_id": "TCK-88"},
                "result": {"ok": True},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_create_action = _strict_full_flow_action(progress)
    assert crm_create_action is not None
    assert crm_create_action[0] == "crm.create_deal"

    transcript.append(
        {
            "action": {
                "tool": "crm.create_deal",
                "args": {"name": "Deal"},
                "result": {"id": "D-902"},
            }
        }
    )
    progress = _full_flow_progress(transcript)
    crm_log_action = _strict_full_flow_action(progress)
    assert crm_log_action is not None
    assert crm_log_action[0] == "crm.log_activity"
    assert crm_log_action[1]["deal_id"] == "D-902"


def test_strict_full_flow_complete_requires_all_full_subgoals() -> None:
    progress = {
        "citations": True,
        "approval_with_amount": True,
        "email_sent": True,
        "email_parsed": True,
        "doc_logged": True,
        "ticket_updated": True,
        "crm_logged": True,
    }
    assert _strict_full_flow_complete(progress) is True
    progress["crm_logged"] = False
    assert _strict_full_flow_complete(progress) is False


def test_should_bypass_strict_planning_only_after_quote_is_parsed() -> None:
    early_progress = {
        "citations": True,
        "approval_with_amount": True,
        "email_sent": True,
        "email_parsed": False,
        "doc_logged": False,
        "ticket_updated": False,
        "crm_logged": False,
    }
    late_progress = {
        **early_progress,
        "email_parsed": True,
    }

    assert (
        _should_bypass_strict_planning(
            early_progress,
            (
                "docs.create",
                {"title": "Vendor quote summary"},
            ),
        )
        is False
    )
    assert (
        _should_bypass_strict_planning(
            late_progress,
            (
                "docs.create",
                {"title": "Vendor quote summary"},
            ),
        )
        is True
    )
    assert (
        _should_bypass_strict_planning(
            late_progress,
            (
                "mail.list",
                {},
            ),
        )
        is False
    )


def test_llm_harness_classifies_infrastructure_failures() -> None:
    exc = EpisodeFailure("Episode failed: rate limit", transcript=[])
    exc.__cause__ = RuntimeError("429 rate limit")

    assert _is_infrastructure_failure_message("Provider timed out") is True
    assert _is_infrastructure_failure_message("Schema mismatch") is False
    assert _episode_failure_exit_code(exc) == 3


def test_llm_cli_writes_summary_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def _fake_run_episode(**kwargs):
        artifacts_dir = Path(kwargs["artifacts_dir"])
        (artifacts_dir / "trace.jsonl").write_text(
            json.dumps({"type": "call", "time_ms": 10}) + "\n",
            encoding="utf-8",
        )
        metrics_path = Path(kwargs["metrics_path"])
        metrics_path.write_text(
            json.dumps(
                {
                    "calls": 1,
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                    "estimated_cost_usd": 0.12,
                    "latency_p95_ms": 25,
                }
            ),
            encoding="utf-8",
        )
        return [
            {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}}
        ]

    monkeypatch.setattr("vei.cli.vei_llm_test.run_episode", _fake_run_episode)
    monkeypatch.setattr(
        "vei.cli.vei_llm_test.compute_score",
        lambda artifacts_dir, success_mode: {"success": True, "costs": {"actions": 1}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    runner = typer.testing.CliRunner()
    artifacts = tmp_path / "artifacts"
    result = runner.invoke(
        llm_app,
        [
            "--provider",
            "openai",
            "--model",
            "gpt-5",
            "--artifacts",
            str(artifacts),
            "--no-print-transcript",
        ],
    )

    assert result.exit_code == 0, result.output
    summary_payload = json.loads(
        (artifacts / "summary.json").read_text(encoding="utf-8")
    )
    assert summary_payload["summary"]["success"] is True
    assert summary_payload["summary"]["llm_calls"] == 1
    assert summary_payload["summary"]["total_tokens"] == 18


def test_llm_cli_writes_summary_artifact_on_require_success_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def _fake_run_episode(**kwargs):
        artifacts_dir = Path(kwargs["artifacts_dir"])
        (artifacts_dir / "trace.jsonl").write_text(
            json.dumps({"type": "call", "time_ms": 10}) + "\n",
            encoding="utf-8",
        )
        metrics_path = Path(kwargs["metrics_path"])
        metrics_path.write_text(
            json.dumps(
                {
                    "calls": 1,
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "estimated_cost_usd": None,
                    "latency_p95_ms": 10,
                }
            ),
            encoding="utf-8",
        )
        return [
            {"action": {"tool": "browser.read", "args": {}, "result": {"url": "u"}}}
        ]

    monkeypatch.setattr("vei.cli.vei_llm_test.run_episode", _fake_run_episode)
    monkeypatch.setattr(
        "vei.cli.vei_llm_test.compute_score",
        lambda artifacts_dir, success_mode: {"success": False, "costs": {"actions": 1}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    runner = typer.testing.CliRunner()
    artifacts = tmp_path / "artifacts"
    result = runner.invoke(
        llm_app,
        [
            "--provider",
            "openai",
            "--model",
            "gpt-5",
            "--artifacts",
            str(artifacts),
            "--require-success",
            "--no-print-transcript",
        ],
    )

    assert result.exit_code == 1, result.output
    summary_payload = json.loads(
        (artifacts / "summary.json").read_text(encoding="utf-8")
    )
    assert summary_payload["summary"]["success"] is False
    assert summary_payload["summary"]["total_tokens"] == 5


@pytest.mark.anyio
async def test_run_episode_defaults_stdio_log_level_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    @asynccontextmanager
    async def _fake_stdio_client(params):
        captured.update(params.env or {})
        yield (object(), object())

    class _FakeSession:
        def __init__(self, read, write):
            del read, write

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        async def initialize(self):
            return None

        async def list_tools(self):
            class _Tools:
                tools = []

            return _Tools()

    monkeypatch.delenv("FASTMCP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FASTMCP_DEBUG", raising=False)
    monkeypatch.setattr("vei.cli.vei_llm_test.stdio_client", _fake_stdio_client)
    monkeypatch.setattr("vei.cli.vei_llm_test.ClientSession", _FakeSession)

    transcript = await run_episode(
        model="gpt-5",
        sse_url="",
        max_steps=0,
        provider="openai",
    )

    assert transcript == []
    assert captured["FASTMCP_LOG_LEVEL"] == "ERROR"
    assert captured["FASTMCP_DEBUG"] == "0"
