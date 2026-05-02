"""CLI integration tests for `vei wiki`."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei_wiki import app
from vei.context.api import ContextSnapshot, ContextSourceResult


def _write_synthetic_snapshot(target: Path) -> Path:
    snapshot = ContextSnapshot(
        organization_name="Acme Renewals",
        organization_domain="acme.example",
        captured_at="2026-04-01T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="granola",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "transcripts": [
                        {
                            "id": "G-1",
                            "title": "CASE-456 weekly sync",
                            "body": "Discussed renewal blocker; legal sign-off pending.",
                            "owner": "alice@acme.example",
                            "updated_at": "2026-03-30T15:00:00Z",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="slack",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "renewals",
                            "messages": [
                                {
                                    "ts": "1712017800",
                                    "user": "alice",
                                    "text": "CASE-456 renewal blocker review",
                                }
                            ],
                        }
                    ],
                    "users": [
                        {"id": "alice", "email": "alice@acme.example", "name": "Alice"}
                    ],
                },
            ),
        ],
    )
    target.mkdir(parents=True, exist_ok=True)
    snapshot_path = target / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    return snapshot_path


def test_wiki_cli_build_writes_json_and_markdown(tmp_path: Path) -> None:
    source_dir = tmp_path / "context"
    _write_synthetic_snapshot(source_dir)
    output_dir = tmp_path / "wiki"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "build",
            "--source-dir",
            str(source_dir),
            "--output",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_dir / "company_wiki.json").exists()
    assert (output_dir / "index.md").exists()
    assert (output_dir / "wiki_build_report.json").exists()
    pages_dir = output_dir / "pages"
    expected_page_ids = {
        "overview",
        "recent-changes",
        "cases",
        "people",
        "knowledge",
        "skills",
        "evidence-index",
    }
    written = {path.stem for path in pages_dir.glob("*.md")}
    assert expected_page_ids == written


def test_wiki_cli_refresh_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _write_synthetic_snapshot(workspace)
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["refresh", "--workspace", str(workspace)],
    )
    assert result.exit_code == 0, result.output
    artifacts = workspace / ".artifacts" / "wiki"
    assert (artifacts / "company_wiki.json").exists()
    report_payload = json.loads(
        (artifacts / "wiki_build_report.json").read_text(encoding="utf-8")
    )
    assert report_payload["status"] in {"ok", "partial"}
    assert report_payload["page_count"] == 7


def test_wiki_cli_query_text_and_json(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _write_synthetic_snapshot(workspace)
    runner = CliRunner()

    text_result = runner.invoke(
        app,
        [
            "query",
            "knowledge",
            "--workspace",
            str(workspace),
            "--limit",
            "5",
        ],
    )
    assert text_result.exit_code == 0, text_result.output
    assert "knowledge" in text_result.output.lower()

    json_result = runner.invoke(
        app,
        [
            "query",
            "knowledge",
            "--workspace",
            str(workspace),
            "--format",
            "json",
        ],
    )
    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert payload["query"] == "knowledge"
    assert payload["total_hits"] >= 1


def test_wiki_cli_query_requires_source(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["query", "anything"])
    assert result.exit_code != 0
    combined = (result.output or "") + (
        str(result.exception) if result.exception else ""
    )
    assert "wiki" in combined.lower() or "workspace" in combined.lower()


def test_wiki_cli_refresh_reports_missing_context(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    runner = CliRunner()
    result = runner.invoke(app, ["refresh", "--workspace", str(workspace)])
    assert result.exit_code != 0
