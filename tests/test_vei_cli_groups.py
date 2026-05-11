from __future__ import annotations

from pathlib import Path

import typer.testing

from vei.cli.vei import app


def test_root_help_shows_grouped_top_level_commands() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, result.output
    assert "workspace" in result.output
    assert "admin" in result.output
    assert "knowledge" in result.output
    assert "workflow" in result.output
    lines = result.output.splitlines()
    assert not any("│ project" in line for line in lines)
    assert not any("│ twin" in line for line in lines)


def test_hidden_legacy_command_aliases_still_work() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(app, ["project", "--help"])
    assert result.exit_code == 0, result.output
    assert "Create, import, review, and compile VEI workspaces." in result.output


def test_eval_demo_command_is_benchmark_demo(tmp_path: Path) -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        app,
        [
            "eval",
            "demo",
            "--family",
            "security_containment",
            "--artifacts-root",
            str(tmp_path),
            "--run-id",
            "root_eval_demo",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Starting benchmark demo for security_containment" in result.output
    assert (tmp_path / "root_eval_demo" / "demo_result.json").exists()


def test_eval_agent_demo_group_keeps_lightweight_demo_commands() -> None:
    runner = typer.testing.CliRunner()
    result = runner.invoke(
        app,
        ["eval", "agent-demo", "--help"],
        env={"NO_COLOR": "1", "COLUMNS": "160"},
    )

    assert result.exit_code == 0, result.output
    assert "build" in result.output
    assert "run" in result.output
