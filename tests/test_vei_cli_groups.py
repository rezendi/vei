from __future__ import annotations

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
