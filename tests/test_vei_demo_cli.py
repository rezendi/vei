"""Tests for vei demo build CLI command."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from vei.cli.vei_demo import app

runner = CliRunner()


def test_demo_build_creates_simulation_workspace(tmp_path: Path) -> None:
    output = tmp_path / "pinnacle_demo"
    result = runner.invoke(
        app, ["build", "--vertical", "b2b_saas", "--output", str(output)]
    )
    assert result.exit_code == 0, result.output

    manifest_path = output / "vei_project.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())

    assert manifest["title"] == "Pinnacle Analytics"
    assert manifest["metadata"].get("workspace_mode") == "simulation"
    assert "customer_twin" not in manifest.get("metadata", {})


def test_demo_build_context_snapshot_exists(tmp_path: Path) -> None:
    output = tmp_path / "pinnacle_demo"
    result = runner.invoke(
        app, ["build", "--vertical", "b2b_saas", "--output", str(output)]
    )
    assert result.exit_code == 0, result.output

    snapshot_path = output / "context_snapshot.json"
    assert snapshot_path.exists()
