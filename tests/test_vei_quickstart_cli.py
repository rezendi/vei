from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from vei.cli.vei import app
from vei.cli import vei_quickstart


def test_quickstart_reports_invalid_live_demo_combo(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    root = tmp_path / "quickstart_workspace"

    fake_state = SimpleNamespace(
        mission=SimpleNamespace(mission_name="service_day_collision"),
        run_id="human_play_123",
    )

    monkeypatch.setattr(
        "vei.playable.prepare_playable_workspace",
        lambda *args, **kwargs: fake_state,
    )
    monkeypatch.setattr(
        vei_quickstart,
        "_ensure_twin_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("mirror demo mode requires connector_mode='sim'")
        ),
    )

    result = runner.invoke(
        app,
        [
            "quickstart",
            "run",
            "--world",
            "service_ops",
            "--root",
            str(root),
            "--mirror-demo",
            "--connector-mode",
            "live",
            "--no-baseline",
        ],
    )

    assert result.exit_code == 2
    assert "mirror demo mode requires connector_mode='sim'" in result.output
    assert "Traceback" not in result.output
