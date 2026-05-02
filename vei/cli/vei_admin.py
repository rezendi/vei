from __future__ import annotations

import typer

from vei.cli.vei_blueprint import app as blueprint_app
from vei.cli.vei_contract import app as contract_app
from vei.cli.vei_doctor import app as doctor_app
from vei.cli.vei_quickstart import app as quickstart_app
from vei.cli.vei_release import app as release_app
from vei.cli.vei_report import app as report_app
from vei.cli.vei_synthesize import app as synthesize_app
from vei.cli.vei_visualize import app as visualize_app
from vei.cli.vei_world import app as world_app

app = typer.Typer(
    help="Run operator, release, reporting, and platform maintenance commands.",
    no_args_is_help=True,
)

app.add_typer(doctor_app, name="doctor")
app.add_typer(quickstart_app, name="quickstart")
app.add_typer(release_app, name="release")
app.add_typer(report_app, name="report")
app.add_typer(
    synthesize_app,
    name="synthesize",
    help="Generate synthesis configs and runbooks.",
)
app.add_typer(
    visualize_app,
    name="visualize",
    help="Render visualization artifacts from runs and traces.",
)
app.add_typer(blueprint_app, name="blueprint")
app.add_typer(contract_app, name="contract")
app.add_typer(world_app, name="world")
