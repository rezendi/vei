from __future__ import annotations

import typer

from vei.cli.vei_context import app as context_app
from vei.cli.vei_ingest import app as ingest_app
from vei.cli.vei_project import app as project_app
from vei.cli.vei_twin import app as twin_app

app = typer.Typer(
    help="Manage workspace setup, import, context capture, and twin runtime tasks.",
    no_args_is_help=True,
)

app.add_typer(project_app, name="project")
app.add_typer(
    context_app,
    name="context",
    help="Capture and inspect context bundles.",
)
app.add_typer(ingest_app, name="ingest")
app.add_typer(twin_app, name="twin")
