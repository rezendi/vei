from __future__ import annotations

import typer

from vei.cli.vei_knowledge import app as knowledge_app
from vei.cli.vei_skillmap import app as skillmap_app

app = typer.Typer(
    help="Compose company knowledge views and evidence-backed skill maps.",
    no_args_is_help=True,
)

app.add_typer(knowledge_app)
app.add_typer(
    skillmap_app,
    name="skillmap",
    help="Build evidence-backed company skill maps from context bundles.",
)
