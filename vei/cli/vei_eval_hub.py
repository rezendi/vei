from __future__ import annotations

import typer

from vei.cli.vei_demo import app as demo_app
from vei.cli.vei_det_pipeline import app as det_app
from vei.cli.vei_eval import app as eval_app
from vei.cli.vei_llm_test import app as llm_test_app
from vei.cli.vei_showcase import app as showcase_app
from vei.cli.vei_smoke import app as smoke_app

app = typer.Typer(
    help="Run evaluations, demos, smoke checks, and deterministic pipelines.",
    no_args_is_help=True,
)

app.add_typer(eval_app)
app.add_typer(
    demo_app,
    name="agent-demo",
    help="Run lightweight scripted or LLM-driven demos.",
)
app.add_typer(
    smoke_app,
    name="smoke",
    help="Run transport and harness smoke checks.",
)
app.add_typer(showcase_app, name="showcase")
app.add_typer(
    llm_test_app,
    name="llm-test",
    help="Run the live LLM harness against the MCP world.",
)
app.add_typer(det_app, name="det")
