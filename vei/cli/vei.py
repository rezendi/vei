from __future__ import annotations

import typer

from vei.cli._lazy import LazyCommandSpec, LazyTyperGroup


class VEILazyGroup(LazyTyperGroup):
    lazy_commands = {
        "workspace": LazyCommandSpec(
            module_path="vei.cli.vei_workspace",
            help="Manage workspace setup, imports, context capture, and twin runtime.",
        ),
        "run": LazyCommandSpec(
            module_path="vei.cli.vei_run",
            help="Launch and inspect workspace runs.",
        ),
        "eval": LazyCommandSpec(
            module_path="vei.cli.vei_eval_hub",
            help="Run evaluations, demos, smoke checks, and deterministic pipelines.",
        ),
        "inspect": LazyCommandSpec(
            module_path="vei.cli.vei_inspect",
            help="Inspect fidelity, orientation, and workspace state.",
        ),
        "ui": LazyCommandSpec(
            module_path="vei.cli.vei_ui",
            help="Serve the local FastAPI studio surfaces.",
        ),
        "whatif": LazyCommandSpec(
            module_path="vei.cli.vei_whatif",
            help="Explore counterfactuals and replayable what-if episodes.",
        ),
        "knowledge": LazyCommandSpec(
            module_path="vei.cli.vei_knowledge_hub",
            help="Compose company knowledge views and evidence-backed skill maps.",
        ),
        "provenance": LazyCommandSpec(
            module_path="vei.cli.vei_provenance",
            help="Inspect agent evidence, access review, blast radius, and policy replay.",
        ),
        "wiki": LazyCommandSpec(
            module_path="vei.cli.vei_wiki",
            help="Build, refresh, and query the materialized company wiki.",
        ),
        "workflow": LazyCommandSpec(
            module_path="vei.cli.vei_workflow",
            help="Mine, label, promote, and package evidence-backed business task specs.",
        ),
        "discover": LazyCommandSpec(
            module_path="vei.cli.vei_discover",
            help="Autonomously discover workflow families and generate company skills.",
        ),
        "connectors": LazyCommandSpec(
            module_path="vei.cli.vei_connectors",
            help="Manage optional local connector services such as PipesHub.",
        ),
        "rollout": LazyCommandSpec(
            module_path="vei.cli.vei_rollout",
            help="Generate scripted rollouts from the simulated company for RL training.",
        ),
        "train": LazyCommandSpec(
            module_path="vei.cli.vei_train",
            help="Train policies (e.g. behavior cloning) from rollout traces.",
        ),
        "admin": LazyCommandSpec(
            module_path="vei.cli.vei_admin",
            help="Run operator, release, reporting, and platform maintenance commands.",
        ),
        "mcm": LazyCommandSpec(
            module_path="vei.cli.vei_mcm",
            help="Build and compare tenant measurement manifests (kill-hardcoded-keywords).",
        ),
        "pyinsights": LazyCommandSpec(
            module_path="vei.cli.vei_pyinsights",
            help="Run Py Insights validated daily refreshes.",
        ),
        "bus-factor": LazyCommandSpec(
            module_path="vei.cli.vei_bus_factor",
            help="Identify actors with sole or near-sole ownership of named work.",
        ),
        # Backward-compatible aliases (hidden in `vei --help`).
        "project": LazyCommandSpec(
            module_path="vei.cli.vei_project",
            help="Manage workspace imports, sources, and project scaffolding.",
            hidden=True,
        ),
        "context": LazyCommandSpec(
            module_path="vei.cli.vei_context",
            help="Capture and inspect context bundles.",
            hidden=True,
        ),
        "ingest": LazyCommandSpec(
            module_path="vei.cli.vei_ingest",
            help="Ingest company state and agent-activity evidence.",
            hidden=True,
        ),
        "twin": LazyCommandSpec(
            module_path="vei.cli.vei_twin",
            help="Build and serve customer twin environments.",
            hidden=True,
        ),
        "skillmap": LazyCommandSpec(
            module_path="vei.cli.vei_skillmap",
            help="Build evidence-backed company skill maps from context bundles.",
            hidden=True,
        ),
        "doctor": LazyCommandSpec(
            module_path="vei.cli.vei_doctor",
            help="Inspect local setup and surface common workspace issues.",
            hidden=True,
        ),
        "quickstart": LazyCommandSpec(
            module_path="vei.cli.vei_quickstart",
            help="Launch guided local demos and twin-backed workspaces.",
            hidden=True,
        ),
        "release": LazyCommandSpec(
            module_path="vei.cli.vei_release",
            help="Build release artifacts and nightly snapshots.",
            hidden=True,
        ),
        "world": LazyCommandSpec(
            module_path="vei.cli.vei_world",
            help="Inspect world catalogs and blueprint-backed sessions.",
            hidden=True,
        ),
        "blueprint": LazyCommandSpec(
            module_path="vei.cli.vei_blueprint",
            help="Inspect, generate, and scaffold blueprint assets.",
            hidden=True,
        ),
        "contract": LazyCommandSpec(
            module_path="vei.cli.vei_contract",
            help="Inspect and validate workspace contracts.",
            hidden=True,
        ),
        "demo": LazyCommandSpec(
            module_path="vei.cli.vei_demo",
            help="Run lightweight scripted or LLM-driven demos.",
            hidden=True,
        ),
        "det": LazyCommandSpec(
            module_path="vei.cli.vei_det_pipeline",
            help="Run the deterministic data and evaluation pipeline.",
            hidden=True,
        ),
        "llm-test": LazyCommandSpec(
            module_path="vei.cli.vei_llm_test",
            help="Run the live LLM harness against the MCP world.",
            hidden=True,
        ),
        "report": LazyCommandSpec(
            module_path="vei.cli.vei_report",
            help="Render run and benchmark reports.",
            hidden=True,
        ),
        "smoke": LazyCommandSpec(
            module_path="vei.cli.vei_smoke",
            help="Run transport and harness smoke checks.",
            hidden=True,
        ),
        "showcase": LazyCommandSpec(
            module_path="vei.cli.vei_showcase",
            help="Run curated showcase scenarios.",
            hidden=True,
        ),
        "synthesize": LazyCommandSpec(
            module_path="vei.cli.vei_synthesize",
            help="Generate synthesis configs and runbooks.",
            hidden=True,
        ),
        "visualize": LazyCommandSpec(
            module_path="vei.cli.vei_visualize",
            help="Render visualization artifacts from runs and traces.",
            hidden=True,
        ),
    }


app = typer.Typer(
    add_completion=False,
    cls=VEILazyGroup,
    no_args_is_help=True,
    help="VEI — programmable enterprise simulation, context capture, and synthesis.",
)


@app.callback()
def main() -> None:
    """Expose the lazy top-level VEI command group."""
    return None


if __name__ == "__main__":
    app()
