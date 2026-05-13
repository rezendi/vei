from __future__ import annotations

from pathlib import Path

import typer

from vei.discovery.api import discover_company_workflows_and_skills
from vei.discovery.models import DiscoverySourceInput, DiscoverySourceRole

app = typer.Typer(
    add_completion=False,
    help="Autonomously discover company workflows and generate draft skills.",
)


@app.callback()
def main() -> None:
    """Autonomous workflow and skill discovery commands."""
    return None


@app.command("run")
def run(
    source: list[Path] = typer.Option(
        [],
        "--source",
        help="Snapshot or bundle path to include as an untyped source. Repeatable.",
    ),
    historical_source: list[Path] = typer.Option(
        [],
        "--historical-source",
        help="Historical snapshot or bundle path, e.g. stored PoY history. Repeatable.",
    ),
    live_source: list[Path] = typer.Option(
        [],
        "--live-source",
        help="Live/current snapshot or bundle path, e.g. Py Insights connectors. Repeatable.",
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Directory for lineage, workflow, skill, and manifest artifacts.",
    ),
    company_name: str = typer.Option(
        "",
        "--company-name",
        "--org",
        help="Canonical company name override.",
    ),
    company_domain: str = typer.Option(
        "",
        "--company-domain",
        "--domain",
        help="Canonical company domain override.",
    ),
    alias: list[str] = typer.Option(
        [],
        "--alias",
        help="Known company alias or prior name. Repeatable.",
    ),
    workflow_limit: int = typer.Option(
        25,
        "--workflow-limit",
        min=1,
        help="Maximum workflow families to emit.",
    ),
    skill_limit: int = typer.Option(
        12,
        "--skill-limit",
        min=1,
        help="Maximum generated skills to emit.",
    ),
) -> None:
    """Discover workflows and generate draft skills from canonical history."""

    inputs = _source_inputs(
        source=source,
        historical_source=historical_source,
        live_source=live_source,
    )
    if not inputs:
        raise typer.BadParameter(
            "provide at least one --source, --historical-source, or --live-source"
        )
    result = discover_company_workflows_and_skills(
        inputs,
        company_name=company_name,
        company_domain=company_domain,
        aliases=alias,
        output=output,
        workflow_limit=workflow_limit,
        skill_limit=skill_limit,
    )
    typer.echo(
        "Discovered "
        f"{len(result.workflows)} workflow family/families and "
        f"{result.skill_map.skill_count} generated skill(s) "
        f"from {result.lineage.event_count} event(s) -> {result.output_dir}"
    )


def _source_inputs(
    *,
    source: list[Path],
    historical_source: list[Path],
    live_source: list[Path],
) -> list[DiscoverySourceInput]:
    inputs: list[DiscoverySourceInput] = []
    for path in historical_source:
        inputs.append(
            DiscoverySourceInput(
                path=str(path),
                role=DiscoverySourceRole.HISTORICAL,
                label="historical",
            )
        )
    for path in live_source:
        inputs.append(
            DiscoverySourceInput(
                path=str(path),
                role=DiscoverySourceRole.LIVE,
                label="live",
            )
        )
    for path in source:
        inputs.append(
            DiscoverySourceInput(
                path=str(path),
                role=DiscoverySourceRole.SNAPSHOT,
                label="snapshot",
            )
        )
    return inputs
