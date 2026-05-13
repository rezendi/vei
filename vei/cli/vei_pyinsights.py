from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import typer

from vei.pyinsights.daily_refresh import (
    DEFAULT_CONTEXT_BUNDLE,
    DEFAULT_DAILY_ROOT,
    FreshnessPolicy,
    RefreshMode,
    run_validated_daily_refresh,
)

app = typer.Typer(
    add_completion=False,
    help="Py Insights validated daily refresh workflows.",
)


@app.command("daily-refresh")
def daily_refresh(
    mode: RefreshMode = typer.Option(
        "incremental-validated",
        "--mode",
        help="Refresh mode: incremental-validated | full-validated",
    ),
    previous: str = typer.Option(
        "latest-valid",
        "--previous",
        help="Previous validated run path, latest-valid, or none.",
    ),
    as_of: str = typer.Option(
        "today",
        "--as-of",
        help="As-of date as YYYY-MM-DD or today.",
    ),
    context_bundle: Path = typer.Option(
        DEFAULT_CONTEXT_BUNDLE,
        "--context-bundle",
        help="Py Insights context_snapshot.json to validate.",
    ),
    output_root: Path = typer.Option(
        DEFAULT_DAILY_ROOT,
        "--output-root",
        help="Root directory for daily refresh attempts.",
    ),
    label: str = typer.Option(
        "",
        "--label",
        help="Optional run label. Defaults to pyinsights_daily_YYYYMMDD.",
    ),
    model_run_root: Path | None = typer.Option(
        None,
        "--model-run-root",
        help="World-model run root. Defaults to latest Py Insights curated model run.",
    ),
    strategic_run_root: Path | None = typer.Option(
        None,
        "--strategic-run-root",
        help="Strategic-state run root. Defaults to latest Py Insights strategic run.",
    ),
    workflow_output: Path | None = typer.Option(
        None,
        "--workflow-output",
        help="Workflow output directory. Defaults inside the daily run root.",
    ),
    skill_map: Path | None = typer.Option(
        None,
        "--skill-map",
        help="company_skill_map.json. Defaults beside the context bundle.",
    ),
    source_freshness_policy: FreshnessPolicy = typer.Option(
        "all-sources-current",
        "--source-freshness-policy",
        help=(
            "Freshness gate: all-sources-current | bundle-current | "
            "allow-stale-caveats."
        ),
    ),
    refresh_workflows: bool = typer.Option(
        True,
        "--refresh-workflows/--reuse-workflows",
        help="Re-mine semantic workflows before validation.",
    ),
    allow_report_on_warning: bool = typer.Option(
        True,
        "--allow-report-on-warning/--fail-on-warning",
        help="Warnings are recorded but do not block CEO report emission.",
    ),
    indent: int = typer.Option(2, help="JSON indent."),
) -> None:
    """Run the validated daily Py Insights refresh gate."""

    try:
        manifest = run_validated_daily_refresh(
            mode=mode,
            previous=previous,
            as_of=_parse_as_of(as_of),
            context_bundle=context_bundle,
            output_root=output_root,
            label=label,
            model_run_root=model_run_root,
            strategic_run_root=strategic_run_root,
            workflow_output=workflow_output,
            skill_map_path=skill_map,
            source_freshness_policy=source_freshness_policy,
            refresh_workflows=refresh_workflows,
            allow_report_on_warning=allow_report_on_warning,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(manifest.model_dump(mode="json"), indent=indent))
    if not manifest.validation_passed:
        raise typer.Exit(code=1)


def _parse_as_of(value: str) -> str | date:
    normalized = value.strip().lower()
    if normalized == "today":
        return normalized
    return date.fromisoformat(normalized)


if __name__ == "__main__":
    app()
