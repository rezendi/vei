"""Bus-factor CLI.

Usage:

    vei bus-factor <tenant>                       # prints markdown, writes
                                                  # to _vei_out/<tenant>/bus_factor/
    vei bus-factor <tenant> --redact              # anonymized output
    vei bus-factor <tenant> --output report.md    # explicit output path
    vei bus-factor <tenant> --source-dir <path>   # override snapshot path
    vei bus-factor <tenant> --window-days 90      # custom window
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import click

from vei.bus_factor import (
    BusFactorReport,
    compute_bus_factor_report,
    render_report_markdown,
    resolve_tenant_snapshot,
)
from vei.bus_factor.api import (
    DEFAULT_ACTIVITY_SHARE_THRESHOLD,
    DEFAULT_WINDOW_DAYS,
)


def _default_output_path(tenant: str, report: BusFactorReport) -> Path:
    base = Path("_vei_out") / tenant / "bus_factor"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return base / f"bus_factor_report_{stamp}.md"


@click.command(help="Identify actors with sole or near-sole ownership of named work.")
@click.argument("tenant")
@click.option(
    "--source-dir",
    type=click.Path(path_type=Path),
    help="Override snapshot resolution; path to context_snapshot.json or its directory.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help=(
        "Markdown output path. Defaults to "
        "_vei_out/<tenant>/bus_factor/bus_factor_report_<date>.md"
    ),
)
@click.option(
    "--json",
    "json_output",
    type=click.Path(path_type=Path),
    help="Also write the structured JSON report to this path.",
)
@click.option(
    "--redact",
    is_flag=True,
    help="Anonymize actor identities and elide source paths in the markdown.",
)
@click.option(
    "--window-days",
    default=DEFAULT_WINDOW_DAYS,
    show_default=True,
    type=int,
    help="Analysis window in days (counted back from the most recent event).",
)
@click.option(
    "--activity-share",
    "activity_share_threshold",
    default=DEFAULT_ACTIVITY_SHARE_THRESHOLD,
    show_default=True,
    type=float,
    help="Activity-share threshold for 'primary driver on workflow' (0..1).",
)
@click.option(
    "--skill-map",
    type=click.Path(path_type=Path),
    help="Override skill map auto-discovery.",
)
@click.option(
    "--workflow-candidates",
    type=click.Path(path_type=Path),
    help="Override workflow_candidates.json auto-discovery.",
)
@click.option(
    "--workflow-labels",
    type=click.Path(path_type=Path),
    help="Override workflow_labels.json auto-discovery.",
)
def app(
    tenant: str,
    source_dir: Path | None,
    output: Path | None,
    json_output: Path | None,
    redact: bool,
    window_days: int,
    activity_share_threshold: float,
    skill_map: Path | None,
    workflow_candidates: Path | None,
    workflow_labels: Path | None,
) -> None:
    """Generate a bus-factor report for the named tenant."""

    if source_dir is not None:
        path = source_dir.expanduser().resolve()
        if path.is_dir():
            snapshot = path / "context_snapshot.json"
        else:
            snapshot = path
        if not snapshot.is_file():
            raise click.BadParameter(f"context_snapshot.json not found at {path}")
    else:
        try:
            snapshot = resolve_tenant_snapshot(tenant)
        except FileNotFoundError as exc:
            raise click.BadParameter(str(exc)) from exc

    try:
        report = compute_bus_factor_report(
            context_path=snapshot,
            tenant_id=tenant,
            window_days=window_days,
            activity_share_threshold=activity_share_threshold,
            skill_map_path=skill_map,
            workflow_candidates_path=workflow_candidates,
            workflow_labels_path=workflow_labels,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise click.BadParameter(str(exc)) from exc

    markdown = render_report_markdown(report, redact=redact)
    click.echo(markdown)

    target = output or _default_output_path(tenant, report)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(markdown, encoding="utf-8")
    click.echo(f"wrote markdown: {target}", err=True)

    if json_output is not None:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(
            json.dumps(report.model_dump(mode="json"), indent=2) + "\n",
            encoding="utf-8",
        )
        click.echo(f"wrote json: {json_output}", err=True)


if __name__ == "__main__":
    app()
