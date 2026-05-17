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

import typer

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

app = typer.Typer(
    add_completion=False,
    invoke_without_command=True,
    no_args_is_help=False,
    help="Identify actors with sole or near-sole ownership of named work.",
)


def _default_output_path(tenant: str, report: BusFactorReport) -> Path:
    base = Path("_vei_out") / tenant / "bus_factor"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return base / f"bus_factor_report_{stamp}.md"


@app.callback(invoke_without_command=True)
def main(
    tenant: str = typer.Argument(
        ...,
        help="Tenant id (resolved to _vei_out/<tenant>/[combined-live-*/]context_snapshot.json).",
    ),
    source_dir: Path | None = typer.Option(
        None,
        "--source-dir",
        help="Override snapshot resolution; path to context_snapshot.json or its directory.",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Markdown output path. Defaults to _vei_out/<tenant>/bus_factor/bus_factor_report_<date>.md",
    ),
    json_output: Path | None = typer.Option(
        None,
        "--json",
        help="Also write the structured JSON report to this path.",
    ),
    redact: bool = typer.Option(
        False,
        "--redact",
        help="Anonymize actor identities and elide source paths in the markdown.",
    ),
    window_days: int = typer.Option(
        DEFAULT_WINDOW_DAYS,
        "--window-days",
        help="Analysis window in days (counted back from the most recent event).",
    ),
    activity_share_threshold: float = typer.Option(
        DEFAULT_ACTIVITY_SHARE_THRESHOLD,
        "--activity-share",
        help="Activity-share threshold for 'primary driver on workflow' (0..1).",
    ),
    skill_map: Path | None = typer.Option(
        None,
        "--skill-map",
        help="Override skill map auto-discovery.",
    ),
    workflow_candidates: Path | None = typer.Option(
        None,
        "--workflow-candidates",
        help="Override workflow_candidates.json auto-discovery.",
    ),
    workflow_labels: Path | None = typer.Option(
        None,
        "--workflow-labels",
        help="Override workflow_labels.json auto-discovery.",
    ),
) -> None:
    """Generate a bus-factor report for the named tenant."""

    if source_dir is not None:
        path = source_dir.expanduser().resolve()
        if path.is_dir():
            snapshot = path / "context_snapshot.json"
        else:
            snapshot = path
        if not snapshot.is_file():
            raise typer.BadParameter(f"context_snapshot.json not found at {path}")
    else:
        try:
            snapshot = resolve_tenant_snapshot(tenant)
        except FileNotFoundError as exc:
            raise typer.BadParameter(str(exc)) from exc

    report = compute_bus_factor_report(
        context_path=snapshot,
        tenant_id=tenant,
        window_days=window_days,
        activity_share_threshold=activity_share_threshold,
        skill_map_path=skill_map,
        workflow_candidates_path=workflow_candidates,
        workflow_labels_path=workflow_labels,
    )

    markdown = render_report_markdown(report, redact=redact)
    typer.echo(markdown)

    target = output or _default_output_path(tenant, report)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(markdown, encoding="utf-8")
    typer.echo(f"wrote markdown: {target}", err=True)

    if json_output is not None:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(
            json.dumps(report.model_dump(mode="json"), indent=2) + "\n",
            encoding="utf-8",
        )
        typer.echo(f"wrote json: {json_output}", err=True)


if __name__ == "__main__":
    app()
