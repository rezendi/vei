from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from vei.workflow.api import (
    WorkflowLabelKind,
    add_workflow_label,
    mine_workflows,
    package_workflow_environment,
    promote_workflow_candidate,
    refresh_workflows,
)

app = typer.Typer(
    add_completion=False,
    help="Mine, label, promote, and package evidence-backed business task specs.",
)


def _emit(payload: object, indent: int) -> None:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json")  # type: ignore[assignment]
    typer.echo(json.dumps(payload, indent=indent))


@app.command("mine")
def mine(
    source_dir: Path = typer.Option(
        ..., help="context_snapshot.json path or directory containing it"
    ),
    output: Path = typer.Option(..., help="Workflow mining output directory"),
    limit: int = typer.Option(25, min=1, help="Maximum candidates to keep"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Mine recurring work candidates from a canonical company history."""

    try:
        result = mine_workflows(source_dir, output=output, limit=limit)
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(result, indent)


@app.command("label")
def label_candidate(
    root: Path = typer.Option(..., help="Workflow mining output directory"),
    candidate_id: str = typer.Option(..., help="Candidate id to label"),
    label: WorkflowLabelKind = typer.Option(..., help="Workflow label"),
    note: str = typer.Option("", help="Human note for the label"),
    event_id: Optional[list[str]] = typer.Option(
        None, "--event-id", help="Optional source event id to attach"
    ),
    example_id: Optional[list[str]] = typer.Option(
        None, "--example-id", help="Optional observed example id to attach"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Attach a file-backed human label to a candidate."""

    try:
        created = add_workflow_label(
            root,
            candidate_id=candidate_id,
            label=label,
            note=note,
            event_ids=event_id,
            example_ids=example_id,
        )
    except (KeyError, ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(created, indent)


@app.command("promote")
def promote(
    root: Path = typer.Option(..., help="Workflow mining output directory"),
    candidate_id: str = typer.Option(..., help="Candidate id to promote"),
    output: Path = typer.Option(..., help="Destination task_spec.json path"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Promote a candidate into a declarative Business Task Spec."""

    try:
        spec = promote_workflow_candidate(
            root,
            candidate_id=candidate_id,
            output=output,
        )
    except (KeyError, ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(spec, indent)


@app.command("package-env")
def package_env(
    spec: Path = typer.Option(..., help="Business Task Spec JSON"),
    source_dir: Path = typer.Option(
        ..., help="context_snapshot.json path or directory containing it"
    ),
    output: Path = typer.Option(..., help="Environment package output directory"),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Package a reviewed RL-ready workflow environment."""

    try:
        manifest = package_workflow_environment(
            spec_path=spec,
            source_dir=source_dir,
            output=output,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(manifest, indent)


@app.command("refresh")
def refresh(
    source_dir: Path = typer.Option(
        ..., help="context_snapshot.json path or directory containing it"
    ),
    workspace: Path = typer.Option(..., help="Workspace root for adjacent artifacts"),
    output: Path = typer.Option(..., help="Workflow mining output directory"),
    limit: int = typer.Option(25, min=1, help="Maximum candidates to keep"),
    refresh_wiki: bool = typer.Option(False, help="Also rebuild wiki artifacts"),
    refresh_skillmap: bool = typer.Option(
        False, help="Also rebuild skill-map artifacts"
    ),
    indent: int = typer.Option(2, help="Pretty indent"),
) -> None:
    """Refresh workflow candidates while preserving existing labels."""

    try:
        report = refresh_workflows(
            source_dir=source_dir,
            workspace=workspace,
            output=output,
            limit=limit,
            refresh_wiki_artifacts=refresh_wiki,
            refresh_skillmap_artifacts=refresh_skillmap,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(report, indent)


if __name__ == "__main__":
    app()
