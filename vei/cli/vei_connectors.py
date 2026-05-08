from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.connectors.pipeshub import (
    DEFAULT_PIPESHUB_IMAGE_TAG,
    DEFAULT_PIPESHUB_PORT,
    PipesHubRuntimeConfig,
    default_runtime_dir,
    ensure_runtime_files,
    runtime_logs,
    runtime_status,
    start_runtime,
    stop_runtime,
)

app = typer.Typer(add_completion=False, help="Manage local connector services.")
pipeshub_app = typer.Typer(
    add_completion=False,
    help="Launch and inspect a local PipesHub connector stack.",
)
app.add_typer(pipeshub_app, name="pipeshub")


def _config(
    runtime_dir: Path,
    *,
    image_tag: str,
    host: str,
    port: int,
    project_name: str,
) -> PipesHubRuntimeConfig:
    return PipesHubRuntimeConfig(
        runtime_dir=runtime_dir,
        image_tag=image_tag,
        host=host,
        port=port,
        project_name=project_name,
    )


@pipeshub_app.command()
def up(
    runtime_dir: Path = typer.Option(
        default_runtime_dir(),
        "--runtime-dir",
        help="Local directory for generated PipesHub compose/env files.",
    ),
    image_tag: str = typer.Option(
        DEFAULT_PIPESHUB_IMAGE_TAG,
        "--image-tag",
        help="Pinned pipeshubai/pipeshub-ai image tag.",
    ),
    host: str = typer.Option("127.0.0.1", "--host", help="Host bind address."),
    port: int = typer.Option(
        DEFAULT_PIPESHUB_PORT,
        "--port",
        min=1,
        max=65535,
        help="Host port for the PipesHub UI/API.",
    ),
    project_name: str = typer.Option(
        "vei-pipeshub", "--project-name", help="Docker Compose project name."
    ),
    pull: bool = typer.Option(False, "--pull", help="Pull images before starting."),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Write compose/env files and print metadata without starting Docker.",
    ),
    overwrite: bool = typer.Option(
        False, "--overwrite", help="Regenerate existing compose/env files."
    ),
    format: str = typer.Option("plain", "--format", help="plain | json"),
) -> None:
    """Create and optionally start a local PipesHub pilot stack."""
    config = _config(
        runtime_dir,
        image_tag=image_tag,
        host=host,
        port=port,
        project_name=project_name,
    )
    payload = ensure_runtime_files(config, overwrite=overwrite)
    payload["dry_run"] = dry_run
    if not dry_run:
        try:
            payload["docker_output"] = start_runtime(config, pull=pull)
        except RuntimeError as exc:
            raise typer.BadParameter(str(exc)) from exc
    _emit(payload, format=format)


@pipeshub_app.command()
def status(
    runtime_dir: Path = typer.Option(default_runtime_dir(), "--runtime-dir"),
    project_name: str = typer.Option("vei-pipeshub", "--project-name"),
) -> None:
    """Show Docker Compose status for the local PipesHub stack."""
    config = PipesHubRuntimeConfig(runtime_dir=runtime_dir, project_name=project_name)
    try:
        typer.echo(runtime_status(config))
    except RuntimeError as exc:
        raise typer.BadParameter(str(exc)) from exc


@pipeshub_app.command()
def logs(
    runtime_dir: Path = typer.Option(default_runtime_dir(), "--runtime-dir"),
    project_name: str = typer.Option("vei-pipeshub", "--project-name"),
    tail: int = typer.Option(200, "--tail", min=1, help="Number of log lines."),
) -> None:
    """Show recent logs for the local PipesHub stack."""
    config = PipesHubRuntimeConfig(runtime_dir=runtime_dir, project_name=project_name)
    try:
        typer.echo(runtime_logs(config, tail=tail))
    except RuntimeError as exc:
        raise typer.BadParameter(str(exc)) from exc


@pipeshub_app.command()
def down(
    runtime_dir: Path = typer.Option(default_runtime_dir(), "--runtime-dir"),
    project_name: str = typer.Option("vei-pipeshub", "--project-name"),
) -> None:
    """Stop the local PipesHub stack without deleting volumes."""
    config = PipesHubRuntimeConfig(runtime_dir=runtime_dir, project_name=project_name)
    try:
        typer.echo(stop_runtime(config))
    except RuntimeError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _emit(payload: dict, *, format: str) -> None:
    if format == "json":
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if format != "plain":
        raise typer.BadParameter("format must be plain or json")
    typer.echo(f"PipesHub runtime: {payload['runtime_dir']}")
    typer.echo(f"Base URL:         {payload['base_url']}")
    typer.echo(f"Image:            {payload['image']}")
    for path in payload.get("wrote", []):
        typer.echo(f"Wrote:            {path}")
    for warning in payload.get("warnings", []):
        typer.echo(f"Note:             {warning}")
    if payload.get("dry_run"):
        typer.echo("Dry run:          docker compose was not started")
    elif payload.get("docker_output"):
        typer.echo(payload["docker_output"])
