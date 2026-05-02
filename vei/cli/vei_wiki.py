"""`vei wiki` -- build, refresh, and query the materialized Company Wiki."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.wiki.api import (
    CompanyWikiSnapshot,
    DEFAULT_WIKI_OUTPUT_DIR,
    WIKI_SNAPSHOT_FILE,
    build_wiki_from_context_path,
    build_wiki_from_workspace,
    load_wiki_snapshot,
    query_wiki,
    refresh_wiki,
    write_wiki_artifacts,
)

app = typer.Typer(add_completion=False)


@app.command("build")
def build(
    source_dir: str = typer.Option(
        ...,
        "--source-dir",
        "--source",
        help="Path to a context snapshot file or directory containing context_snapshot.json.",
    ),
    output: str = typer.Option(
        "wiki",
        "--output",
        "-o",
        help="Directory for company_wiki.json + index.md + pages/*.md.",
    ),
) -> None:
    """Build a wiki snapshot from a context bundle."""

    snapshot = build_wiki_from_context_path(source_dir)
    output_path = Path(output).expanduser().resolve()
    report = write_wiki_artifacts(snapshot, output_path)
    typer.echo(
        f"Wrote {report.page_count} pages "
        f"({report.citation_count} citations) -> {report.output_dir}"
    )


@app.command("refresh")
def refresh(
    workspace: Path = typer.Option(
        ...,
        "--workspace",
        help="Workspace root containing context_snapshot.json (and optional curated layers).",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output directory. Defaults to <workspace>/.artifacts/wiki.",
    ),
    context: Path | None = typer.Option(
        None,
        "--context",
        help="Optional context snapshot path. Defaults to <workspace>/context_snapshot.json.",
    ),
) -> None:
    """Refresh a workspace wiki, picking up curated knowledge/skill overlays."""

    workspace_path = workspace.expanduser().resolve()
    target = (
        Path(output).expanduser().resolve()
        if output
        else workspace_path.joinpath(*DEFAULT_WIKI_OUTPUT_DIR)
    )
    report = refresh_wiki(
        workspace_path,
        output_dir=target,
        context_path=context,
    )
    if report.status == "error":
        typer.echo("\n".join(report.errors) or "wiki refresh failed", err=True)
        raise typer.Exit(1)
    typer.echo(
        f"Refreshed {report.page_count} pages "
        f"({report.citation_count} citations) "
        f"-> {report.output_dir}"
    )


@app.command("query")
def query(
    text: str = typer.Argument(..., help="Search query."),
    wiki: Path | None = typer.Option(
        None,
        "--wiki",
        help="Path to a built wiki directory (must contain company_wiki.json).",
    ),
    workspace: Path | None = typer.Option(
        None,
        "--workspace",
        help="Workspace root. The wiki is rebuilt in-memory if no artifact exists.",
    ),
    limit: int = typer.Option(
        10,
        "--limit",
        help="Maximum number of hits to return.",
        min=1,
    ),
    output_format: str = typer.Option(
        "text",
        "--format",
        help="Output format: text or json.",
        case_sensitive=False,
    ),
) -> None:
    """Run a title-only keyword query against a wiki snapshot."""

    if wiki is None and workspace is None:
        raise typer.BadParameter("either --wiki or --workspace is required")

    snapshot = _resolve_snapshot(wiki=wiki, workspace=workspace)
    result = query_wiki(snapshot, text, limit=limit)
    fmt = output_format.strip().lower()
    if fmt == "json":
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    if not result.hits:
        typer.echo(f"No matches for: {text}")
        raise typer.Exit(0)
    typer.echo(f"{result.total_hits} hit(s) for: {text}")
    for hit in result.hits:
        if hit.matched_field == "section_title":
            typer.echo(
                f"- [{hit.score:.1f}] {hit.page_title} :: {hit.section_title} "
                f"(page={hit.page_id}, section={hit.section_id})"
            )
        else:
            typer.echo(f"- [{hit.score:.1f}] {hit.page_title} (page={hit.page_id})")


def _resolve_snapshot(
    *,
    wiki: Path | None,
    workspace: Path | None,
) -> CompanyWikiSnapshot:
    if wiki is not None:
        wiki_path = wiki.expanduser().resolve()
        snapshot = load_wiki_snapshot(wiki_path)
        if snapshot is None:
            raise typer.BadParameter(f"no {WIKI_SNAPSHOT_FILE} found at {wiki_path}")
        return snapshot
    if workspace is None:
        raise typer.BadParameter("either --wiki or --workspace is required")
    workspace_path = workspace.expanduser().resolve()
    persisted_dir = workspace_path.joinpath(*DEFAULT_WIKI_OUTPUT_DIR)
    persisted = load_wiki_snapshot(persisted_dir)
    if persisted is not None:
        return persisted
    return build_wiki_from_workspace(workspace_path)


if __name__ == "__main__":
    app()
