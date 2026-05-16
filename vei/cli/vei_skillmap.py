from __future__ import annotations

import json
from pathlib import Path
import traceback

import typer

from vei.skillmap.api import (
    CompanySkillMap,
    build_company_skill_map_from_context_path,
    build_company_skill_map_from_workspace,
    enrich_skill_map_with_world_model_opportunities,
    validate_company_skill_map,
    write_company_skill_map_outputs,
)
from vei.ingest.api import load_agent_activity_events
from vei.provenance.api import build_evidence_pack

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


app = typer.Typer(add_completion=False)


@app.command("build")
def build(
    source_dir: str = typer.Option(
        ...,
        "--source-dir",
        "--source",
        help="Path to a context snapshot or bundle directory.",
    ),
    output: str = typer.Option(
        "company_skill_map",
        "--output",
        "-o",
        help="Directory for company_skill_map.json and Markdown reports.",
    ),
    limit: int = typer.Option(
        12,
        "--limit",
        help="Maximum number of candidate skills to emit.",
        min=1,
    ),
    replay: bool = typer.Option(
        True,
        "--replay/--no-replay",
        help="Attach deterministic historical replay scores when a context bundle can be loaded as a what-if world.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="LLM provider for skill synthesis. Defaults to .agents.yml.",
    ),
    model: str | None = typer.Option(
        None,
        "--model",
        help="LLM model for skill synthesis. Defaults to .agents.yml interactive_model.",
    ),
    previous_map: str | None = typer.Option(
        None,
        "--previous-map",
        help="Previous company_skill_map.json or output directory to preserve review state and retire missing skills.",
    ),
    timeout_s: int = typer.Option(
        240,
        "--timeout-s",
        help="LLM request timeout in seconds.",
        min=1,
    ),
    catalog_shard_size: int = typer.Option(
        80,
        "--catalog-shard-size",
        help="Evidence items per LLM call. All shards are processed; use 0 to send one full catalog.",
        min=0,
    ),
    world_model_report: str | None = typer.Option(
        None,
        "--world-model-report",
        help="Optional strategic-state results CSV/JSON used to add cited skill gaps/upgrades.",
    ),
    max_world_model_opportunities: int = typer.Option(
        8,
        "--max-world-model-opportunities",
        help="Maximum cited world-model skill opportunities to attach.",
        min=0,
    ),
    progress: bool = typer.Option(
        True,
        "--progress/--no-progress",
        help="Print live LLM extraction progress to stderr.",
    ),
) -> None:
    """Build an evidence-backed company skill map from a context bundle."""
    load_dotenv(override=False)
    try:
        skill_map = build_company_skill_map_from_context_path(
            source_dir,
            limit=limit,
            include_replay=replay,
            provider=provider,
            model=model,
            previous_map_path=previous_map,
            timeout_s=timeout_s,
            catalog_shard_size=catalog_shard_size,
            progress=_progress_reporter if progress else None,
        )
        if world_model_report:
            skill_map = enrich_skill_map_with_world_model_opportunities(
                skill_map,
                context_path=source_dir,
                world_model_report_path=world_model_report,
                max_opportunities=max_world_model_opportunities,
            )
    except Exception as exc:  # noqa: BLE001
        _exit_skillmap_failure("build", exc)
    paths = write_company_skill_map_outputs(skill_map, output)
    typer.echo(
        "Wrote "
        f"{skill_map.skill_count} skills "
        f"({skill_map.validation.error_count} errors, "
        f"{skill_map.validation.warning_count} warnings) "
        f"-> {paths['json'].parent}"
    )


@app.command("refresh")
def refresh(
    workspace: Path = typer.Option(
        ...,
        "--workspace",
        help="Workspace with context_snapshot.json and Control evidence.",
    ),
    context: Path | None = typer.Option(
        None,
        "--context",
        help="Optional context snapshot path. Defaults to <workspace>/context_snapshot.json.",
    ),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Directory for company_skill_map.json and reports. Defaults to <workspace>/skill_map.",
    ),
    limit: int = typer.Option(
        12,
        "--limit",
        help="Maximum number of candidate skills to emit.",
        min=1,
    ),
    replay: bool = typer.Option(
        True,
        "--replay/--no-replay",
        help="Attach deterministic historical replay scores when the context bundle can be loaded as a what-if world.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="LLM provider for skill synthesis. Defaults to .agents.yml.",
    ),
    model: str | None = typer.Option(
        None,
        "--model",
        help="LLM model for skill synthesis. Defaults to .agents.yml interactive_model.",
    ),
    previous_map: str | None = typer.Option(
        None,
        "--previous-map",
        help=(
            "Previous company_skill_map.json or output directory. "
            "Defaults to the output directory when company_skill_map.json exists."
        ),
    ),
    timeout_s: int = typer.Option(
        240,
        "--timeout-s",
        help="LLM request timeout in seconds.",
        min=1,
    ),
    catalog_shard_size: int = typer.Option(
        80,
        "--catalog-shard-size",
        help="Evidence items per LLM call. All shards are processed; use 0 to send one full catalog.",
        min=0,
    ),
    world_model_report: str | None = typer.Option(
        None,
        "--world-model-report",
        help="Optional strategic-state results CSV/JSON used to add cited skill gaps/upgrades.",
    ),
    max_world_model_opportunities: int = typer.Option(
        8,
        "--max-world-model-opportunities",
        help="Maximum cited world-model skill opportunities to attach.",
        min=0,
    ),
    progress: bool = typer.Option(
        True,
        "--progress/--no-progress",
        help="Print live LLM extraction progress to stderr.",
    ),
) -> None:
    """Refresh a living skill map from context plus imported Control evidence."""
    load_dotenv(override=False)
    workspace_path = workspace.expanduser().resolve()
    output_dir = (
        Path(output).expanduser().resolve() if output else workspace_path / "skill_map"
    )
    previous_map_path = previous_map or _default_previous_map(output_dir)
    try:
        skill_map = build_company_skill_map_from_workspace(
            workspace_path,
            context_path=context,
            limit=limit,
            include_replay=replay,
            provider=provider,
            model=model,
            previous_map_path=previous_map_path,
            timeout_s=timeout_s,
            catalog_shard_size=catalog_shard_size,
            progress=_progress_reporter if progress else None,
        )
        if world_model_report:
            skill_map = enrich_skill_map_with_world_model_opportunities(
                skill_map,
                context_path=context or workspace_path,
                world_model_report_path=world_model_report,
                max_opportunities=max_world_model_opportunities,
            )
    except Exception as exc:  # noqa: BLE001
        _exit_skillmap_failure("refresh", exc)
    paths = write_company_skill_map_outputs(skill_map, output_dir)
    evidence_pack_path = output_dir / "control_evidence_pack.json"
    events = load_agent_activity_events(str(workspace_path))
    evidence_pack = build_evidence_pack(events, workspace=workspace_path)
    evidence_pack_path.write_text(
        json.dumps(evidence_pack.model_dump(mode="json"), indent=2) + "\n",
        encoding="utf-8",
    )
    typer.echo(
        "Refreshed "
        f"{skill_map.skill_count} skills from "
        f"{skill_map.metadata.get('control_event_count', 0)} Control event(s) "
        f"({skill_map.validation.error_count} errors, "
        f"{skill_map.validation.warning_count} warnings) "
        f"-> {paths['json'].parent}"
    )


@app.command("validate")
def validate(
    map_path: str = typer.Option(..., "--map", help="Path to company_skill_map.json."),
    output: str = typer.Option(
        "-", "--output", "-o", help="Output validation JSON path or stdout."
    ),
) -> None:
    """Validate a company skill map before activation."""
    path = Path(map_path).expanduser().resolve()
    if not path.exists():
        raise typer.BadParameter(f"skill map not found: {map_path}")
    skill_map = CompanySkillMap.model_validate_json(path.read_text(encoding="utf-8"))
    validation = validate_company_skill_map(skill_map)
    text = json.dumps(validation.model_dump(mode="json"), indent=2) + "\n"
    if output == "-":
        typer.echo(text, nl=False)
        if not validation.ok:
            raise typer.Exit(1)
        return
    output_path = Path(output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    typer.echo(
        "Validated "
        f"{validation.active_skill_count} active and "
        f"{validation.draft_skill_count} draft skills "
        f"({validation.error_count} errors, {validation.warning_count} warnings) "
        f"-> {output_path}"
    )
    if not validation.ok:
        raise typer.Exit(1)


def _default_previous_map(output_dir: Path) -> str | None:
    candidate = output_dir / "company_skill_map.json"
    return str(candidate) if candidate.exists() else None


def _progress_reporter(message: str) -> None:
    typer.echo(message, err=True)


def _exit_skillmap_failure(action: str, exc: Exception) -> None:
    message = str(exc).strip() or type(exc).__name__
    if len(message) > 800:
        message = message[:800].rstrip() + "..."
    typer.echo(f"Skill map {action} failed: {message}", err=True)
    typer.echo(
        "Try the Codex-backed default "
        "`--provider codex --model gpt-5.4`, increase "
        "`--timeout-s`, or pass a direct API provider/model explicitly.",
        err=True,
    )
    debug_path = Path(".artifacts") / f"skillmap_{action}_error.txt"
    try:
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            encoding="utf-8",
        )
        typer.echo(f"Debug traceback written to {debug_path}", err=True)
    except OSError:
        pass
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
