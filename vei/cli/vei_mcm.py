"""Measurement-classification manifest CLI.

Surfaces:

- `vei mcm extract` — freeze the current handwritten keyword bags into a
  fixture manifest. Use it as a regression oracle and as the comparator for
  LLM-derived manifests.
- `vei mcm propose` — single-pass LLM proposer over a canonical event sample.
- `vei mcm diff` — markdown diff between two manifests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover

    def load_dotenv(*args: Any, **kwargs: Any) -> None:
        return None


app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Build and compare tenant measurement manifests (kill-hardcoded-keywords plan).",
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_output(output: Path | None, tenant_id: str, suffix: str) -> Path:
    if output is not None:
        return output
    return Path(f"tenant_measurement_manifest.{tenant_id}.{suffix}.json")


@app.command("extract")
def extract_cmd(
    tenant: str = typer.Option(
        ..., "--tenant", help="Tenant id (e.g. pyinsights, dispatch, enron)."
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path. Defaults to tenant_measurement_manifest.<tenant>.fixture.json",
    ),
    org_context_file: Path | None = typer.Option(
        None, "--org-context-file", help="Optional JSON file with org_context to embed."
    ),
) -> None:
    """Freeze the current handwritten keyword bags as a fixture manifest."""
    from vei.whatif.measurement_fixtures import extract_fixture_manifest
    from vei.whatif.measurement_manifest import save_manifest

    org_context = _read_json(org_context_file) if org_context_file else None
    manifest = extract_fixture_manifest(tenant_id=tenant, org_context=org_context)
    target = _resolve_output(output, manifest.tenant_id, "fixture")
    save_manifest(manifest, target)
    typer.echo(f"wrote fixture manifest: {target} ({len(manifest.heads)} heads)")


@app.command("propose")
def propose_cmd(
    tenant: str = typer.Option(
        ..., "--tenant", help="Tenant id (free-form for OOD corpora like enron)."
    ),
    events_json: Path = typer.Option(
        ...,
        "--events-json",
        help=(
            "JSON file containing a list of WhatIfEvent objects, or a dict with "
            "an 'events' key."
        ),
    ),
    org_context_file: Path | None = typer.Option(
        None, "--org-context-file", help="Optional JSON file with org_context."
    ),
    provider: str = typer.Option(
        "auto",
        "--provider",
        help="LLM provider (auto or openai).",
    ),
    model: str = typer.Option("gpt-5-mini", "--model", help="LLM model id."),
    max_events: int = typer.Option(
        80, "--max-events", help="Max events to include in the prompt."
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output manifest path."
    ),
) -> None:
    """Run a single-pass LLM proposer over an event sample and write a manifest."""
    load_dotenv(override=False)

    from vei.whatif.measurement_manifest import save_manifest
    from vei.whatif.measurement_proposer import propose_manifest
    from vei.whatif.models import WhatIfEvent

    raw = _read_json(events_json)
    if isinstance(raw, dict) and "events" in raw:
        raw = raw["events"]
    if not isinstance(raw, list):
        raise typer.BadParameter("events-json must contain a list of events.")
    events = [WhatIfEvent.model_validate(item) for item in raw]
    if not events:
        raise typer.BadParameter("event sample is empty.")

    org_context = _read_json(org_context_file) if org_context_file else None
    typer.echo(
        f"proposing manifest for tenant={tenant} from {len(events)} events using {provider}:{model}"
    )
    result = propose_manifest(
        tenant_id=tenant,
        events=events,
        org_context=org_context,
        provider=provider,
        model=model,
        max_events=max_events,
    )
    target = _resolve_output(output, tenant, "llm")
    save_manifest(result.manifest, target)
    typer.echo(
        f"wrote proposed manifest: {target} ({len(result.manifest.heads)} heads, "
        f"provider={result.provider}, model={result.model})"
    )


@app.command("diff")
def diff_cmd(
    a: Path = typer.Argument(..., help="Path to manifest A (typically the fixture)."),
    b: Path = typer.Argument(
        ..., help="Path to manifest B (typically the proposed manifest)."
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Write markdown to file. Default: print to stdout."
    ),
) -> None:
    """Print a markdown diff comparing two manifests head-by-head."""
    from vei.whatif.measurement_diff import diff_manifests, render_diff_markdown
    from vei.whatif.measurement_manifest import load_manifest

    manifest_a = load_manifest(a)
    manifest_b = load_manifest(b)
    diff = diff_manifests(manifest_a, manifest_b)
    rendered = render_diff_markdown(diff)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        typer.echo(f"wrote diff: {output}")
    else:
        typer.echo(rendered)


@app.command("list-tenants")
def list_tenants_cmd() -> None:
    """List tenants that have a built-in domain spec in source today."""
    from vei.whatif.measurement_fixtures import known_fixture_tenants

    for name in known_fixture_tenants():
        typer.echo(name)


@app.command("sample-events")
def sample_events_cmd(
    corpus: str = typer.Option(
        ..., "--corpus", help="Built-in corpus to sample (e.g. enron)."
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Output JSON path for the WhatIfEvent list.",
    ),
) -> None:
    """Dump a built-in event sample to JSON for use with `vei mcm propose`."""
    from vei.whatif.measurement_samples import (
        available_corpora,
        sample_events_for_corpus,
    )

    try:
        events = sample_events_for_corpus(corpus)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{exc}\nAvailable corpora: {', '.join(available_corpora())}"
        ) from exc

    output.parent.mkdir(parents=True, exist_ok=True)
    payload = [e.model_dump(mode="json") for e in events]
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    typer.echo(f"wrote {len(events)} events to {output}")


if __name__ == "__main__":
    app()
