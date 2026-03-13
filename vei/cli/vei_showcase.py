from __future__ import annotations

import json
from pathlib import Path

import typer

from vei.verticals import (
    get_vertical_pack_manifest,
)
from vei.verticals.demo import (
    VerticalShowcaseSpec,
    resolve_vertical_names,
    run_vertical_showcase,
)


app = typer.Typer(
    add_completion=False,
    help="Run polished showcase bundles for VEI product demos.",
)


@app.command("verticals")
def verticals_command(
    root: Path = typer.Option(
        Path("_vei_out/vertical_showcase"),
        help="Root directory for generated showcase workspaces and bundles",
    ),
    vertical: list[str] = typer.Option(
        [],
        "--vertical",
        "-v",
        help="Optional subset of vertical packs to include",
    ),
    compare_runner: str = typer.Option(
        "scripted", help="Comparison runner for each vertical: scripted|bc|llm"
    ),
    run_id: str = typer.Option("vertical_showcase", help="Showcase bundle identifier"),
    overwrite: bool = typer.Option(
        True, help="Recreate each vertical workspace before running"
    ),
    seed: int = typer.Option(42042, help="Seed for reproducibility"),
    max_steps: int = typer.Option(18, help="Max steps for comparison runs"),
    compare_model: str | None = typer.Option(
        None, help="Model name when compare-runner=llm"
    ),
    compare_provider: str | None = typer.Option(
        None, help="Provider name when compare-runner=llm"
    ),
    compare_bc_model: Path | None = typer.Option(
        None,
        exists=True,
        readable=True,
        help="BC policy file when compare-runner=bc",
    ),
) -> None:
    normalized_runner = compare_runner.strip().lower()
    if normalized_runner not in {"scripted", "bc", "llm"}:
        raise typer.BadParameter("compare-runner must be one of scripted|bc|llm")
    if normalized_runner == "llm" and not compare_model:
        raise typer.BadParameter("llm showcase requires --compare-model")
    if normalized_runner == "bc" and compare_bc_model is None:
        raise typer.BadParameter("bc showcase requires --compare-bc-model")

    selected_verticals = resolve_vertical_names(vertical)
    for name in selected_verticals:
        try:
            get_vertical_pack_manifest(name)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc

    result = run_vertical_showcase(
        VerticalShowcaseSpec(
            vertical_names=selected_verticals,
            root=root,
            compare_runner=normalized_runner,  # type: ignore[arg-type]
            overwrite=overwrite,
            seed=seed,
            max_steps=max_steps,
            compare_model=compare_model,
            compare_provider=compare_provider,
            compare_bc_model_path=compare_bc_model,
            run_id=run_id,
        )
    )
    typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    app()
