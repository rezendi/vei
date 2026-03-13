from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Literal

from pydantic import BaseModel, Field

from vei.run.api import launch_workspace_run
from vei.verticals.packs import (
    VerticalPackManifest,
    get_vertical_pack_manifest,
    list_vertical_pack_manifests,
)
from vei.workspace.api import create_workspace_from_template, preview_workspace_scenario


VerticalCompareRunner = Literal["scripted", "bc", "llm"]


class VerticalDemoSpec(BaseModel):
    vertical_name: str
    workspace_root: Path
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None


class VerticalDemoResult(BaseModel):
    manifest: VerticalPackManifest
    workspace_root: Path
    scenario_name: str
    workflow_run_id: str
    comparison_run_id: str
    compare_runner: VerticalCompareRunner
    workflow_manifest_path: Path
    comparison_manifest_path: Path
    contract_path: Path
    overview_path: Path
    ui_command: str
    what_if_branches: list[str] = Field(default_factory=list)
    baseline_success: bool = False
    comparison_success: bool = False
    baseline_contract_ok: bool | None = None
    comparison_contract_ok: bool | None = None


class VerticalShowcaseSpec(BaseModel):
    vertical_names: list[str] = Field(default_factory=list)
    root: Path
    compare_runner: VerticalCompareRunner = "scripted"
    overwrite: bool = True
    seed: int = 42042
    max_steps: int = 18
    compare_model: str | None = None
    compare_provider: str | None = None
    compare_bc_model_path: Path | None = None
    compare_task: str | None = None
    run_id: str = "vertical_showcase"


class VerticalShowcaseResult(BaseModel):
    run_id: str
    root: Path
    compare_runner: VerticalCompareRunner
    overview_path: Path
    result_path: Path
    demos: list[VerticalDemoResult] = Field(default_factory=list)


def prepare_vertical_demo(spec: VerticalDemoSpec) -> VerticalDemoResult:
    if spec.compare_runner == "llm" and not spec.compare_model:
        raise ValueError("llm comparison requires compare_model")
    if spec.compare_runner == "bc" and spec.compare_bc_model_path is None:
        raise ValueError("bc comparison requires compare_bc_model_path")
    manifest = get_vertical_pack_manifest(spec.vertical_name)
    create_workspace_from_template(
        root=spec.workspace_root,
        source_kind="vertical",
        source_ref=manifest.name,
        overwrite=spec.overwrite,
    )
    workflow_manifest = launch_workspace_run(
        spec.workspace_root,
        runner="workflow",
        run_id="workflow_baseline",
        seed=spec.seed,
        max_steps=spec.max_steps,
    )
    comparison_manifest = launch_workspace_run(
        spec.workspace_root,
        runner=spec.compare_runner,
        run_id=f"{spec.compare_runner}_comparison",
        seed=spec.seed,
        model=spec.compare_model,
        provider=spec.compare_provider,
        bc_model_path=spec.compare_bc_model_path,
        task=spec.compare_task,
        max_steps=spec.max_steps,
    )
    preview = preview_workspace_scenario(spec.workspace_root)
    workspace_root = Path(spec.workspace_root).expanduser().resolve()
    run_root = workspace_root / "runs"
    overview_path = workspace_root / "vertical_demo_overview.md"
    what_if_branches = _extract_what_if_branches(preview) or list(
        manifest.what_if_branches
    )
    result = VerticalDemoResult(
        manifest=manifest,
        workspace_root=workspace_root,
        scenario_name=str(preview["scenario"]["name"]),
        workflow_run_id=workflow_manifest.run_id,
        comparison_run_id=comparison_manifest.run_id,
        compare_runner=spec.compare_runner,
        workflow_manifest_path=run_root
        / workflow_manifest.run_id
        / "run_manifest.json",
        comparison_manifest_path=run_root
        / comparison_manifest.run_id
        / "run_manifest.json",
        contract_path=workspace_root
        / "contracts"
        / f"{preview['scenario']['name']}.contract.json",
        overview_path=overview_path,
        ui_command=(
            "python -m vei.cli.vei ui serve "
            f"--root {workspace_root} --host 127.0.0.1 --port 3011"
        ),
        what_if_branches=what_if_branches,
        baseline_success=workflow_manifest.success,
        comparison_success=comparison_manifest.success,
        baseline_contract_ok=(
            workflow_manifest.contract.ok if workflow_manifest.contract else None
        ),
        comparison_contract_ok=(
            comparison_manifest.contract.ok if comparison_manifest.contract else None
        ),
    )
    overview_path.write_text(render_vertical_demo_overview(result), encoding="utf-8")
    (workspace_root / "vertical_demo_result.json").write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result


def run_vertical_showcase(spec: VerticalShowcaseSpec) -> VerticalShowcaseResult:
    showcase_root = spec.root.expanduser().resolve() / spec.run_id
    showcase_root.mkdir(parents=True, exist_ok=True)
    selected = (
        [get_vertical_pack_manifest(name) for name in spec.vertical_names]
        if spec.vertical_names
        else list_vertical_pack_manifests()
    )
    demos: list[VerticalDemoResult] = []
    for item in selected:
        demos.append(
            prepare_vertical_demo(
                VerticalDemoSpec(
                    vertical_name=item.name,
                    workspace_root=showcase_root / item.name,
                    compare_runner=spec.compare_runner,
                    overwrite=spec.overwrite,
                    seed=spec.seed,
                    max_steps=spec.max_steps,
                    compare_model=spec.compare_model,
                    compare_provider=spec.compare_provider,
                    compare_bc_model_path=spec.compare_bc_model_path,
                    compare_task=spec.compare_task,
                )
            )
        )
    result = VerticalShowcaseResult(
        run_id=spec.run_id,
        root=showcase_root,
        compare_runner=spec.compare_runner,
        overview_path=showcase_root / "vertical_showcase_overview.md",
        result_path=showcase_root / "vertical_showcase_result.json",
        demos=demos,
    )
    result.overview_path.write_text(
        render_vertical_showcase_overview(result), encoding="utf-8"
    )
    result.result_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2), encoding="utf-8"
    )
    return result


def render_vertical_demo_overview(result: VerticalDemoResult) -> str:
    lines = [
        f"# {result.manifest.title}",
        "",
        result.manifest.description,
        "",
        f"- Company: `{result.manifest.company_name}`",
        f"- Scenario: `{result.scenario_name}`",
        f"- Workflow baseline: `{result.workflow_run_id}`",
        f"- Comparison ({result.compare_runner}): `{result.comparison_run_id}`",
        f"- Contract path: `{result.contract_path}`",
        f"- UI: `{result.ui_command}`",
        "",
        "What this proves:",
    ]
    lines.extend(f"- {bullet}" for bullet in result.manifest.proves)
    if result.what_if_branches:
        lines.extend(["", "What-if branches:"])
        lines.extend(f"- {branch}" for branch in result.what_if_branches)
    return "\n".join(lines).rstrip() + "\n"


def render_vertical_showcase_overview(result: VerticalShowcaseResult) -> str:
    lines = [
        "# VEI Vertical World Pack Showcase",
        "",
        f"Run ID: `{result.run_id}`",
        f"Comparison runner: `{result.compare_runner}`",
        f"Workspaces: `{len(result.demos)}`",
        "",
    ]
    for demo in result.demos:
        lines.extend(
            [
                f"## {demo.manifest.title}",
                "",
                demo.manifest.description,
                "",
                f"- Workspace: `{demo.workspace_root}`",
                f"- Baseline contract: `{demo.baseline_contract_ok}`",
                f"- Comparison contract: `{demo.comparison_contract_ok}`",
                f"- Overview: `{demo.overview_path}`",
                f"- UI: `{demo.ui_command}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def resolve_vertical_names(names: Iterable[str] | None = None) -> list[str]:
    cleaned = [name.strip().lower() for name in (names or []) if name.strip()]
    if cleaned:
        return cleaned
    return [item.name for item in list_vertical_pack_manifests()]


def _extract_what_if_branches(preview: dict[str, object]) -> list[str]:
    scenario = preview.get("scenario")
    if not isinstance(scenario, dict):
        return []
    metadata = scenario.get("metadata")
    if not isinstance(metadata, dict):
        return []
    builder_environment = metadata.get("builder_environment")
    if not isinstance(builder_environment, dict):
        return []
    branches = builder_environment.get("what_if_branches")
    if not isinstance(branches, list):
        return []
    return [str(item) for item in branches if str(item).strip()]


__all__ = [
    "VerticalCompareRunner",
    "VerticalDemoResult",
    "VerticalDemoSpec",
    "VerticalShowcaseResult",
    "VerticalShowcaseSpec",
    "prepare_vertical_demo",
    "render_vertical_demo_overview",
    "render_vertical_showcase_overview",
    "resolve_vertical_names",
    "run_vertical_showcase",
]
