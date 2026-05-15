from __future__ import annotations

import csv
import json
import math
import os
import re
import shutil
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from vei.context.api import (
    ContextSnapshot,
    capture_pipeshub_context_from_env,
    capture_teams_graph_context_from_env,
    context_iso_now,
    merge_context_source_results,
    new_pipeshub_capture_run_id,
    new_teams_graph_capture_run_id,
    verify_context_snapshot,
    write_canonical_history_sidecars,
    write_pipeshub_capture_outputs,
    write_teams_graph_capture_outputs,
)
from vei.skillmap.api import (
    CompanySkillMap,
    build_company_skill_map_from_context_path,
    enrich_skill_map_with_world_model_opportunities,
    write_company_skill_map_outputs,
)
from vei.workflow.api import mine_workflows

RefreshMode = Literal["incremental-validated", "full-validated"]
RefreshStatus = Literal["validated", "not_validated"]
FreshnessPolicy = Literal[
    "all-sources-current",
    "bundle-current",
    "allow-stale-caveats",
]

DEFAULT_CONTEXT_BUNDLE = Path(
    "_vei_out/py-insights/combined-live-20260504-20260513/context_snapshot.json"
)
DEFAULT_DAILY_ROOT = Path("_vei_out/pyinsights_daily_refresh")
DEFAULT_WORLD_MODEL_ROOT = Path("_vei_out/world_model_multitenant_jepa")
DEFAULT_STRATEGIC_ROOT = Path("_vei_out/world_model_strategic_state_points")
DEFAULT_TENANT_ID = "pyinsights"
DEFAULT_CAPTURE_PROVIDERS = ("onedrive", "outlook", "teams")
_PIPESHUB_ONLY_PROVIDERS = frozenset({"onedrive", "outlook"})

_EVALUATION_LEVEL_ORDER = {
    "descriptive": 0,
    "labeled": 1,
    "rubric_evaluable": 2,
    "contract_evaluable": 3,
    "rl_packaged": 4,
}
_ALLOWED_WORKFLOW_GENERATORS = {
    "skillmap_semantic_v1",
    "world_model_skill_opportunity_v1",
}
_NOISY_WORKFLOW_TITLE_RE = re.compile(
    r"^(?:hi\b|hello\b|hey\b|ok\b|okay\b|sure\b|yes\b|no\b|thanks\b|"
    r"thank you\b|done\b|cool\b|great\b|chat/|https?://|www\.)",
    re.IGNORECASE,
)

_RAW_SECRET_PATTERNS = (
    re.compile(r"\b[a-z][a-z0-9+.-]*://\S+@\S+", re.IGNORECASE),
    re.compile(
        r"(?i)\b(?:api[_-]?key|token|secret|password|passwd|pwd|cvv)\s*[=:]\s*[^\s,;]+"
    ),
    re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE),
)


class DailyRefreshCheck(BaseModel):
    code: str
    passed: bool
    severity: Literal["error", "warning"] = "error"
    detail: str = ""


class DailyRefreshManifest(BaseModel):
    version: str = "pyinsights_daily_refresh_v1"
    status: RefreshStatus
    mode: RefreshMode
    implementation_stage: str = "validated_artifact_reuse_v1"
    tenant_id: str = DEFAULT_TENANT_ID
    as_of: str
    run_root: str
    previous_valid_run: str = ""
    validation_passed: bool
    source_watermarks: dict[str, Any] = Field(default_factory=dict)
    references: dict[str, str] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    checks: list[DailyRefreshCheck] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class FreshContextCaptureResult:
    context_path: Path | None = None
    checks: list[DailyRefreshCheck] = field(default_factory=list)
    references: dict[str, str] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def run_validated_daily_refresh(
    *,
    as_of: str | date,
    mode: RefreshMode = "incremental-validated",
    previous: str | Path = "latest-valid",
    context_bundle: str | Path = DEFAULT_CONTEXT_BUNDLE,
    output_root: str | Path = DEFAULT_DAILY_ROOT,
    label: str = "",
    model_run_root: str | Path | None = None,
    strategic_run_root: str | Path | None = None,
    workflow_output: str | Path | None = None,
    skill_map_path: str | Path | None = None,
    source_freshness_policy: FreshnessPolicy = "all-sources-current",
    refresh_workflows: bool = True,
    allow_report_on_warning: bool = True,
    fresh_capture: bool = True,
    capture_workspace: str | Path | None = None,
    capture_connectors: list[str] | None = None,
    capture_limit: int = 5000,
    capture_timeout_s: int = 30,
    capture_include_content: bool = False,
) -> DailyRefreshManifest:
    """Validate a Py Insights daily run and emit the CEO report only if trusted.

    V1 intentionally reuses the current artifact-producing steps instead of
    pretending true delta ingestion and warm-start training exist. The product
    contract is already enforced: failed validation writes a failure note, not a
    CEO report.
    """

    as_of_date = _resolve_as_of(as_of)
    output_base = Path(output_root).expanduser().resolve()
    previous_valid = _resolve_previous_valid(previous, output_base)
    run_label = label.strip() or f"pyinsights_daily_{as_of_date:%Y%m%d}"
    run_root = output_base / _slug(run_label)
    run_root.mkdir(parents=True, exist_ok=True)

    checks: list[DailyRefreshCheck] = []
    notes: list[str] = []
    capture_references: dict[str, str] = {}
    if previous_valid == run_root:
        notes.append(
            "Previous run resolved to current run root; ignoring it for comparison."
        )
        previous_valid = None
    if previous_valid is None:
        notes.append(
            "No previous validated daily run was found; using full validation fallback."
        )
    configured_context_path = Path(context_bundle).expanduser().resolve()
    configured_context_root = configured_context_path.parent
    context_path = configured_context_path
    if fresh_capture:
        capture_result = _capture_fresh_context_for_as_of(
            existing_context_path=context_path,
            as_of_date=as_of_date,
            run_root=run_root,
            capture_workspace=(
                Path(capture_workspace).expanduser().resolve()
                if capture_workspace is not None
                else None
            ),
            capture_connectors=capture_connectors,
            limit=capture_limit,
            timeout_s=capture_timeout_s,
            include_content=capture_include_content,
        )
        checks.extend(capture_result.checks)
        notes.extend(capture_result.notes)
        capture_references.update(capture_result.references)
        if capture_result.context_path is not None:
            context_path = capture_result.context_path
    context_root = context_path.parent
    event_index_path = context_root / "canonical_event_index.json"
    canonical_events_path = context_root / "canonical_events.jsonl"
    verify_path = _latest_context_verify(context_root)
    skill_path = _resolve_skill_map_path(
        skill_map_path,
        context_root=context_root,
        configured_context_root=configured_context_root,
        previous_valid=previous_valid,
    )
    model_root = (
        Path(model_run_root).expanduser().resolve()
        if model_run_root is not None
        else _find_latest_model_run(DEFAULT_WORLD_MODEL_ROOT)
    )
    strategic_root = (
        Path(strategic_run_root).expanduser().resolve()
        if strategic_run_root is not None
        else _find_latest_strategic_run(DEFAULT_STRATEGIC_ROOT)
    )
    strategic_csv_path = strategic_root / "strategic_state_point_results.csv"
    strategic_json_path = strategic_root / "strategic_state_point_results.json"
    saturation_guard_path = (
        strategic_root / "strategic_state_point_saturation_guard.json"
    )
    source_ceo_report_path = _find_ceo_report(strategic_root)
    dataset_manifest_path = model_root / "dataset" / "dataset_manifest.json"
    checkpoint_path = model_root / "model_runs" / "jepa_latent" / "model.pt"
    train_result_path = model_root / "model_runs" / "jepa_latent" / "train_result.json"
    eval_result_path = model_root / "model_runs" / "jepa_latent" / "eval_result.json"
    target_manifest_path = model_root / "target_manifests" / f"{DEFAULT_TENANT_ID}.json"
    effective_skill_path = skill_path
    skill_output_root = run_root / "skill_map"
    workflow_root = (
        Path(workflow_output).expanduser().resolve()
        if workflow_output is not None
        else run_root / "workflows"
    )

    _check_required_files(
        checks,
        [
            context_path,
            event_index_path,
            canonical_events_path,
            verify_path,
            skill_path,
            dataset_manifest_path,
            checkpoint_path,
            train_result_path,
            eval_result_path,
            target_manifest_path,
            strategic_csv_path,
            strategic_json_path,
            saturation_guard_path,
        ],
    )

    context_payload = _read_json_or_empty(context_path)
    event_index = _read_json_or_empty(event_index_path)
    verify_payload = _read_json_or_empty(verify_path)
    canonical_event_ids = _read_jsonl_event_ids(canonical_events_path)
    index_rows = event_index.get("rows", []) if isinstance(event_index, dict) else []
    index_event_ids = [
        str(row.get("event_id", ""))
        for row in index_rows
        if isinstance(row, dict) and row.get("event_id")
    ]
    source_watermarks = _source_watermarks(context_payload, event_index)

    _validate_canonical_spine(
        checks,
        as_of_date=as_of_date,
        context_payload=context_payload,
        event_index=event_index,
        verify_payload=verify_payload,
        canonical_event_ids=canonical_event_ids,
        index_event_ids=index_event_ids,
        index_rows=index_rows,
        source_freshness_policy=source_freshness_policy,
    )
    _validate_previous_immutability(
        checks,
        previous_valid=previous_valid,
        current_event_index_path=event_index_path,
        current_event_ids=index_event_ids,
    )

    should_refresh_skillmap_from_context = (
        skill_map_path is None and context_path != configured_context_path
    )
    refreshed_skill_map = False
    if should_refresh_skillmap_from_context:
        try:
            base_skill_map = build_company_skill_map_from_context_path(
                context_path,
                limit=12,
                include_replay=True,
                previous_map_path=str(skill_path) if skill_path.is_file() else None,
            )
            enriched_skill_map = base_skill_map
            if strategic_csv_path.is_file():
                enriched_skill_map = enrich_skill_map_with_world_model_opportunities(
                    base_skill_map,
                    context_path=context_path,
                    world_model_report_path=strategic_csv_path,
                    max_opportunities=8,
                    require_trusted_ranking=True,
                )
            _demote_activation_candidates_without_owners(enriched_skill_map)
            skill_outputs = write_company_skill_map_outputs(
                enriched_skill_map, skill_output_root
            )
            effective_skill_path = skill_outputs["json"]
            refreshed_skill_map = True
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.refresh_from_context_bundle_ran",
                    passed=True,
                    detail=str(effective_skill_path),
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.refresh_from_context_bundle_ran",
                    passed=False,
                    severity="warning",
                    detail=str(exc),
                )
            )
    if not refreshed_skill_map and skill_path.is_file():
        try:
            base_skill_map = CompanySkillMap.model_validate_json(
                skill_path.read_text(encoding="utf-8")
            )
            enriched_skill_map = base_skill_map
            if strategic_csv_path.is_file():
                enriched_skill_map = enrich_skill_map_with_world_model_opportunities(
                    base_skill_map,
                    context_path=context_path,
                    world_model_report_path=strategic_csv_path,
                    max_opportunities=8,
                    require_trusted_ranking=True,
                )
            _demote_activation_candidates_without_owners(enriched_skill_map)
            skill_outputs = write_company_skill_map_outputs(
                enriched_skill_map, skill_output_root
            )
            effective_skill_path = skill_outputs["json"]
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.reuse_prior_skill_map_ran",
                    passed=True,
                    detail=str(effective_skill_path),
                )
            )
        except Exception as fallback_exc:  # noqa: BLE001
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.reuse_prior_skill_map_ran",
                    passed=False,
                    detail=str(fallback_exc),
                )
            )

    if refresh_workflows and strategic_csv_path.is_file():
        mine_workflows(
            context_path,
            output=workflow_root,
            limit=25,
            skill_map_path=effective_skill_path,
            world_model_report_path=strategic_csv_path,
        )
    elif workflow_output is None:
        workflow_root = (
            context_root / "workflows_semantic_v2"
            if (
                context_root / "workflows_semantic_v2" / "workflow_candidates.json"
            ).is_file()
            else context_root / "workflows"
        )

    workflow_result_path = workflow_root / "workflow_candidates.json"
    workflow_manifest_path = workflow_root / "workflow_mining_manifest.json"
    workflows_payload = _read_json_or_empty(workflow_result_path)
    skill_payload = _read_json_or_empty(effective_skill_path)
    skill_reference_event_ids = set(index_event_ids)
    if skill_path.is_file():
        skill_context_root = skill_path.parent.parent
        skill_event_index_path = skill_context_root / "canonical_event_index.json"
        if skill_event_index_path.is_file():
            skill_event_index = _read_json_or_empty(skill_event_index_path)
            skill_reference_event_ids |= {
                str(row.get("event_id", ""))
                for row in skill_event_index.get("rows", []) or []
                if isinstance(row, dict) and row.get("event_id")
            }
    _validate_workflows(
        checks,
        workflows_payload=workflows_payload,
        workflow_manifest=_read_json_or_empty(workflow_manifest_path),
        valid_event_ids=set(index_event_ids),
    )
    _validate_skill_map(
        checks,
        skill_payload=skill_payload,
        valid_event_ids=skill_reference_event_ids,
    )
    _validate_model_artifacts(
        checks,
        dataset_manifest=_read_json_or_empty(dataset_manifest_path),
        train_result=_read_json_or_empty(train_result_path),
        eval_result=_read_json_or_empty(eval_result_path),
        target_manifest=_read_json_or_empty(target_manifest_path),
        target_manifest_path=target_manifest_path,
        checkpoint_path=checkpoint_path,
        previous_valid=previous_valid,
    )
    _validate_strategic_run(
        checks,
        strategic_rows=_read_csv_rows(strategic_csv_path),
        strategic_payload=_read_json_or_empty(strategic_json_path),
        saturation_guard=_read_json_or_empty(saturation_guard_path),
    )

    errors = [
        check for check in checks if not check.passed and check.severity == "error"
    ]
    warnings = [
        check for check in checks if not check.passed and check.severity == "warning"
    ]
    validation_passed = not errors and (allow_report_on_warning or not warnings)
    status: RefreshStatus = "validated" if validation_passed else "not_validated"

    artifacts: dict[str, str] = {
        "validation_manifest": str(run_root / "validation_manifest.json"),
        "workflow_skill_summary": str(run_root / "workflow_skill_refresh_summary.md"),
        "workflow_discovery_summary": str(
            run_root / "workflow_skill_refresh_summary.md"
        ),
        "skill_map": str(effective_skill_path),
    }
    references = {
        "context_bundle": str(context_path),
        "canonical_event_index": str(event_index_path),
        "canonical_events": str(canonical_events_path),
        "context_verify": str(verify_path),
        "source_skill_map": str(skill_path),
        "skill_map": str(effective_skill_path),
        "workflow_candidates": str(workflow_result_path),
        "workflow_mining_manifest": str(workflow_manifest_path),
        "dataset_manifest": str(dataset_manifest_path),
        "jepa_checkpoint": str(checkpoint_path),
        "train_result": str(train_result_path),
        "eval_result": str(eval_result_path),
        "target_manifest": str(target_manifest_path),
        "strategic_results_csv": str(strategic_csv_path),
        "strategic_results_json": str(strategic_json_path),
        "saturation_guard": str(saturation_guard_path),
    }
    if source_ceo_report_path is not None:
        references["source_ceo_report"] = str(source_ceo_report_path)
    references.update(capture_references)

    if validation_passed:
        report_path = (
            run_root / f"pyinsights_ceo_counterfactual_report_{as_of_date:%Y%m%d}.md"
        )
        if source_ceo_report_path is not None and source_ceo_report_path.is_file():
            shutil.copy2(source_ceo_report_path, report_path)
        else:
            report_path.write_text(
                _render_minimal_validated_report(as_of_date, workflows_payload),
                encoding="utf-8",
            )
        artifacts["ceo_report"] = str(report_path)
    else:
        failure_path = run_root / "validation_failure.md"
        failure_path.write_text(
            _render_validation_failure(as_of_date, checks, references),
            encoding="utf-8",
        )
        artifacts["validation_failure_note"] = str(failure_path)

    summary_path = run_root / "workflow_skill_refresh_summary.md"
    summary_path.write_text(
        _render_workflow_skill_summary(
            as_of_date=as_of_date,
            workflows_payload=workflows_payload,
            skill_payload=skill_payload,
            workflow_manifest=_read_json_or_empty(workflow_manifest_path),
        ),
        encoding="utf-8",
    )

    manifest = DailyRefreshManifest(
        status=status,
        mode=mode,
        as_of=as_of_date.isoformat(),
        run_root=str(run_root),
        previous_valid_run=str(previous_valid) if previous_valid is not None else "",
        validation_passed=validation_passed,
        source_watermarks=source_watermarks,
        references=references,
        artifacts=artifacts,
        checks=checks,
        notes=notes,
    )
    (run_root / "validation_manifest.json").write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def _resolve_skill_map_path(
    skill_map_path: str | Path | None,
    *,
    context_root: Path,
    configured_context_root: Path,
    previous_valid: Path | None,
) -> Path:
    if skill_map_path is not None:
        resolved = Path(skill_map_path).expanduser().resolve()
        if not resolved.is_file():
            raise FileNotFoundError(f"skill map not found: {resolved}")
        return resolved

    candidates: list[Path] = []
    if previous_valid is not None:
        candidates.append(previous_valid / "skill_map" / "company_skill_map.json")
        candidates.append(
            previous_valid / ".artifacts" / "skillmap" / "company_skill_map.json"
        )

    candidates.extend(
        [
            configured_context_root / "skill_map" / "company_skill_map.json",
            configured_context_root
            / ".artifacts"
            / "skillmap"
            / "company_skill_map.json",
            context_root / "skill_map" / "company_skill_map.json",
            context_root / ".artifacts" / "skillmap" / "company_skill_map.json",
        ]
    )

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    joined = "\n".join(f"- {path}" for path in candidates)
    raise FileNotFoundError(f"skill map not found; tried:\n{joined}")


def _capture_fresh_context_for_as_of(
    *,
    existing_context_path: Path,
    as_of_date: date,
    run_root: Path,
    capture_workspace: Path | None,
    capture_connectors: list[str] | None,
    limit: int,
    timeout_s: int,
    include_content: bool,
) -> FreshContextCaptureResult:
    _load_dotenv_if_available()
    existing_payload = _read_json_or_empty(existing_context_path)
    expected_providers = _expected_capture_providers(
        existing_payload,
        capture_connectors=capture_connectors,
    )
    expected_providers, skipped_providers = _prune_expected_providers_for_env(
        expected_providers
    )
    if _context_bundle_current_for_as_of(
        existing_payload,
        as_of_date=as_of_date,
        expected_providers=expected_providers,
    ):
        return FreshContextCaptureResult(
            checks=[
                DailyRefreshCheck(
                    code="source.fresh_capture_for_as_of",
                    passed=True,
                    detail=f"existing context bundle is current for {as_of_date}",
                )
            ],
            notes=["Existing context bundle is already fresh for the as-of date."],
        )

    workspace = capture_workspace or run_root / "source_capture"
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        capture_result = _run_live_context_capture(
            workspace=workspace,
            existing_context_payload=existing_payload,
            as_of_date=as_of_date,
            expected_providers=expected_providers,
            capture_connectors=capture_connectors,
            limit=limit,
            timeout_s=timeout_s,
            include_content=include_content,
        )
        if skipped_providers:
            capture_result.checks.append(
                DailyRefreshCheck(
                    code="source.expected_providers_skipped_unconfigured",
                    passed=True,
                    severity="warning",
                    detail=f"skipped={sorted(skipped_providers)}",
                )
            )
            capture_result.notes.append(
                "Fresh capture skipped unconfigured providers: "
                + ", ".join(sorted(skipped_providers))
                + "."
            )
        return capture_result
    except Exception as exc:  # noqa: BLE001
        return FreshContextCaptureResult(
            checks=[
                DailyRefreshCheck(
                    code="source.fresh_capture_for_as_of",
                    passed=False,
                    detail=(
                        f"fresh source capture failed for {as_of_date}: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )
            ],
            notes=[
                "Fresh source capture failed; daily validation continued against "
                "the configured context bundle and should withhold trusted reports."
            ],
        )


def _run_live_context_capture(
    *,
    workspace: Path,
    existing_context_payload: dict[str, Any],
    as_of_date: date,
    expected_providers: set[str],
    capture_connectors: list[str] | None,
    limit: int,
    timeout_s: int,
    include_content: bool,
) -> FreshContextCaptureResult:
    _load_dotenv_if_available()
    organization_name = (
        str(existing_context_payload.get("organization_name") or "").strip()
        or "Py Insights"
    )
    organization_domain = str(
        existing_context_payload.get("organization_domain") or ""
    ).strip()
    connector_names = _normalized_capture_connectors(
        capture_connectors,
        expected_providers=expected_providers,
    )

    snapshots: list[Any] = []
    references: dict[str, str] = {}
    notes: list[str] = []
    capture_errors: list[str] = []

    if _pipeshub_capture_configured():
        pipeshub_captures = _capture_pipeshub_connector_snapshots(
            workspace=workspace,
            organization_name=organization_name,
            organization_domain=organization_domain,
            connector_names=connector_names,
            expected_providers=expected_providers,
            limit=limit,
            timeout_s=timeout_s,
            include_content=include_content,
        )
        snapshots.extend(pipeshub_captures.snapshots)
        references.update(pipeshub_captures.references)
        notes.extend(pipeshub_captures.notes)
        capture_errors.extend(pipeshub_captures.errors)
    else:
        pipeshub_required = bool(expected_providers & _PIPESHUB_ONLY_PROVIDERS)
        if pipeshub_required:
            capture_errors.append(
                "pipeshub: missing PIPESHUB_BASE_URL/PIPESHUB_BEARER_AUTH configuration"
            )

    captured_providers = _snapshot_provider_names(snapshots)
    if "teams" in expected_providers and "teams" not in captured_providers:
        if _teams_graph_capture_configured():
            try:
                run_id = new_teams_graph_capture_run_id()
                teams_workspace = workspace / "microsoft_teams"
                sync_root = (
                    teams_workspace
                    / "imports"
                    / "source_syncs"
                    / "microsoft_teams"
                    / run_id
                )
                capture = capture_teams_graph_context_from_env(
                    timeout_s=timeout_s,
                    organization_name=organization_name,
                    organization_domain=organization_domain,
                    limit=limit,
                    run_id=run_id,
                    manifest_path=sync_root / "capture_manifest.json",
                    raw_records_path=sync_root / "records.jsonl",
                )
                write_teams_graph_capture_outputs(
                    capture,
                    workspace=teams_workspace,
                    output=workspace / "teams_context_snapshot.json",
                )
                snapshots.append(capture.snapshot)
                references["source_capture_teams_report"] = str(
                    sync_root / "capture_report.json"
                )
                notes.append("Microsoft Graph Teams fresh capture completed.")
            except Exception as exc:  # noqa: BLE001
                capture_errors.append(
                    f"microsoft_graph_teams: {type(exc).__name__}: {exc}"
                )
        else:
            capture_errors.append(
                "microsoft_graph_teams: missing VEI_MSFT_TENANT_ID/"
                "VEI_MSFT_CLIENT_ID/VEI_MSFT_CLIENT_SECRET configuration"
            )

    if not snapshots:
        return FreshContextCaptureResult(
            checks=[
                DailyRefreshCheck(
                    code="source.fresh_capture_for_as_of",
                    passed=False,
                    detail="; ".join(capture_errors),
                )
            ],
            references=references,
            notes=notes,
        )

    combined_path = workspace / "context_snapshot.json"
    combined_snapshot = _combine_context_snapshots(
        snapshots,
        organization_name=organization_name,
        organization_domain=organization_domain,
        as_of_date=as_of_date,
    )
    combined_path.write_text(
        combined_snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    paths = write_canonical_history_sidecars(combined_snapshot, combined_path)
    verify_result = verify_context_snapshot(
        combined_snapshot,
        snapshot_path=combined_path,
    )
    verify_path = workspace / f"context_verify_{as_of_date:%Y%m%d}.json"
    verify_path.write_text(
        verify_result.model_dump_json(indent=2),
        encoding="utf-8",
    )

    final_providers = _snapshot_provider_names([combined_snapshot])
    missing_providers = sorted(expected_providers - final_providers)
    checks = [
        DailyRefreshCheck(
            code="source.fresh_capture_for_as_of",
            passed=True,
            detail=f"context={combined_path} providers={sorted(final_providers)}",
        ),
        DailyRefreshCheck(
            code="source.fresh_capture_expected_providers_present",
            passed=not missing_providers,
            detail=(
                f"expected={sorted(expected_providers)} "
                f"captured={sorted(final_providers)} missing={missing_providers}"
            ),
        ),
    ]
    if capture_errors:
        checks.append(
            DailyRefreshCheck(
                code="source.fresh_capture_partial_errors_absent",
                passed=False,
                severity="warning" if not missing_providers else "error",
                detail="; ".join(capture_errors),
            )
        )
    references.update(
        {
            "source_capture_context": str(combined_path),
            "source_capture_canonical_events": str(paths.events_path),
            "source_capture_canonical_event_index": str(paths.index_path),
            "source_capture_context_verify": str(verify_path),
        }
    )
    notes.append("Fresh source capture bundle was used for daily validation.")
    return FreshContextCaptureResult(
        context_path=combined_path,
        checks=checks,
        references=references,
        notes=notes,
    )


@dataclass
class _PipesHubDailyCaptureResult:
    snapshots: list[Any] = field(default_factory=list)
    references: dict[str, str] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _capture_pipeshub_connector_snapshots(
    *,
    workspace: Path,
    organization_name: str,
    organization_domain: str,
    connector_names: list[str],
    expected_providers: set[str],
    limit: int,
    timeout_s: int,
    include_content: bool,
) -> _PipesHubDailyCaptureResult:
    result = _PipesHubDailyCaptureResult()
    groups = _pipeshub_connector_capture_groups(connector_names, expected_providers)
    for group in groups:
        group_label = _pipeshub_capture_group_label(group, expected_providers)
        try:
            run_id = f"{new_pipeshub_capture_run_id()}_{group_label}"
            pipeshub_workspace = workspace / "pipeshub" / group_label
            sync_root = (
                pipeshub_workspace / "imports" / "source_syncs" / "pipeshub" / run_id
            )
            capture = capture_pipeshub_context_from_env(
                timeout_s=timeout_s,
                organization_name=organization_name,
                organization_domain=organization_domain,
                connectors=group,
                include_content=include_content,
                limit=limit,
                page_size=100,
                run_id=run_id,
                manifest_path=sync_root / "capture_manifest.json",
                raw_records_path=sync_root / "records.jsonl",
            )
            write_pipeshub_capture_outputs(
                capture,
                workspace=pipeshub_workspace,
                output=workspace / f"pipeshub_{group_label}_context_snapshot.json",
            )
            result.snapshots.append(capture.snapshot)
            reference_key = f"source_capture_pipeshub_{group_label}_report"
            result.references[reference_key] = str(sync_root / "capture_report.json")
            result.notes.append(
                "PipesHub fresh capture completed for "
                + ", ".join(group or sorted(expected_providers))
                + "."
            )
        except Exception as exc:  # noqa: BLE001
            requested = ", ".join(group or sorted(expected_providers))
            result.errors.append(f"pipeshub:{requested}: {type(exc).__name__}: {exc}")
    return result


def _pipeshub_connector_capture_groups(
    connector_names: list[str],
    expected_providers: set[str],
) -> list[list[str]]:
    names = list(
        dict.fromkeys(name.strip() for name in connector_names if name.strip())
    )
    if not names:
        names = sorted(expected_providers)
    if len(names) <= 1:
        return [names]
    return [[name] for name in names]


def _pipeshub_capture_group_label(
    connector_group: list[str],
    expected_providers: set[str],
) -> str:
    if not connector_group:
        return _slug("_".join(sorted(expected_providers)) or "all")
    return _slug("_".join(connector_group))


def _combine_context_snapshots(
    snapshots: list[Any],
    *,
    organization_name: str,
    organization_domain: str,
    as_of_date: date,
) -> Any:
    sources = []
    for snapshot in snapshots:
        sources.extend(list(snapshot.sources))
    return ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=context_iso_now(),
        sources=merge_context_source_results(sources),
        metadata={
            "snapshot_role": "company_history_bundle",
            "fresh_capture_for_as_of": as_of_date.isoformat(),
            "capture_source": "pyinsights_daily_refresh",
        },
    )


def _expected_capture_providers(
    context_payload: dict[str, Any],
    *,
    capture_connectors: list[str] | None,
) -> set[str]:
    if capture_connectors:
        return {
            _normalize_provider_name(connector)
            for connector in capture_connectors
            if connector.strip()
        }
    providers = {
        _normalize_provider_name(str(source.get("provider") or ""))
        for source in context_payload.get("sources", []) or []
        if isinstance(source, dict) and source.get("provider")
    }
    return providers or set(DEFAULT_CAPTURE_PROVIDERS)


def _prune_expected_providers_for_env(
    expected_providers: set[str],
) -> tuple[set[str], set[str]]:
    if not expected_providers:
        return expected_providers, set()
    if _pipeshub_capture_configured():
        return expected_providers, set()
    skipped = {
        provider
        for provider in expected_providers
        if provider in _PIPESHUB_ONLY_PROVIDERS
    }
    return expected_providers - skipped, skipped


def _normalized_capture_connectors(
    capture_connectors: list[str] | None,
    *,
    expected_providers: set[str],
) -> list[str]:
    if capture_connectors:
        return [
            connector.strip() for connector in capture_connectors if connector.strip()
        ]
    return sorted(expected_providers)


def _context_bundle_current_for_as_of(
    context_payload: dict[str, Any],
    *,
    as_of_date: date,
    expected_providers: set[str],
) -> bool:
    if not context_payload:
        return False
    captured_at = _parse_datetime(context_payload.get("captured_at"))
    if captured_at is None or captured_at.date() < as_of_date:
        return False
    sources = context_payload.get("sources", []) or []
    provider_dates: dict[str, datetime | None] = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        provider = _normalize_provider_name(str(source.get("provider") or ""))
        if provider:
            provider_dates[provider] = _parse_datetime(source.get("captured_at"))
    if not expected_providers.issubset(set(provider_dates)):
        return False
    return all(
        provider_dates[provider] is not None
        and provider_dates[provider].date() >= as_of_date
        for provider in expected_providers
    )


def _snapshot_provider_names(snapshots: list[Any]) -> set[str]:
    providers: set[str] = set()
    for snapshot in snapshots:
        for source in getattr(snapshot, "sources", []) or []:
            provider = _normalize_provider_name(str(getattr(source, "provider", "")))
            if provider:
                providers.add(provider)
    return providers


def _normalize_provider_name(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "microsoft_onedrive": "onedrive",
        "microsoft_outlook": "outlook",
        "microsoft_teams": "teams",
        "microsoftteams": "teams",
    }
    return aliases.get(normalized, normalized)


def _pipeshub_capture_configured() -> bool:
    return bool(os.environ.get("PIPESHUB_BEARER_AUTH", "").strip())


def _teams_graph_capture_configured() -> bool:
    return all(
        os.environ.get(name, "").strip()
        for name in (
            "VEI_MSFT_TENANT_ID",
            "VEI_MSFT_CLIENT_ID",
            "VEI_MSFT_CLIENT_SECRET",
        )
    )


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(override=False)


def _demote_activation_candidates_without_owners(skill_map: CompanySkillMap) -> None:
    """Activation-candidate is a promotion surface: require named owner+reviewer.

    The skill-map pipeline already warns when draft skills lack owner/reviewer,
    but daily validation treats activation_candidate as a hard readiness signal.
    If owner/reviewer are absent, keep the skill in shadow_ready so the daily
    run remains internally consistent without inventing ownership.
    """

    for skill in skill_map.skills:
        if skill.deployment_readiness != "activation_candidate":
            continue
        if str(skill.owner).strip() and str(skill.reviewer).strip():
            continue
        skill.deployment_readiness = "shadow_ready"


def _resolve_as_of(value: str | date) -> date:
    if isinstance(value, date):
        return value
    normalized = value.strip().lower()
    if normalized == "today":
        return datetime.now().date()
    return date.fromisoformat(normalized)


def _resolve_previous_valid(previous: str | Path, output_root: Path) -> Path | None:
    if str(previous) == "latest-valid":
        return _find_latest_valid_run(output_root)
    if str(previous).strip().lower() in {"", "none"}:
        return None
    path = Path(previous).expanduser().resolve()
    return path if path.exists() else None


def _find_latest_valid_run(output_root: Path) -> Path | None:
    candidates: list[tuple[str, float, Path]] = []
    for manifest_path in output_root.glob("*/validation_manifest.json"):
        payload = _read_json_or_empty(manifest_path)
        if payload.get("status") != "validated":
            continue
        run_root = manifest_path.parent
        candidates.append(
            (
                str(payload.get("as_of", "")),
                manifest_path.stat().st_mtime,
                run_root,
            )
        )
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][2]


def _find_latest_model_run(root: Path) -> Path:
    candidates = [
        path
        for path in root.glob("*")
        if path.is_dir()
        and (path / "model_runs" / "jepa_latent" / "model.pt").is_file()
        and (path / "target_manifests" / f"{DEFAULT_TENANT_ID}.json").is_file()
    ]
    if not candidates:
        raise FileNotFoundError(f"no Py Insights JEPA model run found under {root}")
    return max(candidates, key=lambda path: path.stat().st_mtime).resolve()


def _find_latest_strategic_run(root: Path) -> Path:
    candidates = [
        path
        for path in root.glob("*")
        if path.is_dir()
        and DEFAULT_TENANT_ID in path.name
        and (path / "strategic_state_point_saturation_guard.json").is_file()
        and (path / "strategic_state_point_results.csv").is_file()
    ]
    if not candidates:
        raise FileNotFoundError(
            f"no Py Insights strategic-state run found under {root}"
        )
    return max(candidates, key=lambda path: path.stat().st_mtime).resolve()


def _find_ceo_report(strategic_root: Path) -> Path | None:
    reports = sorted(strategic_root.glob("pyinsights_ceo_counterfactual_report_*.md"))
    return reports[-1] if reports else None


def _latest_context_verify(context_root: Path) -> Path:
    candidates = sorted(context_root.glob("context_verify*.json"))
    return candidates[-1] if candidates else context_root / "context_verify.json"


def _check_required_files(checks: list[DailyRefreshCheck], paths: list[Path]) -> None:
    for path in paths:
        checks.append(
            DailyRefreshCheck(
                code="artifact.exists",
                passed=path.is_file(),
                detail=str(path),
            )
        )


def _validate_canonical_spine(
    checks: list[DailyRefreshCheck],
    *,
    as_of_date: date,
    context_payload: dict[str, Any],
    event_index: dict[str, Any],
    verify_payload: dict[str, Any],
    canonical_event_ids: list[str],
    index_event_ids: list[str],
    index_rows: list[Any],
    source_freshness_policy: FreshnessPolicy,
) -> None:
    checks.append(
        DailyRefreshCheck(
            code="canonical.index_event_count_matches_rows",
            passed=int(event_index.get("event_count") or -1) == len(index_rows),
            detail=f"event_count={event_index.get('event_count')} rows={len(index_rows)}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="canonical.jsonl_event_count_matches_index",
            passed=len(canonical_event_ids) == len(index_event_ids),
            detail=f"jsonl={len(canonical_event_ids)} index={len(index_event_ids)}",
        )
    )
    duplicate_index_ids = _duplicates(index_event_ids)
    duplicate_jsonl_ids = _duplicates(canonical_event_ids)
    checks.append(
        DailyRefreshCheck(
            code="canonical.no_duplicate_event_ids",
            passed=not duplicate_index_ids and not duplicate_jsonl_ids,
            detail=(
                f"index_duplicates={duplicate_index_ids[:5]} "
                f"jsonl_duplicates={duplicate_jsonl_ids[:5]}"
            ),
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="canonical.index_matches_jsonl_event_ids",
            passed=set(index_event_ids) == set(canonical_event_ids),
            detail=f"index_only={len(set(index_event_ids)-set(canonical_event_ids))} jsonl_only={len(set(canonical_event_ids)-set(index_event_ids))}",
        )
    )
    surface_counts = Counter(
        str(row.get("surface", ""))
        for row in index_rows
        if isinstance(row, dict) and row.get("surface")
    )
    expected_counts = {
        str(key): int(value)
        for key, value in (event_index.get("surface_counts") or {}).items()
    }
    checks.append(
        DailyRefreshCheck(
            code="canonical.surface_counts_reconcile",
            passed=dict(surface_counts) == expected_counts,
            detail=f"computed={dict(surface_counts)} expected={expected_counts}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="source.verify_ok",
            passed=bool(verify_payload.get("ok")),
            detail=f"ok={verify_payload.get('ok')}",
        )
    )
    failed_error_checks = [
        item
        for item in verify_payload.get("checks", [])
        if isinstance(item, dict)
        and item.get("severity") == "error"
        and item.get("passed") is False
    ]
    checks.append(
        DailyRefreshCheck(
            code="source.verify_no_error_failures",
            passed=not failed_error_checks,
            detail=f"failed_error_checks={len(failed_error_checks)}",
        )
    )
    source_status = {
        str(source.get("provider")): str(source.get("status"))
        for source in context_payload.get("sources", [])
        if isinstance(source, dict)
    }
    bad_sources = {
        provider: status
        for provider, status in source_status.items()
        if status and status != "ok"
    }
    checks.append(
        DailyRefreshCheck(
            code="source.status_all_ok",
            passed=not bad_sources,
            detail=f"statuses={source_status}",
        )
    )
    captured_at = _parse_datetime(context_payload.get("captured_at"))
    checks.append(
        DailyRefreshCheck(
            code="source.bundle_captured_for_as_of",
            passed=(
                source_freshness_policy == "allow-stale-caveats"
                or (captured_at is not None and captured_at.date() >= as_of_date)
            ),
            detail=f"captured_at={context_payload.get('captured_at')} as_of={as_of_date}",
        )
    )
    stale_sources = []
    for source in context_payload.get("sources", []):
        if not isinstance(source, dict):
            continue
        source_captured = _parse_datetime(source.get("captured_at"))
        if source_captured is None or source_captured.date() < as_of_date:
            stale_sources.append(str(source.get("provider") or "unknown"))
    checks.append(
        DailyRefreshCheck(
            code="source.all_connectors_captured_for_as_of",
            passed=(
                source_freshness_policy != "all-sources-current" or not stale_sources
            ),
            detail=f"stale_sources={stale_sources} as_of={as_of_date}",
        )
    )


def _validate_previous_immutability(
    checks: list[DailyRefreshCheck],
    *,
    previous_valid: Path | None,
    current_event_index_path: Path,
    current_event_ids: list[str],
) -> None:
    if previous_valid is None:
        checks.append(
            DailyRefreshCheck(
                code="previous.latest_valid_available",
                passed=False,
                severity="warning",
                detail="No previous validated run; full validation fallback.",
            )
        )
        return
    previous_manifest = _read_json_or_empty(previous_valid / "validation_manifest.json")
    previous_index_path = Path(
        previous_manifest.get("references", {}).get("canonical_event_index", "")
    )
    if not previous_index_path.is_file():
        checks.append(
            DailyRefreshCheck(
                code="previous.canonical_index_available",
                passed=False,
                severity="warning",
                detail=str(previous_index_path),
            )
        )
        return
    previous_event_ids = [
        str(row.get("event_id", ""))
        for row in (_read_json_or_empty(previous_index_path).get("rows", []) or [])
        if isinstance(row, dict) and row.get("event_id")
    ]
    checks.append(
        DailyRefreshCheck(
            code="previous.history_event_ids_preserved",
            passed=set(previous_event_ids).issubset(set(current_event_ids)),
            detail=f"previous={len(previous_event_ids)} current={len(current_event_ids)}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="previous.current_index_hash_recorded",
            passed=current_event_index_path.is_file(),
            severity="warning",
            detail=(
                _sha256_file(current_event_index_path)
                if current_event_index_path.is_file()
                else ""
            ),
        )
    )


def _validate_workflows(
    checks: list[DailyRefreshCheck],
    *,
    workflows_payload: dict[str, Any],
    workflow_manifest: dict[str, Any],
    valid_event_ids: set[str],
) -> None:
    candidates = workflows_payload.get("candidates", [])
    checks.append(
        DailyRefreshCheck(
            code="workflow.candidates_present",
            passed=bool(candidates),
            detail=f"candidate_count={len(candidates)}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="workflow.discovery_queue_has_multiple_candidates",
            passed=len(candidates) >= 2,
            detail=f"candidate_count={len(candidates)}",
        )
    )
    missing_evidence = [
        str(candidate.get("candidate_id", ""))
        for candidate in candidates
        if isinstance(candidate, dict) and not candidate.get("source_event_ids")
    ]
    checks.append(
        DailyRefreshCheck(
            code="workflow.all_candidates_have_evidence_ids",
            passed=not missing_evidence,
            detail=f"missing={missing_evidence[:5]}",
        )
    )
    invalid_refs = _invalid_candidate_event_refs(candidates, valid_event_ids)
    checks.append(
        DailyRefreshCheck(
            code="workflow.evidence_ids_exist",
            passed=not invalid_refs,
            detail=f"invalid_refs={invalid_refs[:5]}",
        )
    )
    generated_by = {
        str(candidate.get("metadata", {}).get("generated_by", ""))
        for candidate in candidates
        if isinstance(candidate, dict)
    }
    checks.append(
        DailyRefreshCheck(
            code="workflow.semantic_primary",
            passed="skillmap_semantic_v1" in generated_by,
            detail=f"generated_by={sorted(generated_by)}",
        )
    )
    unsupported_generators = sorted(
        generator
        for generator in generated_by
        if generator not in _ALLOWED_WORKFLOW_GENERATORS
    )
    checks.append(
        DailyRefreshCheck(
            code="workflow.only_semantic_discovery_sources",
            passed=not unsupported_generators and bool(generated_by),
            detail=f"unsupported_generators={unsupported_generators}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="workflow.semantic_candidate_source_policy",
            passed=workflow_manifest.get("selected_backend") == "semantic",
            detail=f"selected_backend={workflow_manifest.get('selected_backend')}",
        )
    )
    missing_specs = [
        str(candidate.get("candidate_id", ""))
        for candidate in candidates
        if isinstance(candidate, dict)
        and not isinstance(candidate.get("draft_task_spec"), dict)
    ]
    checks.append(
        DailyRefreshCheck(
            code="workflow.all_candidates_have_draft_task_specs",
            passed=not missing_specs,
            detail=f"missing={missing_specs[:5]}",
        )
    )
    below_rubric = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        spec = candidate.get("draft_task_spec")
        level = spec.get("evaluation_level") if isinstance(spec, dict) else ""
        if _evaluation_level_rank(str(level)) < _evaluation_level_rank(
            "rubric_evaluable"
        ):
            below_rubric.append(f"{candidate.get('candidate_id', '')}:{level}")
    checks.append(
        DailyRefreshCheck(
            code="workflow.all_candidates_have_rubric_evaluable_task_specs",
            passed=not below_rubric,
            detail=f"below_rubric={below_rubric[:5]}",
        )
    )
    noisy_titles = [
        str(candidate.get("candidate_id", ""))
        for candidate in candidates
        if isinstance(candidate, dict)
        and _NOISY_WORKFLOW_TITLE_RE.search(str(candidate.get("title", "")).strip())
    ]
    checks.append(
        DailyRefreshCheck(
            code="workflow.workflow_titles_are_operating_patterns",
            passed=not noisy_titles,
            detail=f"noisy_title_candidate_ids={noisy_titles[:5]}",
        )
    )
    raw_sensitive = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for snippet in candidate.get("snippets", []) or []:
            if _contains_raw_sensitive(str(snippet)):
                raw_sensitive.append(str(candidate.get("candidate_id", "")))
                break
    checks.append(
        DailyRefreshCheck(
            code="workflow.ceo_safe_snippets_redacted",
            passed=not raw_sensitive,
            detail=f"raw_sensitive_candidate_ids={raw_sensitive[:5]}",
        )
    )


def _evaluation_level_rank(level: str) -> int:
    return _EVALUATION_LEVEL_ORDER.get(level, -1)


def _validate_skill_map(
    checks: list[DailyRefreshCheck],
    *,
    skill_payload: dict[str, Any],
    valid_event_ids: set[str],
) -> None:
    skills = skill_payload.get("skills", [])
    checks.append(
        DailyRefreshCheck(
            code="skillmap.skills_present",
            passed=bool(skills),
            detail=f"skill_count={len(skills)}",
        )
    )
    accepted = [
        skill
        for skill in skills
        if isinstance(skill, dict)
        and skill.get("status") != "gap"
        and skill.get("candidate_type") != "gap"
    ]
    uncited = [
        str(skill.get("skill_id", ""))
        for skill in accepted
        if not skill.get("evidence_refs")
    ]
    checks.append(
        DailyRefreshCheck(
            code="skillmap.accepted_skills_have_citations",
            passed=not uncited,
            detail=f"uncited={uncited[:5]}",
        )
    )
    invalid_refs: list[str] = []
    for skill in accepted:
        for ref in skill.get("evidence_refs", []) or []:
            if not isinstance(ref, dict) or ref.get("ref_type") != "event":
                continue
            ref_id = str(ref.get("ref_id", ""))
            if ref_id and ref_id not in valid_event_ids:
                invalid_refs.append(f"{skill.get('skill_id')}:{ref_id}")
    checks.append(
        DailyRefreshCheck(
            code="skillmap.cited_events_exist",
            passed=not invalid_refs,
            detail=f"invalid_refs={invalid_refs[:5]}",
        )
    )
    metadata = (
        skill_payload.get("metadata", {}) if isinstance(skill_payload, dict) else {}
    )
    opportunity_meta = metadata.get("world_model_skill_opportunities", {})
    opportunity_gaps = [
        gap
        for gap in skill_payload.get("gaps", []) or []
        if isinstance(gap, dict)
        and gap.get("metadata", {}).get("opportunity_source")
        == "world_model_skill_opportunity_v1"
    ]
    uncited_opportunity_gaps = [
        str(gap.get("gap_id", ""))
        for gap in opportunity_gaps
        if not gap.get("evidence_refs")
    ]
    uncited_upgrades: list[str] = []
    for skill in accepted:
        for opportunity in (
            skill.get("metadata", {}).get("world_model_upgrade_opportunities", []) or []
        ):
            if not isinstance(opportunity, dict):
                continue
            if (
                opportunity.get("opportunity_source")
                != "world_model_skill_opportunity_v1"
            ):
                continue
            if not opportunity.get("supporting_evidence_ids"):
                uncited_upgrades.append(str(skill.get("skill_id", "")))
    checks.append(
        DailyRefreshCheck(
            code="skillmap.world_model_opportunities_are_cited",
            passed=not uncited_opportunity_gaps and not uncited_upgrades,
            detail=(
                f"opportunities={opportunity_meta.get('opportunities_added', 0)} "
                f"uncited_gaps={uncited_opportunity_gaps[:5]} "
                f"uncited_upgrades={uncited_upgrades[:5]}"
            ),
        )
    )
    activations_without_owner = [
        str(skill.get("skill_id", ""))
        for skill in accepted
        if skill.get("deployment_readiness") == "activation_candidate"
        and (
            not str(skill.get("owner", "")).strip()
            or not str(skill.get("reviewer", "")).strip()
        )
    ]
    checks.append(
        DailyRefreshCheck(
            code="skillmap.no_activation_without_owner_reviewer",
            passed=not activations_without_owner,
            detail=f"activation_candidates_missing_owner_or_reviewer={activations_without_owner[:5]}",
        )
    )


def _validate_model_artifacts(
    checks: list[DailyRefreshCheck],
    *,
    dataset_manifest: dict[str, Any],
    train_result: dict[str, Any],
    eval_result: dict[str, Any],
    target_manifest: dict[str, Any],
    target_manifest_path: Path,
    checkpoint_path: Path,
    previous_valid: Path | None,
) -> None:
    checks.append(
        DailyRefreshCheck(
            code="model.checkpoint_exists",
            passed=checkpoint_path.is_file(),
            detail=str(checkpoint_path),
        )
    )
    target_paths = (
        dataset_manifest.get("metadata", {})
        .get("target_layer", {})
        .get("target_manifest_paths", {})
    )
    checks.append(
        DailyRefreshCheck(
            code="model.target_manifest_linked",
            passed=Path(str(target_paths.get(DEFAULT_TENANT_ID, ""))).resolve()
            == target_manifest_path.resolve(),
            detail=f"linked={target_paths.get(DEFAULT_TENANT_ID, '')}",
        )
    )
    unsupported = target_manifest.get("unsupported_heads", [])
    supported = target_manifest.get("supported_heads", [])
    experimental = target_manifest.get("experimental_heads", [])
    checks.append(
        DailyRefreshCheck(
            code="model.supported_heads_present",
            passed=bool(supported),
            detail=f"supported={len(supported)} experimental={len(experimental)} unsupported={len(unsupported)}",
        )
    )
    ranking_policy = (
        dataset_manifest.get("metadata", {})
        .get("target_layer", {})
        .get("ranking_policy", "")
    )
    checks.append(
        DailyRefreshCheck(
            code="model.ranking_policy_excludes_unsupported_proxy_heads",
            passed="supported_structural_and_curated_heads_only" in str(ranking_policy),
            detail=str(ranking_policy),
        )
    )
    train_loss = _float_or_nan(train_result.get("train_loss"))
    validation_loss = _float_or_nan(train_result.get("validation_loss"))
    checks.append(
        DailyRefreshCheck(
            code="model.train_validation_losses_finite",
            passed=math.isfinite(train_loss) and math.isfinite(validation_loss),
            detail=f"train_loss={train_loss} validation_loss={validation_loss}",
        )
    )
    family_mae = eval_result.get("observed_metrics", {}).get("target_family_mae", {})
    curated_semantic_mae = _float_or_nan(family_mae.get("curated_semantic"))
    checks.append(
        DailyRefreshCheck(
            code="model.curated_semantic_eval_present",
            passed=math.isfinite(curated_semantic_mae),
            detail=f"curated_semantic_mae={curated_semantic_mae}",
        )
    )
    if previous_valid is None:
        return
    previous_manifest = _read_json_or_empty(previous_valid / "validation_manifest.json")
    previous_eval_path = Path(
        previous_manifest.get("references", {}).get("eval_result", "")
    )
    if not previous_eval_path.is_file():
        return
    previous_eval = _read_json_or_empty(previous_eval_path)
    previous_family_mae = previous_eval.get("observed_metrics", {}).get(
        "target_family_mae", {}
    )
    previous_curated_mae = _float_or_nan(previous_family_mae.get("curated_semantic"))
    if math.isfinite(previous_curated_mae) and math.isfinite(curated_semantic_mae):
        checks.append(
            DailyRefreshCheck(
                code="model.curated_semantic_mae_not_degraded",
                passed=curated_semantic_mae <= previous_curated_mae * 1.1 + 0.02,
                detail=f"current={curated_semantic_mae} previous={previous_curated_mae}",
            )
        )


def _validate_strategic_run(
    checks: list[DailyRefreshCheck],
    *,
    strategic_rows: list[dict[str, str]],
    strategic_payload: dict[str, Any],
    saturation_guard: dict[str, Any],
) -> None:
    checks.append(
        DailyRefreshCheck(
            code="strategic.rows_present",
            passed=bool(strategic_rows),
            detail=f"row_count={len(strategic_rows)}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="strategic.saturation_guard_passed",
            passed=bool(saturation_guard.get("trusted_for_daily_advice")),
            detail=(
                f"status={saturation_guard.get('status')} "
                f"score_spread={saturation_guard.get('score_spread')} "
                f"reasons={saturation_guard.get('reasons')}"
            ),
        )
    )
    untrusted_rows = [
        row.get("case_id", "")
        for row in strategic_rows
        if str(row.get("saturation_guard_trusted_for_ranking", "")).lower()
        not in {"true", "1", "yes"}
    ]
    checks.append(
        DailyRefreshCheck(
            code="strategic.rows_trusted_for_ranking",
            passed=not untrusted_rows,
            detail=f"untrusted_rows={len(untrusted_rows)}",
        )
    )
    bad_ranking_basis = [
        row.get("case_id", "")
        for row in strategic_rows
        if "supported" not in str(row.get("ranking_basis", "")).lower()
    ]
    checks.append(
        DailyRefreshCheck(
            code="strategic.ranking_uses_supported_heads",
            passed=not bad_ranking_basis,
            detail=f"bad_ranking_basis_rows={len(bad_ranking_basis)}",
        )
    )
    checks.append(
        DailyRefreshCheck(
            code="strategic.result_json_has_guard",
            passed=isinstance(strategic_payload.get("saturation_guard"), dict),
            detail=f"keys={sorted(strategic_payload.keys())}",
        )
    )


def _read_json_or_empty(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.is_file():
        return {}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_jsonl_event_ids(path: Path) -> list[str]:
    if not path.is_file():
        return []
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict) and payload.get("event_id"):
            ids.append(str(payload["event_id"]))
    return ids


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _source_watermarks(
    context_payload: dict[str, Any], event_index: dict[str, Any]
) -> dict[str, Any]:
    providers: dict[str, dict[str, Any]] = {}
    for source in context_payload.get("sources", []) or []:
        if not isinstance(source, dict):
            continue
        provider = str(source.get("provider") or "unknown")
        providers[provider] = {
            "captured_at": source.get("captured_at", ""),
            "status": source.get("status", ""),
            "record_counts": source.get("record_counts", {}),
        }
    for row in event_index.get("rows", []) or []:
        if not isinstance(row, dict):
            continue
        provider = str(row.get("provider") or row.get("surface") or "unknown")
        bucket = providers.setdefault(provider, {})
        timestamp = str(row.get("timestamp") or "")
        if timestamp:
            current = str(bucket.get("max_event_timestamp") or "")
            if not current or timestamp > current:
                bucket["max_event_timestamp"] = timestamp
        bucket["canonical_event_count"] = (
            int(bucket.get("canonical_event_count") or 0) + 1
        )
    return {
        "bundle_captured_at": context_payload.get("captured_at", ""),
        "providers": providers,
    }


def _invalid_candidate_event_refs(
    candidates: list[Any], valid_event_ids: set[str]
) -> list[str]:
    invalid: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("candidate_id", ""))
        for event_id in candidate.get("source_event_ids", []) or []:
            if str(event_id) not in valid_event_ids:
                invalid.append(f"{candidate_id}:{event_id}")
    return invalid


def _contains_raw_sensitive(value: str) -> bool:
    if "[REDACTED_" in value:
        return False
    return any(pattern.search(value) for pattern in _RAW_SECRET_PATTERNS)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.isdigit():
        timestamp_ms = int(text)
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duplicates(values: list[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if count > 1)


def _float_or_nan(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip().lower())
    return normalized.strip("_") or "pyinsights_daily"


def _render_validation_failure(
    as_of_date: date,
    checks: list[DailyRefreshCheck],
    references: dict[str, str],
) -> str:
    failed = [check for check in checks if not check.passed]
    lines = [
        f"# Py Insights Daily Refresh Validation Failure - {as_of_date.isoformat()}",
        "",
        "No CEO report was emitted because validation did not pass.",
        "",
        "## Failed Checks",
        "",
    ]
    for check in failed:
        lines.append(f"- `{check.code}` ({check.severity}): {check.detail}")
    lines.extend(["", "## Key References", ""])
    for key, value in references.items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines) + "\n"


def _render_workflow_skill_summary(
    *,
    as_of_date: date,
    workflows_payload: dict[str, Any],
    skill_payload: dict[str, Any],
    workflow_manifest: dict[str, Any],
) -> str:
    candidates = workflows_payload.get("candidates", []) or []
    skills = skill_payload.get("skills", []) or []
    task_spec_count = _task_spec_count(candidates)
    rubric_or_better_count = _candidate_count_at_or_above(
        candidates, "rubric_evaluable"
    )
    contract_or_better_count = _candidate_count_at_or_above(
        candidates, "contract_evaluable"
    )
    rl_packaged_count = _candidate_count_at_or_above(candidates, "rl_packaged")
    opportunity_meta = (
        skill_payload.get("metadata", {}).get("world_model_skill_opportunities", {})
        if isinstance(skill_payload, dict)
        else {}
    )
    opportunity_gaps = [
        gap
        for gap in skill_payload.get("gaps", []) or []
        if isinstance(gap, dict)
        and gap.get("metadata", {}).get("opportunity_source")
        == "world_model_skill_opportunity_v1"
    ]
    upgrade_skills = [
        skill
        for skill in skills
        if isinstance(skill, dict)
        and skill.get("metadata", {}).get("world_model_upgrade_opportunities")
    ]
    lines = [
        f"# Py Insights Workflow Discovery Daily Summary - {as_of_date.isoformat()}",
        "",
        "Primary artifact: broad workflow discovery. Task specs and package "
        "readiness are downstream per-candidate review states, not a single "
        "handpicked workflow.",
        "",
        f"- Published workflow candidates: `{len(candidates)}`",
        f"- Candidates with draft task specs: `{task_spec_count}`",
        f"- Rubric-evaluable task specs: `{rubric_or_better_count}`",
        f"- Contract-evaluable specs: `{contract_or_better_count}`",
        f"- RL-package-ready specs: `{rl_packaged_count}`",
        f"- Skill count: `{len(skills)}`",
        f"- World-model skill opportunities: `{opportunity_meta.get('opportunities_added', 0)}`",
        f"- Missing-skill opportunities: `{opportunity_meta.get('missing_skill_gap_count', 0)}`",
        f"- Skill-upgrade opportunities: `{opportunity_meta.get('skill_upgrade_count', 0)}`",
        f"- Untrusted strategic rows skipped: `{opportunity_meta.get('rows_skipped_untrusted', 0)}`",
        f"- Semantic candidate count: `{workflow_manifest.get('semantic_candidate_count', '')}`",
        f"- Workflow candidate source: `{workflow_manifest.get('selected_backend', '')}`",
        "",
        "## Workflow Queue",
        "",
    ]
    for index, candidate in enumerate(candidates[:25], start=1):
        if not isinstance(candidate, dict):
            continue
        spec = candidate.get("draft_task_spec", {})
        level = spec.get("evaluation_level", "") if isinstance(spec, dict) else ""
        event_count = len(candidate.get("source_event_ids", []) or [])
        case_count = len(candidate.get("source_case_ids", []) or [])
        lines.append(
            f"{index}. {candidate.get('title', '')} "
            f"(score={candidate.get('rank_score', '')}, "
            f"level={level or 'missing'}, "
            f"events={event_count}, "
            f"cases={case_count}, "
            f"source={candidate.get('metadata', {}).get('generated_by', '')}, "
            f"readiness={candidate.get('metadata', {}).get('deployment_readiness', '')})"
        )
    lines.extend(
        [
            "",
            "## Package Readiness",
            "",
            "- `rubric_evaluable`: reviewable task spec exists for the discovered workflow.",
            "- `contract_evaluable`: deterministic success/failure predicates are attached.",
            "- `rl_packaged`: reviewed contract can be exported with `vei workflow package-env`.",
            "- Current daily discovery should normally produce many rubric-evaluable specs and zero or few package-ready specs until a human promotes candidates.",
        ]
    )
    if opportunity_gaps or upgrade_skills:
        lines.extend(["", "## World-Model Skill Opportunities", ""])
        for gap in opportunity_gaps[:10]:
            lines.append(
                f"- Missing: {gap.get('title', '')} "
                f"(evidence={len(gap.get('evidence_refs', []) or [])})"
            )
        for skill in upgrade_skills[:10]:
            lines.append(
                f"- Upgrade: {skill.get('title', '')} "
                f"(opportunities={skill.get('metadata', {}).get('world_model_upgrade_opportunity_count', '')})"
            )
    return "\n".join(lines) + "\n"


def _task_spec_count(candidates: list[Any]) -> int:
    return sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict)
        and isinstance(candidate.get("draft_task_spec"), dict)
    )


def _candidate_count_at_or_above(candidates: list[Any], level: str) -> int:
    required = _evaluation_level_rank(level)
    return sum(
        1
        for candidate in candidates
        if isinstance(candidate, dict)
        and isinstance(candidate.get("draft_task_spec"), dict)
        and _evaluation_level_rank(
            str(candidate.get("draft_task_spec", {}).get("evaluation_level", ""))
        )
        >= required
    )


def _render_minimal_validated_report(
    as_of_date: date, workflows_payload: dict[str, Any]
) -> str:
    return (
        f"# Py Insights CEO Counterfactual Report - {as_of_date.isoformat()}\n\n"
        "Validated daily refresh passed. No pre-rendered strategic report was found, "
        "so this placeholder records validation status only.\n\n"
        f"Workflow candidates: `{len(workflows_payload.get('candidates', []) or [])}`\n"
    )
