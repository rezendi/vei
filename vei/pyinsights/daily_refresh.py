from __future__ import annotations

import csv
import json
import math
import re
import shutil
from collections import Counter
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from vei.skillmap.api import (
    CompanySkillMap,
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

    context_path = Path(context_bundle).expanduser().resolve()
    context_root = context_path.parent
    event_index_path = context_root / "canonical_event_index.json"
    canonical_events_path = context_root / "canonical_events.jsonl"
    verify_path = _latest_context_verify(context_root)
    skill_path = (
        Path(skill_map_path).expanduser().resolve()
        if skill_map_path is not None
        else context_root / "skill_map" / "company_skill_map.json"
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

    checks: list[DailyRefreshCheck] = []
    notes: list[str] = []
    if previous_valid is None:
        notes.append(
            "No previous validated daily run was found; using full validation fallback."
        )
    elif previous_valid == run_root:
        notes.append(
            "Previous run resolved to current run root; ignoring it for comparison."
        )
        previous_valid = None

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

    if refresh_workflows and skill_path.is_file() and strategic_csv_path.is_file():
        try:
            base_skill_map = CompanySkillMap.model_validate_json(
                skill_path.read_text(encoding="utf-8")
            )
            enriched_skill_map = enrich_skill_map_with_world_model_opportunities(
                base_skill_map,
                context_path=context_path,
                world_model_report_path=strategic_csv_path,
                max_opportunities=8,
                require_trusted_ranking=True,
            )
            skill_outputs = write_company_skill_map_outputs(
                enriched_skill_map, skill_output_root
            )
            effective_skill_path = skill_outputs["json"]
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.world_model_opportunity_refresh_ran",
                    passed=True,
                    detail=str(effective_skill_path),
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(
                DailyRefreshCheck(
                    code="skillmap.world_model_opportunity_refresh_ran",
                    passed=False,
                    detail=str(exc),
                )
            )
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
    _validate_workflows(
        checks,
        workflows_payload=workflows_payload,
        workflow_manifest=_read_json_or_empty(workflow_manifest_path),
        valid_event_ids=set(index_event_ids),
    )
    _validate_skill_map(
        checks,
        skill_payload=skill_payload,
        valid_event_ids=set(index_event_ids),
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
    checks.append(
        DailyRefreshCheck(
            code="workflow.structural_clusters_diagnostic",
            passed=bool(
                workflow_manifest.get("raw_structural_clusters_are_diagnostics")
            ),
            detail=f"manifest={workflow_manifest.get('raw_structural_clusters_are_diagnostics')}",
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
        f"# Py Insights Workflow/Skill Daily Summary - {as_of_date.isoformat()}",
        "",
        f"- Published workflow candidates: `{len(candidates)}`",
        f"- Skill count: `{len(skills)}`",
        f"- World-model skill opportunities: `{opportunity_meta.get('opportunities_added', 0)}`",
        f"- Missing-skill opportunities: `{opportunity_meta.get('missing_skill_gap_count', 0)}`",
        f"- Skill-upgrade opportunities: `{opportunity_meta.get('skill_upgrade_count', 0)}`",
        f"- Untrusted strategic rows skipped: `{opportunity_meta.get('rows_skipped_untrusted', 0)}`",
        f"- Semantic candidate count: `{workflow_manifest.get('semantic_candidate_count', '')}`",
        f"- Structural diagnostic cluster count: `{workflow_manifest.get('structural_candidate_count', '')}`",
        "",
        "## Workflow Queue",
        "",
    ]
    for index, candidate in enumerate(candidates[:25], start=1):
        if not isinstance(candidate, dict):
            continue
        lines.append(
            f"{index}. {candidate.get('title', '')} "
            f"(score={candidate.get('rank_score', '')}, "
            f"readiness={candidate.get('metadata', {}).get('deployment_readiness', '')})"
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


def _render_minimal_validated_report(
    as_of_date: date, workflows_payload: dict[str, Any]
) -> str:
    return (
        f"# Py Insights CEO Counterfactual Report - {as_of_date.isoformat()}\n\n"
        "Validated daily refresh passed. No pre-rendered strategic report was found, "
        "so this placeholder records validation status only.\n\n"
        f"Workflow candidates: `{len(workflows_payload.get('candidates', []) or [])}`\n"
    )
