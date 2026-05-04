from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.whatif.api import (
    WhatIfAuditRecord,
    WhatIfJudgedPairwiseComparison,
    load_branch_point_benchmark_build_result,
    load_branch_point_benchmark_judge_result,
)

from ._api_models import AuditSubmitRequest
from ._workspace_route_context import WorkspaceRouteContext


def register_workspace_whatif_audit_routes(
    app: FastAPI,
    ctx: WorkspaceRouteContext,
) -> None:
    @app.get("/api/workspace/whatif/audit")
    def api_audit_queue() -> JSONResponse:
        try:
            benchmark_root = _resolve_benchmark_root(ctx.root)
        except HTTPException as exc:
            if exc.status_code == 404:
                return JSONResponse({"items": [], "total": 0})
            raise
        judge_result = load_branch_point_benchmark_judge_result(benchmark_root)
        build = load_branch_point_benchmark_build_result(benchmark_root)
        completed = _load_completed_audits(benchmark_root)
        completed_keys = {
            (record.case_id, record.objective_pack_id)
            for record in completed
            if record.status == "completed"
        }
        case_by_id = {case.case_id: case for case in build.cases}
        items = []
        for audit in judge_result.audit_queue:
            key = (audit.case_id, audit.objective_pack_id)
            case = case_by_id.get(audit.case_id)
            if case is None:
                continue
            dossier_path = case.objective_dossier_paths.get(audit.objective_pack_id)
            dossier_text = ""
            if dossier_path and Path(dossier_path).exists():
                dossier_text = Path(dossier_path).read_text(encoding="utf-8")
            judge_ranking = None
            for judgment in judge_result.judgments:
                if (
                    judgment.case_id == audit.case_id
                    and judgment.objective_pack_id == audit.objective_pack_id
                ):
                    judge_ranking = judgment
                    break
            items.append(
                {
                    "case_id": audit.case_id,
                    "objective_pack_id": audit.objective_pack_id,
                    "status": "completed" if key in completed_keys else "pending",
                    "case_title": case.title,
                    "case_summary": case.summary,
                    "dossier_text": dossier_text,
                    "candidates": [
                        {
                            "candidate_id": c.candidate_id,
                            "label": c.label,
                            "prompt": c.prompt,
                        }
                        for c in case.candidates
                    ],
                    "judge_confidence": (
                        judge_ranking.confidence if judge_ranking else None
                    ),
                    "judge_uncertainty_flag": (
                        judge_ranking.uncertainty_flag if judge_ranking else False
                    ),
                }
            )
        return JSONResponse({"items": items, "total": len(items)})

    @app.post("/api/workspace/whatif/audit/{case_id}/{objective_pack_id}")
    def api_audit_submit(
        case_id: str,
        objective_pack_id: str,
        request: AuditSubmitRequest,
    ) -> JSONResponse:
        benchmark_root = _resolve_benchmark_root(ctx.root)
        build = load_branch_point_benchmark_build_result(benchmark_root)
        judge_result = load_branch_point_benchmark_judge_result(benchmark_root)
        case_by_id = {case.case_id: case for case in build.cases}
        case = case_by_id.get(case_id)
        if case is None:
            raise HTTPException(status_code=404, detail="benchmark case not found")
        candidate_ids = [candidate.candidate_id for candidate in case.candidates]
        candidate_id_set = set(candidate_ids)
        if (
            len(request.ordered_candidate_ids) != len(candidate_ids)
            or set(request.ordered_candidate_ids) != candidate_id_set
        ):
            raise HTTPException(
                status_code=400,
                detail="ordered_candidate_ids must contain each candidate exactly once",
            )

        judge_ranking = None
        for judgment in judge_result.judgments:
            if (
                judgment.case_id == case_id
                and judgment.objective_pack_id == objective_pack_id
            ):
                judge_ranking = judgment
                break
        if judge_ranking is None:
            raise HTTPException(
                status_code=404,
                detail="judge ranking not found for case/objective",
            )

        expected_pairs = {
            tuple(sorted((left_id, right_id)))
            for index, left_id in enumerate(candidate_ids)
            for right_id in candidate_ids[index + 1 :]
        }
        seen_pairs: set[tuple[str, str]] = set()
        for comparison in request.pairwise_comparisons:
            pair_key = tuple(
                sorted((comparison.left_candidate_id, comparison.right_candidate_id))
            )
            if pair_key in seen_pairs:
                raise HTTPException(
                    status_code=400,
                    detail="pairwise_comparisons contains duplicate pairs",
                )
            seen_pairs.add(pair_key)
            pair_ids = {
                comparison.left_candidate_id,
                comparison.right_candidate_id,
            }
            if not pair_ids.issubset(candidate_id_set):
                raise HTTPException(
                    status_code=400,
                    detail="pairwise_comparisons contains unknown candidate ids",
                )
            if comparison.preferred_candidate_id not in pair_ids:
                raise HTTPException(
                    status_code=400,
                    detail="preferred_candidate_id must match one side of the pair",
                )
        if seen_pairs != expected_pairs:
            raise HTTPException(
                status_code=400,
                detail="pairwise_comparisons must cover every candidate pair once",
            )

        agreement = list(request.ordered_candidate_ids) == list(
            judge_ranking.ordered_candidate_ids
        )
        record = WhatIfAuditRecord(
            case_id=case_id,
            objective_pack_id=objective_pack_id,
            submission_id=uuid4().hex,
            submitted_at=ctx.iso_now(),
            reviewer_id=request.reviewer_id,
            ordered_candidate_ids=request.ordered_candidate_ids,
            pairwise_comparisons=[
                WhatIfJudgedPairwiseComparison.model_validate(
                    comp.model_dump(mode="json")
                )
                for comp in request.pairwise_comparisons
            ],
            confidence=request.confidence,
            status="completed",
            agreement_with_judge=agreement,
            notes=request.notes,
        )

        completed = _load_completed_audits(benchmark_root)
        completed.append(record)
        _save_completed_audits(benchmark_root, completed)

        reveal: dict[str, Any] = {
            "submitted": record.model_dump(mode="json"),
            "agreement_with_judge": agreement,
        }
        if judge_ranking is not None:
            reveal["judge_ranking"] = {
                "ordered_candidate_ids": judge_ranking.ordered_candidate_ids,
                "pairwise_comparisons": [
                    comp.model_dump(mode="json")
                    for comp in judge_ranking.pairwise_comparisons
                ],
                "confidence": judge_ranking.confidence,
                "notes": judge_ranking.notes,
            }
        return JSONResponse(reveal)


def _resolve_benchmark_root(root: Path) -> Path:
    import os

    env_root = os.environ.get("VEI_BENCHMARK_ROOT", "").strip()
    if env_root:
        candidate = Path(env_root).expanduser().resolve()
    else:
        candidate = root
    if not (candidate / "judge_result.json").exists():
        raise HTTPException(
            status_code=404,
            detail="No judge_result.json found. Run `vei whatif benchmark judge` first.",
        )
    return candidate


def _completed_audit_path(benchmark_root: Path) -> Path:
    return benchmark_root / "completed_audit_records.json"


def _load_completed_audits(benchmark_root: Path) -> list[WhatIfAuditRecord]:
    path = _completed_audit_path(benchmark_root)
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [WhatIfAuditRecord.model_validate(item) for item in payload]


def _save_completed_audits(
    benchmark_root: Path,
    records: list[WhatIfAuditRecord],
) -> None:
    path = _completed_audit_path(benchmark_root)
    path.write_text(
        json.dumps(
            [record.model_dump(mode="json") for record in records],
            indent=2,
        ),
        encoding="utf-8",
    )


__all__ = ["register_workspace_whatif_audit_routes"]
