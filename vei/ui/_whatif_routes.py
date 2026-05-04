from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.project_settings import default_interactive_model_for_provider
from vei.whatif.api import (
    WhatIfCandidateIntervention,
    build_decision_scene,
    build_saved_decision_scene,
    build_saved_ranked_result_payload,
    default_forecast_backend,
    list_objective_packs,
    materialize_episode,
    resolve_whatif_source_path,
    run_counterfactual_experiment,
    run_ranked_counterfactual_experiment,
    search_enron_rosetta_events,
    search_events,
)
from vei.whatif.filenames import EXPERIMENT_RESULT_FILE

from ._api_models import (
    WhatIfOpenRequest,
    WhatIfRankRequest,
    WhatIfRunRequest,
    WhatIfSceneRequest,
    WhatIfSearchRequest,
)
from ._whatif_audit_routes import register_workspace_whatif_audit_routes
from ._whatif_chat_routes import register_workspace_whatif_chat_route
from ._whatif_helpers import (
    can_use_saved_bundle,
    load_historical_summary_or_400,
    saved_historical_request_matches,
    saved_workspace_validation_issues,
    saved_workspace_source_matches_request,
)
from ._workspace_route_context import WorkspaceRouteContext

WhatIfSourceId = Literal["auto", "enron", "mail_archive", "company_history"]


def _source_label(source: str | None) -> str:
    labels = {
        "company_history": "Company history bundle",
        "mail_archive": "Historical mail archive",
        "enron": "Full Enron archive",
        "auto": "Historical archive",
    }
    return labels.get(str(source or "auto"), "Historical archive")


def _same_path(left: Path | str | None, right: Path | str | None) -> bool:
    if left is None or right is None:
        return False
    return Path(left).expanduser().resolve(strict=False) == Path(
        right
    ).expanduser().resolve(strict=False)


def _live_source_matches_saved_bundle(
    ctx: WorkspaceRouteContext,
    source_dir: Path | None,
) -> bool:
    saved_bundle = ctx.saved_bundle()
    if saved_bundle is not None and _same_path(
        source_dir, saved_bundle.source_dir_text()
    ):
        return True
    if (ctx.root / "episode_manifest.json").exists():
        return _same_path(source_dir, ctx.root) or _same_path(
            source_dir,
            ctx.root / "context_snapshot.json",
        )
    return False


def _resolve_live_whatif_source_path_or_400(
    ctx: WorkspaceRouteContext,
    source: str,
) -> tuple[str, Path]:
    if ctx.saved_bundle() is not None:
        load_historical_summary_or_400(ctx.root)
    resolved = resolve_whatif_source_path(ctx.root, requested_source=source)
    if resolved is None:
        raise HTTPException(
            status_code=404,
            detail="live historical source is not configured for this workspace",
        )
    resolved_source, source_dir = resolved
    if _live_source_matches_saved_bundle(ctx, source_dir):
        raise HTTPException(
            status_code=404,
            detail=(
                "live historical source is not configured; use saved reference mode "
                "for this workspace"
            ),
        )
    return resolved_source, source_dir


def _resolve_live_whatif_source_or_400(
    ctx: WorkspaceRouteContext,
    source: str,
    *,
    max_events: int | None = None,
):
    resolved_source, source_dir = _resolve_live_whatif_source_path_or_400(
        ctx,
        source,
    )
    try:
        world = ctx.load_live_world(
            resolved_source,
            source_dir,
            max_events=max_events,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return world, source_dir


def _saved_bundle_or_400(
    ctx: WorkspaceRouteContext,
    *,
    source: str,
    event_id: str | None = None,
    thread_id: str | None = None,
):
    if not can_use_saved_bundle(
        ctx.root,
        requested_mode="saved",
        requested_source=source,
        event_id=event_id,
        thread_id=thread_id,
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "saved reference mode is available only for a valid saved "
                "workspace and its recorded branch point"
            ),
        )
    saved_bundle = ctx.saved_bundle()
    if saved_bundle is None:
        raise HTTPException(
            status_code=400,
            detail="saved reference bundle is not available for this workspace",
        )
    return saved_bundle


def _saved_historical_or_400(
    ctx: WorkspaceRouteContext,
    *,
    source: str,
    event_id: str | None = None,
    thread_id: str | None = None,
):
    historical = saved_historical_request_matches(
        ctx.root,
        event_id=event_id,
        thread_id=thread_id,
    )
    if historical is None:
        raise HTTPException(
            status_code=400,
            detail="saved reference request does not match the recorded branch point",
        )
    historical_source = str(historical.source or "").strip().lower()
    if not saved_workspace_source_matches_request(
        ctx.root,
        requested_source=source,
        historical_source=historical_source,
    ):
        raise HTTPException(
            status_code=400,
            detail="saved reference source does not match the recorded branch source",
        )
    return historical


def _whatif_status_payload(ctx: WorkspaceRouteContext) -> dict[str, Any]:
    if ctx.is_public_history_workspace():
        return {
            "available": False,
            "mode": "live",
            "source": "auto",
            "display": {
                "label": "Historical archive unavailable",
                "detail": "This workspace uses the public-history demo surface.",
                "mode_label": "Unavailable",
            },
            "capabilities": {
                "live": False,
                "saved": False,
                "timeline": False,
                "llm": False,
            },
            "defaults": {"provider": None, "model": None},
            "debug": {
                "source_dir": None,
                "available_providers": [],
                "validation_issues": [],
                "timeline_readiness": None,
            },
            "objective_packs": [],
        }

    from vei.context.api import (
        build_canonical_history_readiness,
        canonical_history_sidecars_exist,
    )

    resolved_live = resolve_whatif_source_path(ctx.root)
    live_source = resolved_live[0] if resolved_live is not None else None
    live_source_dir = resolved_live[1] if resolved_live is not None else None
    live_source_is_saved = _live_source_matches_saved_bundle(ctx, live_source_dir)
    if live_source_is_saved:
        live_source = None
        live_source_dir = None

    saved_available = False
    validation_issues: list[str] = []
    saved_source = "auto"
    if ctx.saved_bundle() is not None or (
        (ctx.root / "episode_manifest.json").exists()
        and (resolved_live is None or live_source_is_saved)
    ):
        if ctx.saved_bundle() is not None:
            validation_issues = saved_workspace_validation_issues(ctx.root)
        historical = load_historical_summary_or_400(ctx.root)
        saved_available = historical is not None
        if historical is not None:
            saved_source = str(historical.source or "auto")

    timeline_readiness = None
    timeline_available = False
    if live_source == "company_history" and live_source_dir is not None:
        timeline_available = canonical_history_sidecars_exist(live_source_dir)
        timeline_readiness = build_canonical_history_readiness(
            live_source_dir
        ).model_dump(mode="json")

    available_providers = ctx.available_providers()
    default_provider = ctx.default_provider()
    live_available = resolved_live is not None and not live_source_is_saved
    mode = "live" if live_available else "saved" if saved_available else "live"
    source = (
        live_source if live_available else saved_source if saved_available else "auto"
    )
    source_dir = str(live_source_dir) if live_source_dir is not None else None
    world_cache_status = None
    if live_available and live_source is not None and live_source_dir is not None:
        world_cache_status = ctx.warm_live_world(live_source, live_source_dir)
    display_label = (
        (
            "Live archive warming"
            if world_cache_status and world_cache_status.get("state") == "warming"
            else (
                "Live archive needs attention"
                if world_cache_status and world_cache_status.get("state") == "error"
                else "Live archive ready"
            )
        )
        if live_available
        else (
            "Saved reference ready"
            if saved_available
            else "Historical archive unavailable"
        )
    )
    display_detail = (
        (
            f"{_source_label(live_source)}. First search may take a moment."
            if world_cache_status and world_cache_status.get("state") == "warming"
            else _source_label(live_source)
        )
        if live_available
        else (
            "Saved reference bundle"
            if saved_available
            else (
                "Set VEI_WHATIF_SOURCE_DIR to a company history bundle or mail archive, "
                "or set VEI_WHATIF_ROSETTA_DIR for the Enron source."
            )
        )
    )
    return {
        "available": live_available or saved_available,
        "mode": mode,
        "source": source,
        "display": {
            "label": display_label,
            "detail": display_detail,
            "mode_label": "Live archive" if mode == "live" else "Saved reference",
        },
        "capabilities": {
            "live": live_available,
            "saved": saved_available,
            "timeline": timeline_available,
            "llm": bool(available_providers),
        },
        "defaults": {
            "provider": default_provider,
            "model": (
                default_interactive_model_for_provider(default_provider)
                if default_provider
                else None
            ),
        },
        "debug": {
            "source_dir": source_dir,
            "saved_source_dir": (
                ctx.saved_bundle().source_dir_text()
                if ctx.saved_bundle() is not None
                else None
            ),
            "available_providers": available_providers,
            "validation_issues": validation_issues,
            "timeline_readiness": timeline_readiness,
            "world_cache": world_cache_status,
        },
        "objective_packs": [
            pack.model_dump(mode="json") for pack in list_objective_packs()
        ],
    }


def register_workspace_whatif_routes(
    app: FastAPI,
    ctx: WorkspaceRouteContext,
) -> None:
    register_workspace_whatif_chat_route(app, ctx)
    register_workspace_whatif_audit_routes(app, ctx)

    @app.get("/api/workspace/whatif")
    def api_workspace_whatif_status() -> JSONResponse:
        return JSONResponse(_whatif_status_payload(ctx))

    @app.get("/api/workspace/whatif/timeline")
    def api_workspace_whatif_timeline(
        mode: Literal["live", "saved"] = "live",
        source: WhatIfSourceId = "auto",
        surface: str | None = None,
        actor: str | None = None,
        case_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        confidence_min: float | None = None,
        limit: int = 40,
    ) -> JSONResponse:
        from vei.context.api import query_canonical_history

        if mode == "saved":
            return JSONResponse({"available": False, "mode": "saved", "rows": []})
        resolved = resolve_whatif_source_path(
            ctx.root,
            requested_source=source,
        )
        if resolved is None:
            return JSONResponse({"available": False, "mode": "live", "rows": []})
        source_name, source_path = resolved
        if source_name != "company_history":
            return JSONResponse(
                {
                    "available": False,
                    "mode": "live",
                    "source": source_name,
                    "rows": [],
                }
            )
        timeline = query_canonical_history(
            source_path,
            surface=surface,
            actor=actor,
            case_id=case_id,
            start=start,
            end=end,
            confidence_min=confidence_min,
            limit=limit,
        )
        payload = timeline.model_dump(mode="json")
        payload["mode"] = "live"
        payload["source"] = source_name
        payload["source_dir"] = str(source_path)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/search")
    def api_workspace_whatif_search(request: WhatIfSearchRequest) -> JSONResponse:
        if request.mode == "saved":
            return JSONResponse(
                {
                    "mode": "saved",
                    "source": request.source,
                    "match_count": 0,
                    "matches": [],
                    "notice": "Search is available in live archive mode.",
                }
            )
        resolved_source, source_dir = _resolve_live_whatif_source_path_or_400(
            ctx,
            request.source,
        )
        if resolved_source == "enron":
            try:
                result = search_enron_rosetta_events(
                    rosetta_dir=source_dir,
                    actor=request.actor,
                    participant=request.participant,
                    thread_id=request.thread_id,
                    event_type=request.event_type,
                    query=request.query,
                    flagged_only=request.flagged_only,
                    limit=request.limit,
                    max_events=request.max_events,
                )
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        else:
            world, _ = _resolve_live_whatif_source_or_400(
                ctx,
                request.source,
                max_events=request.max_events,
            )
            result = search_events(
                world,
                actor=request.actor,
                participant=request.participant,
                thread_id=request.thread_id,
                event_type=request.event_type,
                query=request.query,
                flagged_only=request.flagged_only,
                limit=request.limit,
            )
        payload = result.model_dump(mode="json")
        payload["mode"] = "live"
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/open")
    def api_workspace_whatif_open(request: WhatIfOpenRequest) -> JSONResponse:
        if request.mode == "saved":
            saved_bundle = _saved_bundle_or_400(
                ctx,
                source=request.source,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
            historical = _saved_historical_or_400(
                ctx,
                source=request.source,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
            experiment_payload = saved_bundle.load_json(EXPERIMENT_RESULT_FILE)
            if isinstance(experiment_payload, dict):
                materialization = experiment_payload.get("materialization")
                if isinstance(materialization, dict):
                    return JSONResponse(
                        {
                            "mode": "saved",
                            "source": historical.source,
                            "source_dir": saved_bundle.source_dir_text(),
                            "episode_root": str(ctx.root),
                            "materialization": materialization,
                        }
                    )
            raise HTTPException(
                status_code=400,
                detail="saved reference bundle does not contain materialization",
            )

        world, source_dir = _resolve_live_whatif_source_or_400(
            ctx,
            request.source,
            max_events=request.max_events,
        )
        label = request.label or request.event_id or request.thread_id or "episode"
        episode_root = ctx.whatif_artifacts_root() / "episodes" / ctx.slug(label)
        try:
            materialization = materialize_episode(
                world,
                root=episode_root,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "mode": "live",
                "source": world.source,
                "source_dir": str(source_dir),
                "episode_root": str(episode_root),
                "materialization": materialization.model_dump(mode="json"),
            }
        )

    @app.post("/api/workspace/whatif/scene")
    def api_workspace_whatif_scene(request: WhatIfSceneRequest) -> JSONResponse:
        if request.mode == "saved":
            _saved_historical_or_400(
                ctx,
                source=request.source,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
            try:
                scene = build_saved_decision_scene(ctx.root)
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            payload = scene.model_dump(mode="json")
            payload["mode"] = "saved"
            return JSONResponse(payload)

        world, source_dir = _resolve_live_whatif_source_or_400(
            ctx,
            request.source,
            max_events=request.max_events,
        )
        try:
            scene = build_decision_scene(
                world,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = scene.model_dump(mode="json")
        payload["mode"] = "live"
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/run")
    def api_workspace_whatif_run(request: WhatIfRunRequest) -> JSONResponse:
        if request.mode == "saved":
            saved_bundle = _saved_bundle_or_400(
                ctx,
                source=request.source,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
            experiment_payload = saved_bundle.load_json(EXPERIMENT_RESULT_FILE)
            if isinstance(experiment_payload, dict):
                saved_payload = dict(experiment_payload)
                saved_payload["mode"] = "saved"
                saved_payload["source_dir"] = saved_bundle.source_dir_text()
                saved_payload["saved_result"] = True
                saved_payload["saved_bundle_notice"] = (
                    "Showing the saved reference result for this workspace. "
                    "Studio ignores custom prompt, label, provider, and forecast settings here."
                )
                return JSONResponse(saved_payload)
            raise HTTPException(
                status_code=400,
                detail="saved reference bundle does not contain a run result",
            )

        world, source_dir = _resolve_live_whatif_source_or_400(
            ctx,
            request.source,
            max_events=request.max_events,
        )
        effective_mode = request.experiment_mode
        effective_provider = request.provider
        effective_model = request.model
        if effective_mode in {"llm", "both"}:
            resolved_provider, resolved_model = ctx.resolve_llm_provider(
                request.provider,
                request.model,
            )
            if resolved_provider is None:
                effective_mode = "heuristic_baseline"
            else:
                effective_provider = resolved_provider
                effective_model = resolved_model
        try:
            result = run_counterfactual_experiment(
                world,
                artifacts_root=ctx.whatif_artifacts_root() / "experiments",
                label=request.label,
                counterfactual_prompt=request.prompt,
                event_id=request.event_id,
                thread_id=request.thread_id,
                mode=effective_mode,
                provider=effective_provider,
                model=effective_model,
                ejepa_epochs=request.ejepa_epochs,
                ejepa_batch_size=request.ejepa_batch_size,
                ejepa_force_retrain=request.ejepa_force_retrain,
                ejepa_device=request.ejepa_device,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = result.model_dump(mode="json")
        payload["mode"] = "live"
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)

    @app.post("/api/workspace/whatif/rank")
    def api_workspace_whatif_rank(request: WhatIfRankRequest) -> JSONResponse:
        if not request.candidates:
            raise HTTPException(
                status_code=400,
                detail="at least one candidate is required",
            )
        if request.mode == "saved":
            saved_bundle = _saved_bundle_or_400(
                ctx,
                source=request.source,
                event_id=request.event_id,
                thread_id=request.thread_id,
            )
            payload = build_saved_ranked_result_payload(
                saved_bundle,
                objective_pack_id=request.objective_pack_id,
            )
            if payload is not None:
                payload["mode"] = "saved"
                payload["source_dir"] = saved_bundle.source_dir_text()
                payload["saved_result"] = True
                payload["saved_bundle_notice"] = (
                    "Showing the saved reference ranking for this workspace. "
                    "Studio ignores custom label, candidates, provider, and forecast settings here."
                )
                return JSONResponse(payload)
            raise HTTPException(
                status_code=400,
                detail="saved reference bundle does not contain a ranked result",
            )

        world, source_dir = _resolve_live_whatif_source_or_400(
            ctx,
            request.source,
            max_events=request.max_events,
        )
        normalized_shadow_backend = request.shadow_forecast_backend.strip().lower()
        if normalized_shadow_backend not in {
            "auto",
            "e_jepa",
            "heuristic_baseline",
            "reference",
        }:
            raise HTTPException(
                status_code=400,
                detail=(
                    "shadow_forecast_backend must be auto, e_jepa, heuristic_baseline, or reference"
                ),
            )
        ranked_provider, ranked_model = ctx.resolve_llm_provider(
            request.provider,
            request.model,
        )
        if ranked_provider is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "ranked counterfactual scoring needs an LLM provider key. "
                    "Set OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, "
                    "GEMINI_API_KEY, or OPENROUTER_API_KEY in .env."
                ),
            )
        try:
            result = run_ranked_counterfactual_experiment(
                world,
                artifacts_root=ctx.whatif_artifacts_root() / "ranked",
                label=request.label,
                objective_pack_id=request.objective_pack_id,
                candidate_interventions=[
                    WhatIfCandidateIntervention(
                        label=(candidate.label or candidate.prompt[:40]).strip(),
                        prompt=candidate.prompt,
                    )
                    for candidate in request.candidates
                ],
                event_id=request.event_id,
                thread_id=request.thread_id,
                rollout_count=request.rollout_count,
                provider=ranked_provider,
                model=ranked_model,
                shadow_forecast_backend=(
                    default_forecast_backend()
                    if normalized_shadow_backend == "auto"
                    else normalized_shadow_backend
                ),
                ejepa_epochs=request.ejepa_epochs,
                ejepa_batch_size=request.ejepa_batch_size,
                ejepa_force_retrain=request.ejepa_force_retrain,
                ejepa_device=request.ejepa_device,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        payload = result.model_dump(mode="json")
        payload["mode"] = "live"
        payload["source_dir"] = str(source_dir)
        return JSONResponse(payload)


__all__ = [
    "register_workspace_whatif_routes",
    "run_counterfactual_experiment",
    "run_ranked_counterfactual_experiment",
]
