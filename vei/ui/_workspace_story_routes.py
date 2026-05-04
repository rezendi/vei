from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from vei.verticals import (
    load_workspace_exports_preview,
    load_workspace_presentation,
    load_workspace_story_manifest,
)

from ._whatif_helpers import load_historical_summary_or_400
from ._workspace_route_context import WorkspaceRouteContext


def register_workspace_story_routes(
    app: FastAPI,
    ctx: WorkspaceRouteContext,
) -> None:
    root = ctx.root
    deps: Any = ctx.deps

    @app.get("/api/story")
    def api_story() -> JSONResponse:
        payload = load_workspace_story_manifest(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/exports-preview")
    def api_exports_preview() -> JSONResponse:
        return JSONResponse(
            [
                item.model_dump(mode="json")
                for item in load_workspace_exports_preview(root)
            ]
        )

    @app.get("/api/presentation")
    def api_presentation() -> JSONResponse:
        payload = load_workspace_presentation(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    @app.get("/api/dataset")
    def api_dataset() -> JSONResponse:
        payload = deps.load_workspace_dataset_bundle(root)
        if payload is None:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/fidelity")
    def api_fidelity() -> JSONResponse:
        if load_historical_summary_or_400(root) is not None:
            return JSONResponse({})
        try:
            payload = deps.get_or_build_workspace_fidelity_report(root)
        except ValueError:
            return JSONResponse({})
        return JSONResponse(payload.model_dump(mode="json"))


__all__ = ["register_workspace_story_routes"]
