from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from ._root_mode import load_ui_workspace_summary
from ._whatif_helpers import load_historical_summary_or_400
from ._whatif_routes import register_workspace_whatif_routes
from ._workspace_governor_routes import register_workspace_governor_routes
from ._workspace_route_context import WorkspaceRouteContext
from ._workspace_story_routes import register_workspace_story_routes


def register_workspace_routes(app: FastAPI, root: Path, *, deps: Any) -> None:
    ctx = WorkspaceRouteContext(root=root, deps=deps)

    @app.get("/api/workspace")
    def api_workspace() -> JSONResponse:
        payload = load_ui_workspace_summary(root)
        if payload is None:
            raise HTTPException(
                status_code=404,
                detail="workspace root is not configured",
            )
        return JSONResponse(payload.model_dump(mode="json"))

    @app.get("/api/workspace/historical")
    def api_workspace_historical() -> JSONResponse:
        if ctx.is_public_history_workspace():
            return JSONResponse({})
        payload = load_historical_summary_or_400(root)
        return JSONResponse(payload.model_dump(mode="json") if payload else {})

    register_workspace_whatif_routes(app, ctx)
    register_workspace_governor_routes(app, ctx)
    register_workspace_story_routes(app, ctx)


__all__ = ["register_workspace_routes"]
