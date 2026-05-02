"""Studio routes for the Company Wiki materialized view."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from vei.wiki.api import (
    DEFAULT_WIKI_OUTPUT_DIR,
    build_wiki_from_workspace,
    load_wiki_snapshot,
    query_wiki,
    refresh_wiki,
)


class WikiQueryRequest(BaseModel):
    query: str = Field(default="")
    limit: int = Field(default=10, ge=1, le=100)


def register_wiki_routes(app: FastAPI, root: Path) -> None:
    """Register wiki routes on the FastAPI app rooted at the given workspace."""

    def _wiki_artifact_dir() -> Path:
        return root.joinpath(*DEFAULT_WIKI_OUTPUT_DIR)

    def _resolve_snapshot(*, allow_in_memory: bool = True):
        persisted = load_wiki_snapshot(_wiki_artifact_dir())
        if persisted is not None:
            return persisted
        if not allow_in_memory:
            return None
        try:
            return build_wiki_from_workspace(root)
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"workspace context snapshot not found: {exc}",
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=400,
                detail=f"failed to build wiki: {exc}",
            ) from exc

    @app.get("/api/workspace/wiki")
    def api_wiki() -> JSONResponse:
        snapshot = _resolve_snapshot()
        return JSONResponse(snapshot.model_dump(mode="json"))

    @app.get("/api/workspace/wiki/pages")
    def api_wiki_pages() -> JSONResponse:
        snapshot = _resolve_snapshot()
        index = [
            {
                "page_id": page.page_id,
                "page_kind": page.page_kind,
                "title": page.title,
                "summary": page.summary,
                "section_count": len(page.sections),
                "citation_count": page.citation_count,
                "last_event_ts_ms": page.last_event_ts_ms,
                "next_steps": list(page.next_steps),
            }
            for page in snapshot.pages
        ]
        return JSONResponse({"pages": index, "built_at": snapshot.built_at})

    @app.get("/api/workspace/wiki/pages/{page_id}")
    def api_wiki_page(page_id: str) -> JSONResponse:
        snapshot = _resolve_snapshot()
        page = snapshot.page_by_id(page_id)
        if page is None:
            raise HTTPException(
                status_code=404,
                detail=f"wiki page not found: {page_id}",
            )
        return JSONResponse(page.model_dump(mode="json"))

    @app.post("/api/workspace/wiki/query")
    def api_wiki_query(request: WikiQueryRequest) -> JSONResponse:
        snapshot = _resolve_snapshot()
        result = query_wiki(snapshot, request.query, limit=request.limit)
        return JSONResponse(result.model_dump(mode="json"))

    @app.post("/api/workspace/wiki/refresh")
    def api_wiki_refresh() -> JSONResponse:
        try:
            report = refresh_wiki(root, output_dir=_wiki_artifact_dir())
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=400,
                detail=f"wiki refresh failed: {exc}",
            ) from exc
        return JSONResponse(report.model_dump(mode="json"))


__all__ = ["WikiQueryRequest", "register_wiki_routes"]
