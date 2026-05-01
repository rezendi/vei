from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from vei.whatif.api import answer_saved_historical_chat

from ._api_models import WhatIfChatRequest
from ._whatif_helpers import can_use_saved_bundle


def register_workspace_whatif_chat_route(app: FastAPI, root: Path) -> None:
    @app.post("/api/workspace/whatif/chat")
    def api_workspace_whatif_chat(request: WhatIfChatRequest) -> JSONResponse:
        if not can_use_saved_bundle(
            root,
            requested_source=request.source,
            event_id=request.event_id,
            thread_id=request.thread_id,
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "saved historical chat is available only for the saved branch "
                    "workspace and its recorded branch point"
                ),
            )
        try:
            response = answer_saved_historical_chat(
                root,
                message=request.message,
                selected_citation_ids=request.selected_citation_ids,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(response.model_dump(mode="json"))


__all__ = ["register_workspace_whatif_chat_route"]
