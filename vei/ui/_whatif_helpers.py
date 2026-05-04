from __future__ import annotations

from functools import lru_cache
import logging
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from vei.whatif.api import load_world, resolve_saved_whatif_bundle
from vei.whatif.artifact_validation import validate_saved_workspace

from ._api_models import load_workspace_historical_summary, resolve_whatif_source_path

logger = logging.getLogger(__name__)


@lru_cache(maxsize=32)
def _cached_validation_issues(root_text: str) -> tuple[str, ...]:
    return tuple(validate_saved_workspace(Path(root_text)))


def saved_workspace_validation_issues(root: Path) -> list[str]:
    resolved_root = str(root.expanduser().resolve())
    return list(_cached_validation_issues(resolved_root))


def load_historical_summary_or_400(root: Path):
    issues = saved_workspace_validation_issues(root)
    if issues:
        for issue in issues:
            logger.warning("workspace validation: %s", issue, extra={"root": str(root)})
    try:
        return load_workspace_historical_summary(root)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid saved workspace manifest: {exc}",
        ) from exc


def saved_historical_request_matches(
    root: Path,
    *,
    event_id: str | None = None,
    thread_id: str | None = None,
) -> Any | None:
    historical = load_historical_summary_or_400(root)
    if historical is None:
        return None
    if event_id and event_id != historical.branch_event_id:
        return None
    if thread_id and thread_id != historical.thread_id:
        return None
    return historical


def can_use_saved_bundle(
    root: Path,
    *,
    requested_mode: str = "saved",
    requested_source: str | None = None,
    event_id: str | None = None,
    thread_id: str | None = None,
) -> bool:
    if str(requested_mode or "").strip().lower() != "saved":
        return False
    saved_bundle = resolve_saved_whatif_bundle(root)
    if saved_bundle is None:
        return False
    historical = saved_historical_request_matches(
        root,
        event_id=event_id,
        thread_id=thread_id,
    )
    if historical is None:
        return False
    preferred_source = str(historical.source or "").strip().lower()
    if not saved_workspace_source_matches_request(
        root,
        requested_source=requested_source,
        historical_source=preferred_source,
    ):
        return False
    return True


def saved_workspace_source_matches_request(
    root: Path,
    *,
    requested_source: str | None,
    historical_source: str | None,
) -> bool:
    normalized_requested_source = str(requested_source or "").strip().lower()
    if not normalized_requested_source or normalized_requested_source == "auto":
        return True
    normalized_historical_source = str(historical_source or "").strip().lower()
    return normalized_requested_source == normalized_historical_source


def resolve_whatif_source_or_400(
    root: Path,
    source: str,
    *,
    max_events: int | None = None,
):
    resolved = resolve_whatif_source_path(root, requested_source=source)
    if resolved is None:
        raise HTTPException(
            status_code=404,
            detail="historical source is not configured for this workspace",
        )
    resolved_source, source_dir = resolved
    try:
        world = load_world(
            source=resolved_source,
            source_dir=source_dir,
            max_events=max_events,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return world, source_dir


__all__ = [
    "can_use_saved_bundle",
    "load_historical_summary_or_400",
    "resolve_whatif_source_or_400",
    "saved_historical_request_matches",
    "saved_workspace_validation_issues",
    "saved_workspace_source_matches_request",
]
