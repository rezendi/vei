from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from .episode import load_episode_manifest
from .filenames import (
    BUSINESS_STATE_COMPARISON_FILE,
    EPISODE_MANIFEST_FILE,
    EXPERIMENT_RESULT_FILE,
    PUBLIC_CONTEXT_FILE,
)
from .models import (
    WhatIfEpisodeManifest,
    WhatIfEventReference,
    WhatIfHistoricalChatCitation,
    WhatIfHistoricalChatResponse,
    WhatIfPublicContext,
)
from ._saved_bundle import resolve_saved_whatif_bundle

CHAT_CAVEAT = (
    "This chat path is bounded to the saved workspace. It separates facts visible "
    "before the branch point from recorded future and forecast artifacts saved "
    "after the branch point."
)


@dataclass(frozen=True)
class _CitationCandidate:
    citation: WhatIfHistoricalChatCitation
    text: str


def answer_saved_historical_chat(
    workspace_root: str | Path,
    *,
    message: str,
    selected_citation_ids: Sequence[str] = (),
    max_pre_branch_citations: int = 4,
) -> WhatIfHistoricalChatResponse:
    root = Path(workspace_root).expanduser().resolve()
    saved_bundle = resolve_saved_whatif_bundle(root)
    if saved_bundle is None:
        raise ValueError("saved historical chat requires a saved what-if workspace")

    manifest = load_episode_manifest(root)
    public_context = _load_saved_public_context(root, manifest.public_context)
    experiment_payload = saved_bundle.load_json(EXPERIMENT_RESULT_FILE) or {}
    comparison_payload = saved_bundle.load_json(BUSINESS_STATE_COMPARISON_FILE) or {}
    pre_branch_candidates = _pre_branch_candidates(
        history_preview=manifest.history_preview,
        public_context=public_context,
        branch_timestamp=manifest.branch_timestamp,
    )
    pre_branch = _select_pre_branch_citations(
        pre_branch_candidates,
        selected_citation_ids=selected_citation_ids,
        message=message,
        max_citations=max_pre_branch_citations,
    )
    branch_citation = _branch_point_citation(manifest.branch_event)
    post_branch = _post_branch_citations(
        manifest=manifest,
        experiment_payload=experiment_payload,
        comparison_payload=comparison_payload,
    )
    artifact_citations = _artifact_citations(
        workspace_root=root,
        bundle_root=saved_bundle.bundle_root,
        experiment_payload=experiment_payload,
        comparison_payload=comparison_payload,
    )
    before_summary = _before_branch_summary(pre_branch)
    after_summary = _after_branch_summary(
        manifest=manifest,
        experiment_payload=experiment_payload,
        comparison_payload=comparison_payload,
    )
    citations = [*pre_branch, branch_citation, *post_branch, *artifact_citations]
    cited_event_ids = [citation.event_id for citation in citations if citation.event_id]
    used_saved_artifacts = [
        citation.artifact_path
        for citation in artifact_citations
        if citation.artifact_path
    ]
    branch_date = manifest.branch_timestamp[:10]
    assistant_text = (
        f"Before branch ({branch_date}), the saved evidence says: "
        f"{before_summary} Branch point: {branch_citation.summary} "
        f"After branch in saved artifacts, not evidence available before the branch: "
        f"{after_summary} {CHAT_CAVEAT}"
    )
    return WhatIfHistoricalChatResponse(
        source=manifest.source,
        organization_name=manifest.organization_name,
        thread_id=manifest.thread_id,
        thread_subject=manifest.thread_subject,
        branch_event_id=manifest.branch_event_id,
        branch_timestamp=manifest.branch_timestamp,
        assistant_text=assistant_text,
        before_branch_summary=before_summary,
        after_branch_summary=after_summary,
        cited_event_ids=list(dict.fromkeys(cited_event_ids)),
        citations=_dedupe_citations(citations),
        used_saved_artifacts=list(dict.fromkeys(used_saved_artifacts)),
        caveat=CHAT_CAVEAT,
    )


def _load_saved_public_context(
    workspace_root: Path,
    manifest_public_context: WhatIfPublicContext | None,
) -> WhatIfPublicContext | None:
    sidecar_path = workspace_root / PUBLIC_CONTEXT_FILE
    if not sidecar_path.exists():
        return manifest_public_context
    try:
        return WhatIfPublicContext.model_validate_json(
            sidecar_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return manifest_public_context


def _pre_branch_candidates(
    *,
    history_preview: Sequence[WhatIfEventReference],
    public_context: WhatIfPublicContext | None,
    branch_timestamp: str,
) -> list[_CitationCandidate]:
    candidates: list[_CitationCandidate] = []
    for event in history_preview:
        if event.timestamp and event.timestamp >= branch_timestamp:
            continue
        citation = WhatIfHistoricalChatCitation(
            citation_id=event.event_id,
            scope="pre_branch",
            source_type="event",
            title=event.subject or event.thread_id or event.event_id,
            summary=_brief_text(event.snippet, max_chars=500),
            timestamp=event.timestamp,
            event_id=event.event_id,
        )
        candidates.append(
            _CitationCandidate(
                citation=citation,
                text=" ".join(
                    [
                        event.subject,
                        event.snippet,
                        event.actor_id,
                        event.target_id,
                        event.thread_id,
                        event.surface,
                    ]
                ),
            )
        )
    if public_context is None:
        return candidates
    candidates.extend(
        _public_context_candidates(
            public_context=public_context,
            branch_timestamp=branch_timestamp,
        )
    )
    return _dedupe_candidate_list(candidates)


def _public_context_candidates(
    *,
    public_context: WhatIfPublicContext,
    branch_timestamp: str,
) -> list[_CitationCandidate]:
    candidates: list[_CitationCandidate] = []
    for item in public_context.financial_snapshots:
        if item.as_of and item.as_of >= branch_timestamp:
            continue
        candidates.append(
            _CitationCandidate(
                citation=WhatIfHistoricalChatCitation(
                    citation_id=f"public:{item.snapshot_id}",
                    scope="pre_branch",
                    source_type="public_context",
                    title=item.label or item.snapshot_id,
                    summary=_brief_text(item.summary, max_chars=500),
                    timestamp=item.as_of,
                    source_ids=list(item.source_ids),
                    artifact_path=PUBLIC_CONTEXT_FILE,
                ),
                text=" ".join([item.label, item.kind, item.summary, *item.source_ids]),
            )
        )
    for item in public_context.public_news_events:
        if item.timestamp and item.timestamp >= branch_timestamp:
            continue
        candidates.append(
            _CitationCandidate(
                citation=WhatIfHistoricalChatCitation(
                    citation_id=f"public:{item.event_id}",
                    scope="pre_branch",
                    source_type="public_context",
                    title=item.headline or item.event_id,
                    summary=_brief_text(item.summary, max_chars=500),
                    timestamp=item.timestamp,
                    event_id=item.event_id,
                    source_ids=list(item.source_ids),
                    artifact_path=PUBLIC_CONTEXT_FILE,
                ),
                text=" ".join(
                    [item.headline, item.category, item.summary, *item.source_ids]
                ),
            )
        )
    for item in public_context.stock_history:
        if item.as_of and item.as_of >= branch_timestamp:
            continue
        candidates.append(
            _CitationCandidate(
                citation=WhatIfHistoricalChatCitation(
                    citation_id=f"public:stock:{item.as_of[:10]}",
                    scope="pre_branch",
                    source_type="public_context",
                    title=f"Enron stock close {item.close:g}",
                    summary=_brief_text(item.summary or item.label, max_chars=500),
                    timestamp=item.as_of,
                    source_ids=list(item.source_ids),
                    artifact_path=PUBLIC_CONTEXT_FILE,
                ),
                text=" ".join(
                    [
                        item.label,
                        item.summary,
                        str(item.close),
                        "stock market trading close",
                        *item.source_ids,
                    ]
                ),
            )
        )
    for item in public_context.credit_history:
        if item.as_of and item.as_of >= branch_timestamp:
            continue
        candidates.append(
            _CitationCandidate(
                citation=WhatIfHistoricalChatCitation(
                    citation_id=f"public:{item.event_id}",
                    scope="pre_branch",
                    source_type="public_context",
                    title=item.headline or item.event_id,
                    summary=_brief_text(item.summary, max_chars=500),
                    timestamp=item.as_of,
                    event_id=item.event_id,
                    source_ids=list(item.source_ids),
                    artifact_path=PUBLIC_CONTEXT_FILE,
                ),
                text=" ".join(
                    [
                        item.headline,
                        item.agency,
                        item.category,
                        item.summary,
                        item.from_rating,
                        item.to_rating,
                        item.outlook,
                        item.watch_status,
                        *item.source_ids,
                    ]
                ),
            )
        )
    for item in public_context.ferc_history:
        if item.timestamp and item.timestamp >= branch_timestamp:
            continue
        candidates.append(
            _CitationCandidate(
                citation=WhatIfHistoricalChatCitation(
                    citation_id=f"public:{item.event_id}",
                    scope="pre_branch",
                    source_type="public_context",
                    title=item.headline or item.event_id,
                    summary=_brief_text(item.summary, max_chars=500),
                    timestamp=item.timestamp,
                    event_id=item.event_id,
                    source_ids=list(item.source_ids),
                    artifact_path=PUBLIC_CONTEXT_FILE,
                ),
                text=" ".join(
                    [
                        item.headline,
                        item.agency,
                        item.category,
                        item.summary,
                        *item.source_ids,
                    ]
                ),
            )
        )
    return candidates


def _select_pre_branch_citations(
    candidates: Sequence[_CitationCandidate],
    *,
    selected_citation_ids: Sequence[str],
    message: str,
    max_citations: int,
) -> list[WhatIfHistoricalChatCitation]:
    by_id = {candidate.citation.citation_id: candidate for candidate in candidates}
    by_id.update(
        {
            candidate.citation.event_id: candidate
            for candidate in candidates
            if candidate.citation.event_id
        }
    )
    if selected_citation_ids:
        missing = [
            citation_id
            for citation_id in selected_citation_ids
            if citation_id not in by_id
        ]
        if missing:
            raise ValueError(
                "selected citations are not visible before this branch point: "
                + ", ".join(missing)
            )
        return _dedupe_citations(
            [by_id[citation_id].citation for citation_id in selected_citation_ids]
        )[:max_citations]

    terms = _query_terms(message)
    scored = [
        (
            _candidate_score(candidate, terms=terms),
            candidate.citation.timestamp,
            candidate.citation.citation_id,
            candidate,
        )
        for candidate in candidates
    ]
    if terms and any(score > 0 for score, _timestamp, _id, _candidate in scored):
        selected = [
            candidate
            for score, _timestamp, _id, candidate in sorted(
                scored,
                key=lambda item: (
                    -item[0],
                    item[3].citation.scope != "pre_branch",
                    item[1],
                    item[2],
                ),
            )
            if score > 0
        ]
    else:
        selected = [
            candidate
            for _score, _timestamp, _id, candidate in sorted(
                scored,
                key=lambda item: (
                    _fallback_priority(item[3].citation),
                    item[1],
                    item[2],
                ),
                reverse=True,
            )
        ]
    return _dedupe_citations([candidate.citation for candidate in selected])[
        :max_citations
    ]


def _candidate_score(candidate: _CitationCandidate, *, terms: Sequence[str]) -> int:
    haystack = f"{candidate.text} {candidate.citation.title} {candidate.citation.summary}".lower()
    return sum(1 for term in terms if term in haystack)


def _fallback_priority(citation: WhatIfHistoricalChatCitation) -> int:
    if citation.source_type == "event" and citation.source_ids:
        return 4
    if citation.source_type == "event" and citation.event_id.startswith("enron_"):
        return 4
    if (
        citation.source_type == "public_context"
        and not citation.citation_id.startswith("public:stock:")
    ):
        return 3
    if citation.source_type == "event":
        return 2
    return 1


def _branch_point_citation(
    branch_event: WhatIfEventReference,
) -> WhatIfHistoricalChatCitation:
    title = branch_event.subject or branch_event.thread_id or branch_event.event_id
    recipient = (
        branch_event.to_recipients[0]
        if branch_event.to_recipients
        else branch_event.target_id
    )
    summary = (
        f"{branch_event.actor_id} sent or handled {title!r}"
        f"{f' to {recipient}' if recipient else ''} at the branch point."
    )
    return WhatIfHistoricalChatCitation(
        citation_id=f"branch:{branch_event.event_id}",
        scope="branch_point",
        source_type="event",
        title=title,
        summary=summary,
        timestamp=branch_event.timestamp,
        event_id=branch_event.event_id,
    )


def _post_branch_citations(
    *,
    manifest: WhatIfEpisodeManifest,
    experiment_payload: dict[str, Any],
    comparison_payload: dict[str, Any],
) -> list[WhatIfHistoricalChatCitation]:
    citations: list[WhatIfHistoricalChatCitation] = []
    for event in manifest.baseline_future_preview[:3]:
        citations.append(
            WhatIfHistoricalChatCitation(
                citation_id=f"recorded_future:{event.event_id}",
                scope="post_branch_saved",
                source_type="event",
                title=event.subject or event.thread_id or event.event_id,
                summary=_brief_text(event.snippet, max_chars=500),
                timestamp=event.timestamp,
                event_id=event.event_id,
                artifact_path=EPISODE_MANIFEST_FILE,
            )
        )
    forecast_summary = _forecast_summary(experiment_payload)
    if forecast_summary:
        citations.append(
            WhatIfHistoricalChatCitation(
                citation_id="artifact:forecast_result",
                scope="saved_artifact",
                source_type="forecast",
                title="Saved forecast result",
                summary=forecast_summary,
                artifact_path=EXPERIMENT_RESULT_FILE,
            )
        )
    ranking_summary = _ranking_summary(comparison_payload)
    if ranking_summary:
        citations.append(
            WhatIfHistoricalChatCitation(
                citation_id="artifact:business_state_comparison",
                scope="saved_artifact",
                source_type="ranking",
                title="Saved business-state comparison",
                summary=ranking_summary,
                artifact_path=BUSINESS_STATE_COMPARISON_FILE,
            )
        )
    return _dedupe_citations(citations)


def _artifact_citations(
    *,
    workspace_root: Path,
    bundle_root: Path,
    experiment_payload: dict[str, Any],
    comparison_payload: dict[str, Any],
) -> list[WhatIfHistoricalChatCitation]:
    files = [
        (workspace_root / EPISODE_MANIFEST_FILE, "Saved episode manifest"),
        (workspace_root / PUBLIC_CONTEXT_FILE, "Saved public context"),
    ]
    if experiment_payload:
        files.append((bundle_root / EXPERIMENT_RESULT_FILE, "Saved experiment result"))
    if comparison_payload:
        files.append(
            (
                bundle_root / BUSINESS_STATE_COMPARISON_FILE,
                "Saved business-state comparison",
            )
        )
    citations: list[WhatIfHistoricalChatCitation] = []
    for path, title in files:
        if not path.exists():
            continue
        artifact_path = (
            str(path.relative_to(workspace_root.parent))
            if path.is_relative_to(workspace_root.parent)
            else path.name
        )
        citations.append(
            WhatIfHistoricalChatCitation(
                citation_id=f"artifact:{artifact_path}",
                scope="saved_artifact",
                source_type="saved_artifact",
                title=title,
                summary=f"Saved workspace artifact: {artifact_path}",
                artifact_path=artifact_path,
            )
        )
    return citations


def _before_branch_summary(citations: Sequence[WhatIfHistoricalChatCitation]) -> str:
    if not citations:
        return (
            "No matching pre-branch evidence was found in the saved workspace sample."
        )
    parts = []
    for citation in citations[:4]:
        prefix = citation.timestamp[:10] if citation.timestamp else "undated"
        summary = citation.summary or citation.title
        parts.append(f"{prefix}: {citation.title} ({_brief_text(summary)})")
    return " ".join(parts)


def _after_branch_summary(
    *,
    manifest: WhatIfEpisodeManifest,
    experiment_payload: dict[str, Any],
    comparison_payload: dict[str, Any],
) -> str:
    parts = [
        _historical_action_summary(manifest).strip(),
        _historical_outcome_summary(manifest).strip(),
        _forecast_summary(experiment_payload),
        _ranking_summary(comparison_payload),
    ]
    return " ".join(part for part in parts if part)


def _historical_action_summary(manifest: WhatIfEpisodeManifest) -> str:
    event = manifest.branch_event
    subject = manifest.thread_subject or event.subject or event.thread_id
    recipients = ", ".join(event.to_recipients) or event.target_id
    destination = f" to {recipients}" if recipients else ""
    attachment = (
        " with an attachment reference" if event.has_attachment_reference else ""
    )
    return (
        f"Historically, {event.actor_id} handled {subject!r}{destination}"
        f"{attachment} at the branch point."
    )


def _historical_outcome_summary(manifest: WhatIfEpisodeManifest) -> str:
    forecast = manifest.forecast
    return (
        f"The recorded future has {forecast.future_event_count} follow-up events, "
        f"{forecast.future_external_event_count} outside-addressed sends, and "
        f"{forecast.future_escalation_count} escalations."
    )


def _forecast_summary(payload: dict[str, Any]) -> str:
    forecast = payload.get("forecast_result")
    if not isinstance(forecast, dict):
        return ""
    business_change = forecast.get("business_state_change")
    if isinstance(business_change, dict):
        summary = str(business_change.get("summary") or "").strip()
        if summary:
            return summary
    return str(forecast.get("summary") or "").strip()


def _ranking_summary(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return ""
    first = candidates[0]
    if not isinstance(first, dict):
        return ""
    label = str(first.get("label") or "Top saved candidate").strip()
    business_change = first.get("business_state_change")
    if isinstance(business_change, dict):
        summary = str(business_change.get("summary") or "").strip()
        if summary:
            return f"Top saved candidate: {label}. {summary}"
    forecast = first.get("forecast")
    if isinstance(forecast, dict):
        summary = str(forecast.get("summary") or "").strip()
        if summary:
            return f"Top saved candidate: {label}. {summary}"
    return f"Top saved candidate: {label}."


def _query_terms(message: str) -> list[str]:
    stop = {
        "about",
        "after",
        "before",
        "branch",
        "could",
        "from",
        "known",
        "show",
        "that",
        "this",
        "what",
        "when",
        "were",
        "with",
    }
    return [
        part
        for part in re.split(r"[^a-z0-9]+", message.lower())
        if len(part) >= 4 and part not in stop
    ][:12]


def _dedupe_candidate_list(
    candidates: Sequence[_CitationCandidate],
) -> list[_CitationCandidate]:
    seen: set[str] = set()
    result: list[_CitationCandidate] = []
    for candidate in candidates:
        key = _citation_semantic_key(candidate.citation)
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def _dedupe_citations(
    citations: Sequence[WhatIfHistoricalChatCitation],
) -> list[WhatIfHistoricalChatCitation]:
    seen: set[str] = set()
    result: list[WhatIfHistoricalChatCitation] = []
    for citation in citations:
        key = _citation_semantic_key(citation)
        if key in seen:
            continue
        seen.add(key)
        result.append(citation)
    return result


def _citation_semantic_key(citation: WhatIfHistoricalChatCitation) -> str:
    return "|".join(
        [
            citation.scope,
            citation.source_type,
            citation.timestamp,
            citation.title.lower(),
            _brief_text(citation.summary, max_chars=120).lower(),
            citation.artifact_path,
        ]
    )


def _brief_text(value: str, *, max_chars: int = 240) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 3].rstrip() + "..."


__all__ = [
    "CHAT_CAVEAT",
    "answer_saved_historical_chat",
]
