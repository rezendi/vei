"""Public API for the Company Wiki module."""

from __future__ import annotations

import time
from pathlib import Path

from vei.context.api import ContextSnapshot

from .builder import (
    CONTEXT_SNAPSHOT_FILE,
    build_wiki,
    discover_overlays,
    ensure_canonical_history,
    write_canonical_history_if_missing,
)
from .models import (
    CompanyWikiBuildReport,
    CompanyWikiCitation,
    CompanyWikiPage,
    CompanyWikiQueryHit,
    CompanyWikiQueryResult,
    CompanyWikiSection,
    CompanyWikiSnapshot,
)

WIKI_SNAPSHOT_FILE = "company_wiki.json"
WIKI_BUILD_REPORT_FILE = "wiki_build_report.json"
WIKI_INDEX_FILE = "index.md"
WIKI_PAGES_DIR = "pages"
DEFAULT_WIKI_OUTPUT_DIR = (".artifacts", "wiki")


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------


def build_wiki_from_context_path(path: str | Path) -> CompanyWikiSnapshot:
    """Build a wiki snapshot from a context-snapshot path or directory."""

    snapshot_path = _resolve_snapshot_path(path)
    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    bundle = ensure_canonical_history(snapshot, snapshot_path)
    knowledge_store, skill_map, control_events, _warnings = discover_overlays(
        snapshot_path=snapshot_path,
    )
    return build_wiki(
        snapshot=snapshot,
        bundle=bundle,
        knowledge_store=knowledge_store,
        skill_map=skill_map,
        control_events=control_events,
        source_ref=str(snapshot_path),
    )


def build_wiki_from_workspace(
    workspace: str | Path,
    *,
    context_path: str | Path | None = None,
) -> CompanyWikiSnapshot:
    """Build a wiki snapshot from a workspace root, discovering curated layers.

    When the workspace has a manifest (``vei_project.json``), the wiki uses the
    manifest title as the organization display name so it matches what Studio
    shows in the header. Falls back to the context snapshot's org name.
    """

    workspace_path = Path(workspace).expanduser().resolve()
    snapshot_path = _resolve_snapshot_path(context_path or workspace_path)
    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    bundle = ensure_canonical_history(snapshot, snapshot_path)
    knowledge_store, skill_map, control_events, _warnings = discover_overlays(
        snapshot_path=snapshot_path,
        workspace_root=workspace_path,
    )
    return build_wiki(
        snapshot=snapshot,
        bundle=bundle,
        knowledge_store=knowledge_store,
        skill_map=skill_map,
        control_events=control_events,
        source_ref=str(workspace_path),
        workspace_title=_load_workspace_title(workspace_path),
    )


def _load_workspace_title(workspace_path: Path) -> str:
    """Read the workspace manifest title, returning empty string if unavailable."""

    try:
        from vei.workspace import WORKSPACE_MANIFEST
        from vei.workspace.api import load_workspace

        manifest_path = workspace_path / WORKSPACE_MANIFEST
        if not manifest_path.exists():
            return ""
        manifest = load_workspace(workspace_path)
        return (manifest.title or manifest.name or "").strip()
    except Exception:  # noqa: BLE001
        return ""


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def write_wiki_artifacts(
    snapshot: CompanyWikiSnapshot,
    output_dir: str | Path,
    *,
    warnings: list[str] | None = None,
    elapsed_ms: int = 0,
) -> CompanyWikiBuildReport:
    """Write company_wiki.json + index.md + pages/*.md + wiki_build_report.json."""

    output_path = Path(output_dir).expanduser().resolve()
    pages_path = output_path / WIKI_PAGES_DIR
    pages_path.mkdir(parents=True, exist_ok=True)

    json_path = output_path / WIKI_SNAPSHOT_FILE
    json_path.write_text(
        snapshot.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )

    index_md = _render_index_md(snapshot)
    (output_path / WIKI_INDEX_FILE).write_text(index_md, encoding="utf-8")

    for page in snapshot.pages:
        (pages_path / f"{page.page_id}.md").write_text(
            _render_page_md(page),
            encoding="utf-8",
        )

    citation_count = sum(page.citation_count for page in snapshot.pages)
    overlays_present = dict(snapshot.metadata.get("overlays_present", {}))

    report = CompanyWikiBuildReport(
        status="ok",
        page_count=len(snapshot.pages),
        citation_count=citation_count,
        elapsed_ms=int(elapsed_ms),
        built_at=snapshot.built_at,
        output_dir=str(output_path),
        warnings=list(warnings or []),
        overlays_present=overlays_present,
    )
    (output_path / WIKI_BUILD_REPORT_FILE).write_text(
        report.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def load_wiki_snapshot(wiki_dir: str | Path) -> CompanyWikiSnapshot | None:
    """Load a previously written CompanyWikiSnapshot, returning None if missing."""

    base = Path(wiki_dir).expanduser().resolve()
    candidate = base
    if base.is_dir():
        candidate = base / WIKI_SNAPSHOT_FILE
    if not candidate.exists():
        return None
    return CompanyWikiSnapshot.model_validate_json(
        candidate.read_text(encoding="utf-8")
    )


# ---------------------------------------------------------------------------
# Refresh + query
# ---------------------------------------------------------------------------


def refresh_wiki(
    workspace: str | Path,
    *,
    output_dir: str | Path | None = None,
    context_path: str | Path | None = None,
) -> CompanyWikiBuildReport:
    """Rebuild the wiki for a workspace and write artifacts to disk."""

    workspace_path = Path(workspace).expanduser().resolve()
    target_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir
        else workspace_path.joinpath(*DEFAULT_WIKI_OUTPUT_DIR)
    )

    started_at = time.perf_counter()
    warnings: list[str] = []

    try:
        snapshot_path = _resolve_snapshot_path(context_path or workspace_path)
    except FileNotFoundError as exc:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        report = CompanyWikiBuildReport(
            status="error",
            page_count=0,
            citation_count=0,
            elapsed_ms=elapsed_ms,
            built_at="",
            output_dir=str(target_dir),
            errors=[str(exc)],
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / WIKI_BUILD_REPORT_FILE).write_text(
            report.model_dump_json(indent=2) + "\n",
            encoding="utf-8",
        )
        return report

    snapshot = ContextSnapshot.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    write_canonical_history_if_missing(snapshot, snapshot_path)
    bundle = ensure_canonical_history(snapshot, snapshot_path)
    knowledge_store, skill_map, control_events, overlay_warnings = discover_overlays(
        snapshot_path=snapshot_path,
        workspace_root=workspace_path,
    )
    warnings.extend(overlay_warnings)

    wiki_snapshot = build_wiki(
        snapshot=snapshot,
        bundle=bundle,
        knowledge_store=knowledge_store,
        skill_map=skill_map,
        control_events=control_events,
        source_ref=str(workspace_path),
    )
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    report = write_wiki_artifacts(
        wiki_snapshot,
        target_dir,
        warnings=warnings,
        elapsed_ms=elapsed_ms,
    )
    if warnings:
        report.status = "partial"
        # Re-persist the updated status.
        (target_dir / WIKI_BUILD_REPORT_FILE).write_text(
            report.model_dump_json(indent=2) + "\n",
            encoding="utf-8",
        )
    return report


def query_wiki(
    snapshot: CompanyWikiSnapshot,
    query: str,
    *,
    limit: int = 10,
) -> CompanyWikiQueryResult:
    """Title-only keyword search over the wiki.

    V1 matches against page title + section title with simple substring + token
    overlap scoring. No body search, no snippet generation. V2 may add body
    search.
    """

    query_text = (query or "").strip().lower()
    if not query_text:
        return CompanyWikiQueryResult(query=query or "", hits=[], total_hits=0)
    tokens = {token for token in _tokenize(query_text) if token}

    hits: list[CompanyWikiQueryHit] = []
    for page in snapshot.pages:
        page_title_lower = page.title.lower()
        page_score = _score_text(page_title_lower, tokens, query_text)
        if page_score > 0.0:
            hits.append(
                CompanyWikiQueryHit(
                    page_id=page.page_id,
                    page_title=page.title,
                    section_id="",
                    section_title="",
                    score=page_score,
                    matched_field="page_title",
                    citations=list(page.citations[:3]),
                )
            )
        for section in page.sections:
            section_title_lower = section.title.lower()
            section_score = _score_text(section_title_lower, tokens, query_text)
            if section_score > 0.0:
                hits.append(
                    CompanyWikiQueryHit(
                        page_id=page.page_id,
                        page_title=page.title,
                        section_id=section.section_id,
                        section_title=section.title,
                        score=section_score,
                        matched_field="section_title",
                        citations=list(section.citations[:3]),
                    )
                )

    hits.sort(key=lambda hit: (-hit.score, hit.page_id, hit.section_id))
    limited = hits[: max(1, int(limit))]
    return CompanyWikiQueryResult(query=query, hits=limited, total_hits=len(hits))


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _render_index_md(snapshot: CompanyWikiSnapshot) -> str:
    lines = [
        f"# {snapshot.organization_name} -- Company Wiki",
        "",
        f"_Built at {snapshot.built_at}._",
        "",
        f"- Source: `{snapshot.source_ref}`",
        f"- Source providers: {', '.join(snapshot.source_providers) or '(none)'}",
        "",
        "## Pages",
        "",
    ]
    for page in snapshot.pages:
        lines.append(f"- [{page.title}](pages/{page.page_id}.md) -- {page.summary}")
    lines.append("")
    lines.append("## Metadata")
    lines.append("")
    for key, value in sorted(snapshot.metadata.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    return "\n".join(lines)


def _render_page_md(page: CompanyWikiPage) -> str:
    lines = [f"# {page.title}", "", f"_{page.summary}_", ""]
    for section in page.sections:
        lines.append(f"## {section.title}")
        lines.append("")
        lines.append(section.body_md.strip())
        lines.append("")
        if section.citations:
            lines.append("**Citations**")
            for citation in section.citations[:8]:
                lines.append(_render_citation_md(citation))
            lines.append("")
    if page.next_steps:
        lines.append("## What would make this richer")
        lines.append("")
        for hint in page.next_steps:
            lines.append(f"- {hint}")
        lines.append("")
    return "\n".join(lines)


def _render_citation_md(citation: CompanyWikiCitation) -> str:
    pieces: list[str] = [f"`{citation.source}`", f"`{citation.ref_id}`"]
    if citation.surface:
        pieces.append(f"surface=`{citation.surface}`")
    if citation.title:
        pieces.append(citation.title)
    return "- " + " :: ".join(pieces)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_snapshot_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    if resolved.is_dir():
        resolved = resolved / CONTEXT_SNAPSHOT_FILE
    if not resolved.exists():
        raise FileNotFoundError(f"context snapshot not found: {resolved}")
    return resolved


def _tokenize(text: str) -> list[str]:
    cleaned: list[str] = []
    current: list[str] = []
    for char in text.lower():
        if char.isalnum():
            current.append(char)
            continue
        if current:
            cleaned.append("".join(current))
            current = []
    if current:
        cleaned.append("".join(current))
    return [token for token in cleaned if len(token) >= 2]


def _score_text(haystack_lower: str, tokens: set[str], full_query: str) -> float:
    if not haystack_lower:
        return 0.0
    score = 0.0
    if full_query and full_query in haystack_lower:
        score += 5.0
    for token in tokens:
        if token in haystack_lower:
            score += 1.0
    return score


__all__ = [
    "CompanyWikiBuildReport",
    "CompanyWikiCitation",
    "CompanyWikiPage",
    "CompanyWikiQueryHit",
    "CompanyWikiQueryResult",
    "CompanyWikiSection",
    "CompanyWikiSnapshot",
    "DEFAULT_WIKI_OUTPUT_DIR",
    "WIKI_BUILD_REPORT_FILE",
    "WIKI_INDEX_FILE",
    "WIKI_PAGES_DIR",
    "WIKI_SNAPSHOT_FILE",
    "build_wiki_from_context_path",
    "build_wiki_from_workspace",
    "load_wiki_snapshot",
    "query_wiki",
    "refresh_wiki",
    "write_wiki_artifacts",
]
