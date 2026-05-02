"""Wiki page generation -- union projection over canonical events + curated overlays.

The builder projects every page from canonical events and StructureView first, then
overlays curated layers (KnowledgeStore, CompanySkillMap, Control events) as
enrichment. This means every page is meaningful by default; curated layers upgrade
content visibly but are never required.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from vei.context.api import (
    CanonicalHistoryBundle,
    CanonicalHistoryIndexRow,
    ContextSnapshot,
    build_canonical_history_bundle,
    build_canonical_history_readiness,
    canonical_history_paths,
    canonical_history_sidecars_exist,
    load_canonical_history_bundle,
    write_canonical_history_sidecars,
)
from vei.events.api import CanonicalEvent
from vei.knowledge.api import (
    KnowledgeAsset,
    KnowledgeStoreSnapshot,
    asset_status_at,
    store_from_payload,
    utc_now_ms,
)
from vei.skillmap.api import CompanySkill, CompanySkillMap
from vei.structure.api import (
    DerivedCase,
    DerivedEntity,
    StructureView,
    build_structure_view_from_canonical_events,
)

from .models import (
    CASE_CONFIDENCE_THRESHOLD,
    CompanyWikiCitation,
    CompanyWikiPage,
    CompanyWikiSection,
    CompanyWikiSnapshot,
)

CONTEXT_SNAPSHOT_FILE = "context_snapshot.json"

# Max entries rendered in any single page section. Larger lists are truncated to
# keep wiki artifacts compact (the JSON payload can otherwise reach tens of MB
# on real workspaces with thousands of cases or actors). Truncated sections show
# a "Showing top N of M" footer so users know to drill in via `vei wiki query`.
MAX_LIST_ENTRIES = 60

# Curated-overlay file discovery candidates relative to the workspace root.
_KNOWLEDGE_SNAPSHOT_CANDIDATES: tuple[tuple[str, ...], ...] = (
    ("knowledge_snapshot.json",),
    (".artifacts", "knowledge", "knowledge_snapshot.json"),
)

_SKILL_MAP_CANDIDATES: tuple[tuple[str, ...], ...] = (
    ("company_skill_map.json",),
    ("skill_map", "company_skill_map.json"),
    (".artifacts", "skillmap", "company_skill_map.json"),
)

_CONTROL_EVENTS_CANDIDATES: tuple[tuple[str, ...], ...] = (
    (".artifacts", "control", "control_events.jsonl"),
)

# Canonical-event domain.kind → wiki knowledge-entry kind. The pattern is matched
# loosely: surface from the event row + last segment of the kind. The kinds align
# with KnowledgeAssetKind so projected entries can later be promoted to curated
# assets.
_KNOWLEDGE_KIND_BY_SURFACE: dict[str, str] = {
    "docs": "note",
    "mail": "email_summary",
    "slack": "note",
    "tickets": "note",
    "crm": "metric_snapshot",
}

# Granola transcripts get their own kind for clarity.
_GRANOLA_KIND = "transcript"


# ---------------------------------------------------------------------------
# Public builder entry-point
# ---------------------------------------------------------------------------


def build_wiki(
    *,
    snapshot: ContextSnapshot,
    bundle: CanonicalHistoryBundle,
    knowledge_store: KnowledgeStoreSnapshot | None = None,
    skill_map: CompanySkillMap | None = None,
    control_events: list[CanonicalEvent] | None = None,
    source_ref: str = "",
    workspace_title: str = "",
) -> CompanyWikiSnapshot:
    """Project a CompanyWikiSnapshot from the spine and optional curated layers."""

    structure = build_structure_view_from_canonical_events(bundle.events)
    rows = bundle.index.rows
    organization_name = (
        workspace_title or snapshot.organization_name or bundle.index.organization_name
    )
    organization_domain = (
        snapshot.organization_domain or bundle.index.organization_domain
    )
    source_providers = sorted(
        {
            *bundle.index.source_providers,
            *(
                str(source.provider or "").strip().lower()
                for source in snapshot.sources
                if str(source.provider or "").strip()
            ),
        }
    )

    overlays_present = {
        "knowledge_store": knowledge_store is not None and bool(knowledge_store.assets),
        "skill_map": skill_map is not None and bool(skill_map.skills),
        "control_events": bool(control_events),
    }

    primary_actor_ids = _collect_primary_actor_ids(bundle.events)

    pages: list[CompanyWikiPage] = [
        _build_overview_page(
            snapshot=snapshot,
            bundle=bundle,
            structure=structure,
            knowledge_store=knowledge_store,
            skill_map=skill_map,
            control_events=control_events or [],
        ),
        _build_recent_changes_page(rows=rows, knowledge_store=knowledge_store),
        _build_cases_page(structure=structure, knowledge_store=knowledge_store),
        _build_people_page(
            structure=structure,
            skill_map=skill_map,
            primary_actor_ids=primary_actor_ids,
        ),
        _build_knowledge_page(rows=rows, knowledge_store=knowledge_store),
        _build_skills_page(structure=structure, skill_map=skill_map),
        _build_evidence_index_page(
            rows=rows,
            knowledge_store=knowledge_store,
            skill_map=skill_map,
        ),
    ]

    last_event_ts_ms = max((int(row.ts_ms) for row in rows), default=0)
    citation_count = sum(page.citation_count for page in pages)

    return CompanyWikiSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        built_at=_iso_now(),
        source_ref=source_ref,
        source_providers=source_providers,
        pages=pages,
        metadata={
            "event_count": bundle.index.event_count,
            "case_count": bundle.index.case_count,
            "entity_count": len(structure.entities),
            "relation_count": len(structure.relations),
            "last_event_ts_ms": last_event_ts_ms,
            "overlays_present": overlays_present,
            "citation_count": citation_count,
            "snapshot_role": str(
                snapshot.metadata.get("snapshot_role", "company_history_bundle")
            ),
        },
    )


# ---------------------------------------------------------------------------
# Workspace / context-path discovery helpers
# ---------------------------------------------------------------------------


def discover_overlays(
    *,
    snapshot_path: Path,
    workspace_root: Path | None = None,
) -> tuple[
    KnowledgeStoreSnapshot | None,
    CompanySkillMap | None,
    list[CanonicalEvent],
    list[str],
]:
    """Locate optional curated layers next to a snapshot or in a workspace root.

    Control events are read **only** from explicit agent-activity ingest batches
    under ``provenance/agent_activity/<source>/<batch>/canonical_events.jsonl``.
    The snapshot's own canonical bundle is excluded so we never double-count the
    spine as imported control evidence.
    """

    warnings: list[str] = []
    search_roots: list[Path] = [snapshot_path.parent]
    if workspace_root is not None and workspace_root != snapshot_path.parent:
        search_roots.append(workspace_root)

    knowledge_store = _try_load_knowledge_store(search_roots, warnings)
    skill_map = _try_load_skill_map(search_roots, warnings)
    control_events: list[CanonicalEvent] = []
    if workspace_root is not None:
        control_events = _load_agent_activity_batches(workspace_root, warnings)

    return knowledge_store, skill_map, control_events, warnings


def _load_agent_activity_batches(
    workspace_root: Path,
    warnings: list[str],
) -> list[CanonicalEvent]:
    """Load events only from explicit agent-activity ingest batches.

    Excludes the snapshot's own ``canonical_events.jsonl`` so the count of
    "control events" reflects ingested agent activity, not the canonical spine.
    """

    agent_root = workspace_root / "provenance" / "agent_activity"
    if not agent_root.is_dir():
        return []

    try:
        from vei.events.api import load_canonical_events_jsonl
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"failed to import canonical event loader: {exc}")
        return []

    events: list[CanonicalEvent] = []
    seen: set[Path] = set()
    for path in sorted(agent_root.glob("*/*/canonical_events.jsonl")):
        resolved = path.resolve()
        if resolved in seen or not path.is_file():
            continue
        seen.add(resolved)
        try:
            events.extend(load_canonical_events_jsonl(path))
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"failed to load agent-activity batch {path}: {exc}")
    return events


def ensure_canonical_history(
    snapshot: ContextSnapshot,
    snapshot_path: Path,
) -> CanonicalHistoryBundle:
    """Load canonical history sidecars; build them on the fly if they are missing."""

    if canonical_history_sidecars_exist(snapshot_path):
        bundle = load_canonical_history_bundle(snapshot_path)
        if bundle is not None:
            return bundle

    bundle = build_canonical_history_bundle(snapshot)
    paths = canonical_history_paths(snapshot_path)
    bundle = bundle.model_copy(update={"paths": paths})
    return bundle


def write_canonical_history_if_missing(
    snapshot: ContextSnapshot,
    snapshot_path: Path,
) -> None:
    """Persist canonical history sidecars next to the snapshot if absent."""

    if canonical_history_sidecars_exist(snapshot_path):
        return
    write_canonical_history_sidecars(snapshot, snapshot_path)


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------


def _build_overview_page(
    *,
    snapshot: ContextSnapshot,
    bundle: CanonicalHistoryBundle,
    structure: StructureView,
    knowledge_store: KnowledgeStoreSnapshot | None,
    skill_map: CompanySkillMap | None,
    control_events: list[CanonicalEvent],
) -> CompanyWikiPage:
    index = bundle.index
    sections: list[CompanyWikiSection] = []

    summary_lines = [
        f"- Organization: **{index.organization_name or snapshot.organization_name}**",
    ]
    if index.organization_domain or snapshot.organization_domain:
        summary_lines.append(
            f"- Domain: `{index.organization_domain or snapshot.organization_domain}`"
        )
    summary_lines.extend(
        [
            f"- Captured at: `{index.captured_at or snapshot.captured_at or 'unknown'}`",
            f"- Source providers: {', '.join(index.source_providers) or '(none)'}",
            f"- Canonical events: **{index.event_count}**",
            f"- Cases: **{index.case_count}** (clear + probable)",
            f"- Surfaces covered: {', '.join(sorted(index.surface_counts)) or '(none)'}",
            f"- Entities: **{len(structure.entities)}**",
            f"- Relations: **{len(structure.relations)}**",
        ]
    )
    if knowledge_store is not None:
        summary_lines.append(
            f"- Curated knowledge assets: **{len(knowledge_store.assets)}**"
        )
    if skill_map is not None:
        summary_lines.append(f"- Curated skills: **{skill_map.skill_count}**")
    if control_events:
        summary_lines.append(f"- Control events imported: **{len(control_events)}**")

    sections.append(
        CompanyWikiSection(
            section_id="overview-summary",
            title="At a glance",
            body_md="\n".join(summary_lines),
        )
    )

    # Readiness for world-modeling -- borrowed from canonical_history.
    try:
        readiness = build_canonical_history_readiness(bundle.paths.snapshot_path)
    except Exception:  # noqa: BLE001
        readiness = None
    if readiness is not None and readiness.available:
        readiness_md = "\n".join(
            [
                f"- Readiness label: **{readiness.readiness_label}**",
                f"- Ready for world modeling: **{readiness.ready_for_world_modeling}**",
                f"- Stitched events: **{readiness.stitched_event_count}** "
                f"({readiness.high_confidence_stitch_count} high confidence)",
                f"- Exact timestamps: **{readiness.exact_timestamp_count}**",
            ]
        )
        if readiness.notes:
            readiness_md += "\n\n" + "\n".join(f"> {note}" for note in readiness.notes)
        sections.append(
            CompanyWikiSection(
                section_id="overview-readiness",
                title="Readiness",
                body_md=readiness_md,
            )
        )

    # "Structural hints" section -- absorbs the developer-only Orientation card.
    if structure.suggested_investigations:
        hint_lines = [
            f"{index_pos + 1}. {hint}"
            for index_pos, hint in enumerate(structure.suggested_investigations)
        ]
        hint_citations = [
            CompanyWikiCitation(
                source="structure_hypothesis",
                ref_id=hypothesis.hypothesis_id,
                title=hypothesis.title,
                snippet=hypothesis.summary,
                authority="projected",
            )
            for hypothesis in structure.hypotheses
        ]
        sections.append(
            CompanyWikiSection(
                section_id="overview-structural-hints",
                title="Structural hints",
                body_md=(
                    "Auto-derived hints from cross-surface structure. These are "
                    "starting points for investigation, not conclusions.\n\n"
                    + "\n".join(hint_lines)
                ),
                citations=hint_citations,
            )
        )

    next_steps: list[str] = []
    if not knowledge_store or not knowledge_store.assets:
        next_steps.append(
            "Run `vei knowledge ingest` to add curated knowledge assets on top of the projected entries."
        )
    if not skill_map or not skill_map.skills:
        next_steps.append(
            "Run `vei skillmap build` to extract reusable procedures from the event spine."
        )
    if index.event_count == 0:
        next_steps.append(
            "Run `vei context capture` or `vei twin onboard` to bring in canonical history."
        )

    page = CompanyWikiPage(
        page_id="overview",
        page_kind="overview",
        title="Overview",
        summary=(
            f"{index.organization_name or snapshot.organization_name} -- "
            f"{index.event_count} events across {len(index.source_providers)} source(s)."
        ),
        sections=sections,
        last_event_ts_ms=max((int(row.ts_ms) for row in index.rows), default=0),
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_recent_changes_page(
    *,
    rows: list[CanonicalHistoryIndexRow],
    knowledge_store: KnowledgeStoreSnapshot | None,
    limit: int = MAX_LIST_ENTRIES,
) -> CompanyWikiPage:
    sorted_rows = sorted(rows, key=lambda row: int(row.ts_ms), reverse=True)
    recent_rows = sorted_rows[:limit]
    truncated = max(0, len(rows) - len(recent_rows))

    sections: list[CompanyWikiSection] = []
    if recent_rows:
        timeline_lines: list[str] = []
        citations: list[CompanyWikiCitation] = []
        for row in recent_rows:
            timeline_lines.append(_format_event_row_md(row))
            citations.append(_event_citation(row))
        if truncated > 0:
            timeline_lines.append("")
            timeline_lines.append(
                f"_Showing newest {len(recent_rows)} of {len(rows)} canonical "
                f"events. Use `vei wiki query` or the JSON snapshot for the "
                f"remaining {truncated}._"
            )
        sections.append(
            CompanyWikiSection(
                section_id="recent-canonical-events",
                title="Canonical history (last " + str(len(recent_rows)) + " events)",
                body_md="\n".join(timeline_lines),
                citations=citations,
                metadata={
                    "total_count": len(rows),
                    "shown_count": len(recent_rows),
                    "truncated_count": truncated,
                },
            )
        )
    else:
        sections.append(
            CompanyWikiSection(
                section_id="recent-canonical-events",
                title="Canonical history",
                body_md="_No canonical events captured yet._",
            )
        )

    # Knowledge supersession events from the curated store.
    if knowledge_store is not None and knowledge_store.events:
        super_events = [
            event
            for event in knowledge_store.events
            if isinstance(event, dict)
            and str(event.get("kind", "")).startswith("knowledge.")
        ]
        if super_events:
            super_events_sorted = sorted(
                super_events,
                key=lambda evt: int(evt.get("ts_ms", 0) or 0),
                reverse=True,
            )[:20]
            super_lines = [
                "- "
                + str(evt.get("kind", "knowledge.event"))
                + " :: "
                + str(evt.get("event_id", ""))
                for evt in super_events_sorted
            ]
            sections.append(
                CompanyWikiSection(
                    section_id="recent-knowledge-events",
                    title="Recent knowledge updates",
                    body_md="\n".join(super_lines),
                    metadata={"authority": "curated"},
                )
            )

    next_steps: list[str] = []
    if not recent_rows:
        next_steps.append(
            "Capture canonical history with `vei context capture` or `vei twin onboard`."
        )

    page = CompanyWikiPage(
        page_id="recent-changes",
        page_kind="recent_changes",
        title="Recent Changes",
        summary=f"Newest {len(recent_rows)} canonical events.",
        sections=sections,
        last_event_ts_ms=int(recent_rows[0].ts_ms) if recent_rows else 0,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_cases_page(
    *,
    structure: StructureView,
    knowledge_store: KnowledgeStoreSnapshot | None,
) -> CompanyWikiPage:
    clear: list[DerivedCase] = []
    probable: list[DerivedCase] = []
    for case in structure.cases:
        if case.confidence >= CASE_CONFIDENCE_THRESHOLD:
            clear.append(case)
        else:
            probable.append(case)
    clear.sort(key=lambda case: (-len(case.event_ids), case.title))
    probable.sort(key=lambda case: (-len(case.event_ids), case.title))

    asset_links_by_object_ref: dict[str, list[KnowledgeAsset]] = {}
    if knowledge_store is not None:
        for asset in knowledge_store.assets.values():
            for ref in asset.linked_object_refs:
                asset_links_by_object_ref.setdefault(ref, []).append(asset)

    sections: list[CompanyWikiSection] = []
    sections.append(
        _format_cases_section(
            section_id="clear-cases",
            title=f"Clear cases ({len(clear)})",
            cases=clear,
            asset_links=asset_links_by_object_ref,
            confidence_band="clear",
        )
    )
    sections.append(
        _format_cases_section(
            section_id="probable-cases",
            title=f"Probable cases ({len(probable)})",
            cases=probable,
            asset_links=asset_links_by_object_ref,
            confidence_band="probable",
        )
    )

    next_steps: list[str] = []
    if not structure.cases:
        next_steps.append(
            "No cases stitched yet. Capture more dated activity across surfaces, "
            "or normalize imports with `vei context normalize`."
        )

    page = CompanyWikiPage(
        page_id="cases",
        page_kind="cases",
        title="Cases",
        summary=f"{len(clear)} clear, {len(probable)} probable.",
        sections=sections,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_people_page(
    *,
    structure: StructureView,
    skill_map: CompanySkillMap | None,
    primary_actor_ids: set[str],
) -> CompanyWikiPage:
    """People page filtered to entities that act as the primary actor of an event.

    The structure module conflates ``event.actor_ref`` (primary actor) with
    ``event.participants`` (which includes channel names, thread refs, doc IDs,
    etc.). To avoid surfacing channels and IDs as "people," we keep only entities
    whose normalized id was the primary actor of at least one canonical event.
    Anything else stays in the structure index but does not show up here.
    """

    actor_entities = [
        entity for entity in structure.entities if entity.entity_type == "actor"
    ]
    primary = [
        entity
        for entity in actor_entities
        if _strip_actor_prefix(entity.entity_id) in primary_actor_ids
    ]
    primary.sort(key=lambda entity: (-len(entity.evidence.event_ids), entity.entity_id))

    secondary = [
        entity
        for entity in actor_entities
        if _strip_actor_prefix(entity.entity_id) not in primary_actor_ids
    ]
    secondary.sort(
        key=lambda entity: (-len(entity.evidence.event_ids), entity.entity_id)
    )

    skill_owners: dict[str, list[CompanySkill]] = defaultdict(list)
    if skill_map is not None:
        for skill in skill_map.skills:
            owner = (skill.owner or skill.reviewer or "").strip().lower()
            if owner:
                skill_owners[owner].append(skill)

    sections: list[CompanyWikiSection] = []
    sections.append(
        _format_actor_section(
            section_id="people-actors",
            title=f"People ({len(primary)})",
            entities=primary,
            skill_owners=skill_owners,
            empty_md=(
                "_No primary actors derived from the canonical event spine yet._"
            ),
        )
    )

    if secondary:
        sections.append(
            _format_actor_section(
                section_id="people-other-identifiers",
                title=(f"Other identifiers seen in events ({len(secondary)})"),
                entities=secondary,
                skill_owners=skill_owners,
                empty_md="",
                metadata={"authority": "projected", "kind": "secondary_actors"},
                description=(
                    "These identifiers (channels, thread refs, doc/issue IDs) "
                    "appear as participants in canonical events but never as "
                    "the primary actor. Listed here for completeness."
                ),
            )
        )

    next_steps: list[str] = []
    if not primary and not secondary:
        next_steps.append(
            "Capture canonical history with `vei context capture` or `vei twin onboard`."
        )

    summary_parts = [f"{len(primary)} primary actor(s)"]
    if secondary:
        summary_parts.append(f"{len(secondary)} other identifier(s)")
    page = CompanyWikiPage(
        page_id="people",
        page_kind="people",
        title="People",
        summary=", ".join(summary_parts) + ".",
        sections=sections,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_knowledge_page(
    *,
    rows: list[CanonicalHistoryIndexRow],
    knowledge_store: KnowledgeStoreSnapshot | None,
) -> CompanyWikiPage:
    sections: list[CompanyWikiSection] = []

    # ---- Curated overlay first (higher authority) ----
    if knowledge_store is not None and knowledge_store.assets:
        now_ms = utc_now_ms()
        active_assets = [
            asset
            for asset in knowledge_store.assets.values()
            if asset_status_at(asset, now_ms=now_ms) != "expired"
        ]
        active_assets.sort(
            key=lambda asset: (
                -int(asset.metadata.get("captured_at_ms", 0) or 0),
                asset.asset_id,
            )
        )
        visible_curated = active_assets[:MAX_LIST_ENTRIES]
        truncated_curated = len(active_assets) - len(visible_curated)
        body_lines: list[str] = []
        citations: list[CompanyWikiCitation] = []
        for asset in visible_curated:
            status = asset_status_at(asset, now_ms=now_ms)
            body_lines.append(
                f"- **{asset.title}** ({asset.kind}) :: status `{status}` "
                f":: id `{asset.asset_id}`\n"
                f"  - {(asset.summary or asset.body[:160]).strip()}"
            )
            citations.append(
                CompanyWikiCitation(
                    source="knowledge_asset",
                    ref_id=asset.asset_id,
                    title=asset.title,
                    snippet=asset.summary or asset.body[:160],
                    authority="curated",
                    metadata={"kind": asset.kind, "status": status},
                )
            )
        if truncated_curated > 0:
            body_lines.append("")
            body_lines.append(
                f"_Showing newest {len(visible_curated)} of {len(active_assets)} "
                f"curated assets. Use `vei wiki query` or the JSON snapshot for "
                f"the remaining {truncated_curated}._"
            )
        sections.append(
            CompanyWikiSection(
                section_id="knowledge-curated",
                title=f"Curated assets ({len(active_assets)})",
                body_md="\n".join(body_lines),
                citations=citations,
                metadata={
                    "authority": "curated",
                    "total_count": len(active_assets),
                    "shown_count": len(visible_curated),
                    "truncated_count": truncated_curated,
                },
            )
        )

    # ---- Projected entries from canonical events (always populated) ----
    projected_entries = _project_knowledge_entries(rows)
    if projected_entries:
        visible_projected = projected_entries[:MAX_LIST_ENTRIES]
        truncated_projected = len(projected_entries) - len(visible_projected)
        body_lines = []
        citations = []
        for entry in visible_projected:
            body_lines.append(
                f"- _{entry['kind']}_ :: **{entry['title']}** "
                f"({entry['surface']} / {entry['provider']})\n"
                f"  - {entry['snippet']}"
            )
            citations.append(
                CompanyWikiCitation(
                    source="canonical_event",
                    ref_id=entry["event_id"],
                    surface=entry["surface"],
                    title=entry["title"],
                    snippet=entry["snippet"],
                    timestamp=entry["timestamp"],
                    authority="projected",
                    metadata={"kind": entry["kind"], "provider": entry["provider"]},
                )
            )
        if truncated_projected > 0:
            body_lines.append("")
            body_lines.append(
                f"_Showing newest {len(visible_projected)} of "
                f"{len(projected_entries)} projected entries. Use `vei wiki "
                f"query` or the JSON snapshot for the remaining "
                f"{truncated_projected}._"
            )
        sections.append(
            CompanyWikiSection(
                section_id="knowledge-projected",
                title=f"Projected from events ({len(projected_entries)})",
                body_md="\n".join(body_lines),
                citations=citations,
                metadata={
                    "authority": "projected",
                    "total_count": len(projected_entries),
                    "shown_count": len(visible_projected),
                    "truncated_count": truncated_projected,
                },
            )
        )

    if not sections:
        sections.append(
            CompanyWikiSection(
                section_id="knowledge-empty",
                title="No knowledge yet",
                body_md=(
                    "No canonical events with knowledge-mappable kinds and no "
                    "curated knowledge store. Capture context to populate this page."
                ),
            )
        )

    next_steps: list[str] = []
    has_curated = knowledge_store is not None and bool(knowledge_store.assets)
    if not has_curated:
        next_steps.append(
            "Run `vei knowledge ingest --provider notion --provider granola ...` "
            "to upgrade these projections to curated assets with citations."
        )

    page = CompanyWikiPage(
        page_id="knowledge",
        page_kind="knowledge",
        title="Knowledge",
        summary=(
            f"{len(projected_entries)} projected entries"
            + (
                f" + {len(knowledge_store.assets)} curated assets"
                if knowledge_store is not None and knowledge_store.assets
                else ""
            )
            + "."
        ),
        sections=sections,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_skills_page(
    *,
    structure: StructureView,
    skill_map: CompanySkillMap | None,
) -> CompanyWikiPage:
    sections: list[CompanyWikiSection] = []

    curated_skills: list[CompanySkill] = list(skill_map.skills) if skill_map else []
    candidates = _project_candidate_skills(structure)

    # When curated skills exist, mark candidates that are clearly subsumed.
    suppressed_candidate_ids: set[str] = set()
    if curated_skills:
        curated_kinds = _curated_skill_signatures(curated_skills)
        for candidate in candidates:
            if candidate["signature"] in curated_kinds:
                suppressed_candidate_ids.add(candidate["candidate_id"])

    # ---- Curated overlay ----
    if curated_skills:
        body_lines: list[str] = []
        citations: list[CompanyWikiCitation] = []
        for skill in curated_skills:
            body_lines.append(
                f"- **{skill.title}** ({skill.candidate_type}) "
                f":: status `{skill.status}` :: usefulness {skill.usefulness_score:.2f}\n"
                f"  - {skill.summary}"
            )
            citations.append(
                CompanyWikiCitation(
                    source="skill",
                    ref_id=skill.skill_id,
                    title=skill.title,
                    snippet=skill.summary,
                    authority="curated",
                    metadata={
                        "status": skill.status,
                        "candidate_type": skill.candidate_type,
                    },
                )
            )
        sections.append(
            CompanyWikiSection(
                section_id="skills-curated",
                title=f"Curated skills ({len(curated_skills)})",
                body_md="\n".join(body_lines),
                citations=citations,
                metadata={"authority": "curated"},
            )
        )

    # ---- Candidate skills derived from StructureView relations ----
    visible_candidates = [
        candidate
        for candidate in candidates
        if candidate["candidate_id"] not in suppressed_candidate_ids
    ]
    if visible_candidates:
        body_lines = []
        citations = []
        for candidate in visible_candidates[:40]:
            note = (
                " (superseded by curated skill)"
                if candidate["candidate_id"] in suppressed_candidate_ids
                else ""
            )
            body_lines.append(
                f"- _candidate_ :: {candidate['title']}{note}\n"
                f"  - Trigger: {candidate['trigger']}\n"
                f"  - Evidence events: **{candidate['evidence_count']}** "
                f":: surfaces: {', '.join(candidate['surfaces']) or '(none)'}"
            )
            citations.extend(candidate["citations"][:5])
        sections.append(
            CompanyWikiSection(
                section_id="skills-candidates",
                title=f"Candidate skills ({len(visible_candidates)})",
                body_md="\n".join(body_lines),
                citations=citations,
                metadata={"authority": "projected"},
            )
        )
    elif not curated_skills:
        sections.append(
            CompanyWikiSection(
                section_id="skills-empty",
                title="No skills yet",
                body_md=(
                    "No relation patterns frequent enough to surface as candidate "
                    "skills, and no curated skill map. Capture more activity or run "
                    "`vei skillmap build`."
                ),
            )
        )

    # Gaps from curated skill map.
    if skill_map is not None and skill_map.gaps:
        gap_lines = [
            f"- **{gap.title}** ({gap.severity})\n  - {gap.reason}"
            for gap in skill_map.gaps
        ]
        sections.append(
            CompanyWikiSection(
                section_id="skills-gaps",
                title=f"Gaps ({len(skill_map.gaps)})",
                body_md="\n".join(gap_lines),
                metadata={"authority": "curated"},
            )
        )

    next_steps: list[str] = []
    if not curated_skills:
        next_steps.append(
            "Run `vei skillmap build --source-dir <context>` to upgrade candidates "
            "to evidence-backed skills with replay scores."
        )

    page = CompanyWikiPage(
        page_id="skills",
        page_kind="skills",
        title="Skills",
        summary=(
            f"{len(curated_skills)} curated, {len(visible_candidates)} candidate(s)."
        ),
        sections=sections,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


def _build_evidence_index_page(
    *,
    rows: list[CanonicalHistoryIndexRow],
    knowledge_store: KnowledgeStoreSnapshot | None,
    skill_map: CompanySkillMap | None,
) -> CompanyWikiPage:
    surface_counts = Counter(row.surface for row in rows if row.surface)
    provider_counts = Counter(row.provider for row in rows if row.provider)
    domain_counts = Counter(row.domain for row in rows if row.domain)

    sections: list[CompanyWikiSection] = []
    sections.append(
        CompanyWikiSection(
            section_id="evidence-counts",
            title="Counts",
            body_md="\n".join(
                [
                    f"- Canonical events: **{len(rows)}**",
                    "- By surface: "
                    + (
                        ", ".join(
                            f"{name}={count}"
                            for name, count in sorted(surface_counts.items())
                        )
                        or "(none)"
                    ),
                    "- By provider: "
                    + (
                        ", ".join(
                            f"{name}={count}"
                            for name, count in sorted(provider_counts.items())
                        )
                        or "(none)"
                    ),
                    "- By domain: "
                    + (
                        ", ".join(
                            f"{name}={count}"
                            for name, count in sorted(domain_counts.items())
                        )
                        or "(none)"
                    ),
                    "- Curated knowledge assets: "
                    + str(len(knowledge_store.assets) if knowledge_store else 0),
                    "- Curated skills: "
                    + str(skill_map.skill_count if skill_map else 0),
                ]
            ),
        )
    )

    if rows:
        sample_rows = rows[: min(20, len(rows))]
        sample_md = "\n".join(
            f"- `{row.event_id}` :: {row.surface} / {row.provider} / {row.kind} :: {row.timestamp}"
            for row in sample_rows
        )
        citations = [_event_citation(row) for row in sample_rows]
        sections.append(
            CompanyWikiSection(
                section_id="evidence-sample",
                title=f"Sample event IDs ({len(sample_rows)} of {len(rows)})",
                body_md=sample_md,
                citations=citations,
            )
        )

    next_steps: list[str] = []
    if not rows:
        next_steps.append(
            "Capture canonical history with `vei context capture` or `vei twin onboard`."
        )

    page = CompanyWikiPage(
        page_id="evidence-index",
        page_kind="evidence_index",
        title="Evidence Index",
        summary=f"{len(rows)} canonical events indexed.",
        sections=sections,
        next_steps=next_steps,
    )
    page.citation_count = _count_citations(page)
    return page


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------


def _project_knowledge_entries(
    rows: Iterable[CanonicalHistoryIndexRow],
) -> list[dict[str, str]]:
    """Map canonical events to wiki knowledge entries.

    Threads (mail/slack) are aggregated to one entry per `thread_ref` to avoid
    overwhelming the page with one-message-per-row.
    """

    rows_list = list(rows)
    aggregated_threads: dict[str, dict[str, Any]] = {}
    standalone: list[dict[str, str]] = []

    for row in rows_list:
        provider = (row.provider or "").lower()
        surface = (row.surface or "").lower()
        kind = _knowledge_kind_for_row(row)
        if not kind:
            continue

        # Aggregate by thread for mail and slack threads.
        if surface in {"mail", "slack"} and row.thread_ref:
            bucket = aggregated_threads.setdefault(
                row.thread_ref,
                {
                    "kind": kind,
                    "title": row.subject or row.thread_ref,
                    "snippet": row.snippet,
                    "surface": surface,
                    "provider": provider,
                    "event_id": row.event_id,
                    "timestamp": row.timestamp,
                    "ts_ms": int(row.ts_ms),
                    "count": 0,
                },
            )
            bucket["count"] += 1
            if int(row.ts_ms) > int(bucket["ts_ms"]):
                bucket["ts_ms"] = int(row.ts_ms)
                bucket["timestamp"] = row.timestamp
            continue

        standalone.append(
            {
                "kind": kind,
                "title": row.subject or row.event_id,
                "snippet": (row.snippet or row.subject or "")[:280],
                "surface": surface,
                "provider": provider,
                "event_id": row.event_id,
                "timestamp": row.timestamp,
                "ts_ms": int(row.ts_ms),
            }
        )

    aggregated_list = []
    for thread_ref, bucket in aggregated_threads.items():
        bucket_copy = dict(bucket)
        bucket_copy["title"] = (
            bucket_copy["title"]
            if bucket_copy["count"] == 1
            else f"{bucket_copy['title']} ({bucket_copy['count']} messages)"
        )
        bucket_copy["snippet"] = (bucket_copy.get("snippet") or "")[:280]
        bucket_copy.pop("count", None)
        bucket_copy["thread_ref"] = thread_ref
        aggregated_list.append(bucket_copy)

    combined = standalone + aggregated_list
    combined.sort(key=lambda entry: int(entry.get("ts_ms", 0)), reverse=True)
    return combined


def _knowledge_kind_for_row(row: CanonicalHistoryIndexRow) -> str:
    provider = (row.provider or "").lower()
    surface = (row.surface or "").lower()
    kind = (row.kind or "").lower()
    if provider == "granola" or kind == "meeting_note":
        return _GRANOLA_KIND
    if surface == "crm" and "deal" in kind:
        return "metric_snapshot"
    return _KNOWLEDGE_KIND_BY_SURFACE.get(surface, "")


def _project_candidate_skills(
    structure: StructureView,
) -> list[dict[str, Any]]:
    """Group acted_on relations into candidate skill descriptions.

    A candidate is a (actor_kind, target_kind, action_kind) signature with at
    least three supporting events.
    """

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for relation in structure.relations:
        if relation.relation_type != "acted_on":
            continue
        actor_kind = _entity_kind_token(relation.source_entity_id)
        target_kind = _entity_kind_token(relation.target_entity_id)
        action_kind = str(relation.metadata.get("kind") or "act")
        signature = (actor_kind, target_kind, action_kind)
        bucket = grouped.setdefault(
            signature,
            {
                "actor_kind": actor_kind,
                "target_kind": target_kind,
                "action_kind": action_kind,
                "evidence_event_ids": set(),
                "surfaces": set(),
                "relation_ids": [],
            },
        )
        bucket["evidence_event_ids"].update(relation.evidence.event_ids)
        bucket["surfaces"].update(relation.evidence.surfaces)
        bucket["relation_ids"].append(relation.relation_id)

    candidates: list[dict[str, Any]] = []
    for signature, bucket in grouped.items():
        evidence_count = len(bucket["evidence_event_ids"])
        if evidence_count < 3:
            continue
        actor_kind, target_kind, action_kind = signature
        candidate_id = f"candidate-{actor_kind}-{action_kind}-{target_kind}"
        title = (
            f"{actor_kind.title()} {action_kind} on {target_kind}"
            if action_kind != "act"
            else f"{actor_kind.title()} acts on {target_kind}"
        )
        trigger = f"When {actor_kind} performs `{action_kind}` against {target_kind}."
        citations = [
            CompanyWikiCitation(
                source="canonical_event",
                ref_id=event_id,
                authority="projected",
            )
            for event_id in sorted(bucket["evidence_event_ids"])[:8]
        ]
        candidates.append(
            {
                "candidate_id": candidate_id,
                "signature": signature,
                "title": title,
                "trigger": trigger,
                "evidence_count": evidence_count,
                "surfaces": sorted(bucket["surfaces"]),
                "citations": citations,
            }
        )

    candidates.sort(key=lambda item: (-int(item["evidence_count"]), item["title"]))
    return candidates


def _curated_skill_signatures(
    skills: Iterable[CompanySkill],
) -> set[tuple[str, str, str]]:
    """Approximate signatures of curated skills so candidates can be marked
    superseded when they overlap.

    The match is intentionally loose: we only need to suppress noise, not produce
    a perfect pairing. We use the candidate-type and lowercased domain tokens.
    """

    signatures: set[tuple[str, str, str]] = set()
    for skill in skills:
        domain = (skill.domain or "").lower() or "domain"
        candidate_type = skill.candidate_type
        title_token = "_".join(skill.title.lower().split()[:2]) or "skill"
        signatures.add((candidate_type, title_token, domain))
    return signatures


# ---------------------------------------------------------------------------
# Markdown / citation utilities
# ---------------------------------------------------------------------------


def _format_event_row_md(row: CanonicalHistoryIndexRow) -> str:
    actor_md = f" :: actor `{row.actor_id}`" if row.actor_id else ""
    case_md = f" :: case `{row.case_id}`" if row.case_id else ""
    snippet = (row.snippet or row.subject or "").strip().replace("\n", " ")
    if len(snippet) > 200:
        snippet = snippet[:197] + "..."
    return (
        f"- `{row.timestamp}` -- **{row.surface}**/{row.provider} "
        f":: {row.kind}{actor_md}{case_md}\n"
        f"  - {snippet}"
    )


def _format_cases_section(
    *,
    section_id: str,
    title: str,
    cases: list[DerivedCase],
    asset_links: dict[str, list[KnowledgeAsset]],
    confidence_band: str,
) -> CompanyWikiSection:
    if not cases:
        return CompanyWikiSection(
            section_id=section_id,
            title=title,
            body_md=f"_No {confidence_band} cases yet._",
        )
    visible = cases[:MAX_LIST_ENTRIES]
    truncated = len(cases) - len(visible)
    body_lines: list[str] = []
    citations: list[CompanyWikiCitation] = []
    for case in visible:
        linked_assets = asset_links.get(case.case_id, [])
        linked_md = (
            f" :: curated assets: {', '.join(asset.asset_id for asset in linked_assets[:3])}"
            if linked_assets
            else ""
        )
        body_lines.append(
            f"- **{case.title}** ({case.case_id}) "
            f":: source `{case.case_source}` :: confidence `{case.confidence:.2f}`\n"
            f"  - Events: **{len(case.event_ids)}** "
            f":: surfaces: {', '.join(case.surfaces) or '(none)'}{linked_md}"
        )
        citations.append(
            CompanyWikiCitation(
                source="structure_case",
                ref_id=case.case_id,
                title=case.title,
                snippet=", ".join(case.surfaces),
                authority="projected",
                metadata={
                    "confidence": case.confidence,
                    "case_source": case.case_source,
                },
            )
        )
        for asset in linked_assets[:3]:
            citations.append(
                CompanyWikiCitation(
                    source="knowledge_asset",
                    ref_id=asset.asset_id,
                    title=asset.title,
                    authority="curated",
                )
            )
    if truncated > 0:
        body_lines.append("")
        body_lines.append(
            f"_Showing top {len(visible)} of {len(cases)} {confidence_band} "
            f"cases by event count. Use `vei wiki query` or the JSON snapshot "
            f"to drill into the remaining {truncated}._"
        )
    return CompanyWikiSection(
        section_id=section_id,
        title=title,
        body_md="\n".join(body_lines),
        citations=citations,
        metadata={
            "confidence_band": confidence_band,
            "total_count": len(cases),
            "shown_count": len(visible),
            "truncated_count": truncated,
        },
    )


def _event_citation(row: CanonicalHistoryIndexRow) -> CompanyWikiCitation:
    return CompanyWikiCitation(
        source="canonical_event",
        ref_id=row.event_id,
        surface=row.surface,
        title=row.subject,
        snippet=row.snippet,
        timestamp=row.timestamp,
        authority="projected",
        metadata={"provider": row.provider, "kind": row.kind},
    )


def _count_citations(page: CompanyWikiPage) -> int:
    total = len(page.citations)
    for section in page.sections:
        total += len(section.citations)
    return total


def _entity_kind_token(entity_id: str) -> str:
    if ":" in entity_id:
        return entity_id.split(":", 1)[0]
    return entity_id


def _strip_actor_prefix(entity_id: str) -> str:
    """Return the bare actor id from a structure entity id.

    Structure entities for actors use the ``actor:<id>`` form. This helper
    returns the raw id so it can be compared against ``event.actor_ref``.
    """

    if entity_id.startswith("actor:"):
        return entity_id[len("actor:") :]
    return entity_id


def _collect_primary_actor_ids(events: Iterable[CanonicalEvent]) -> set[str]:
    """Set of actor ids that act as the primary actor of at least one event."""

    primary: set[str] = set()
    for event in events:
        actor_ref = event.actor_ref
        if actor_ref is not None and actor_ref.actor_id:
            primary.add(actor_ref.actor_id)
    return primary


def _format_actor_section(
    *,
    section_id: str,
    title: str,
    entities: list[DerivedEntity],
    skill_owners: dict[str, list[CompanySkill]],
    empty_md: str,
    metadata: dict[str, Any] | None = None,
    description: str = "",
) -> CompanyWikiSection:
    if not entities:
        return CompanyWikiSection(
            section_id=section_id,
            title=title,
            body_md=empty_md,
            metadata=dict(metadata or {}),
        )
    visible = entities[:MAX_LIST_ENTRIES]
    truncated = len(entities) - len(visible)
    body_lines: list[str] = []
    if description:
        body_lines.append(f"_{description}_")
        body_lines.append("")
    citations: list[CompanyWikiCitation] = []
    for entity in visible:
        label = entity.title or entity.entity_id
        aliases = f" -- aliases: {', '.join(entity.aliases)}" if entity.aliases else ""
        event_count = len(entity.evidence.event_ids)
        surfaces = (
            f" :: surfaces: {', '.join(entity.evidence.surfaces)}"
            if entity.evidence.surfaces
            else ""
        )
        owner_skills = skill_owners.get(entity.entity_id.lower(), [])
        owner_skills.extend(skill_owners.get(label.lower(), []))
        owners_md = f" :: owns {len(owner_skills)} skill(s)" if owner_skills else ""
        body_lines.append(
            f"- **{label}** ({entity.entity_id}){aliases}\n"
            f"  - Events: **{event_count}**{surfaces}{owners_md}"
        )
        citations.append(
            CompanyWikiCitation(
                source="structure_entity",
                ref_id=entity.entity_id,
                title=label,
                snippet=", ".join(entity.aliases[:3]),
                authority="projected",
            )
        )
    if truncated > 0:
        body_lines.append("")
        body_lines.append(
            f"_Showing top {len(visible)} of {len(entities)} entries by event "
            f"count. Use `vei wiki query` or the JSON snapshot to find specific "
            f"entries among the remaining {truncated}._"
        )
    section_metadata = dict(metadata or {})
    section_metadata.update(
        {
            "total_count": len(entities),
            "shown_count": len(visible),
            "truncated_count": truncated,
        }
    )
    return CompanyWikiSection(
        section_id=section_id,
        title=title,
        body_md="\n".join(body_lines),
        citations=citations,
        metadata=section_metadata,
    )


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Curated-overlay file discovery
# ---------------------------------------------------------------------------


def _try_load_knowledge_store(
    search_roots: list[Path],
    warnings: list[str],
) -> KnowledgeStoreSnapshot | None:
    for root in search_roots:
        for parts in _KNOWLEDGE_SNAPSHOT_CANDIDATES:
            candidate = root.joinpath(*parts)
            if candidate.exists():
                try:
                    payload = candidate.read_text(encoding="utf-8")
                    return store_from_payload(json.loads(payload))
                except Exception as exc:  # noqa: BLE001
                    warnings.append(
                        f"failed to load knowledge snapshot at {candidate}: {exc}"
                    )
                    return None
    return None


def _try_load_skill_map(
    search_roots: list[Path],
    warnings: list[str],
) -> CompanySkillMap | None:
    for root in search_roots:
        for parts in _SKILL_MAP_CANDIDATES:
            candidate = root.joinpath(*parts)
            if candidate.exists():
                try:
                    return CompanySkillMap.model_validate_json(
                        candidate.read_text(encoding="utf-8")
                    )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"failed to load skill map at {candidate}: {exc}")
                    return None
    return None


__all__ = [
    "build_wiki",
    "discover_overlays",
    "ensure_canonical_history",
    "write_canonical_history_if_missing",
]
