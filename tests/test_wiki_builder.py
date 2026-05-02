"""Unit tests for the Company Wiki builder."""

from __future__ import annotations

from pathlib import Path


from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.knowledge.api import (
    KnowledgeAsset,
    KnowledgeProvenance,
    empty_store,
    register_asset,
    utc_now_ms,
)
from vei.skillmap.api import (
    CompanySkill,
    CompanySkillMap,
    SkillEvidenceRef,
    SkillStep,
    SkillTrigger,
)
from vei.wiki.api import (
    CompanyWikiBuildReport,
    build_wiki_from_context_path,
    build_wiki_from_workspace,
    load_wiki_snapshot,
    query_wiki,
    refresh_wiki,
    write_wiki_artifacts,
)


def _build_synthetic_snapshot() -> ContextSnapshot:
    return ContextSnapshot(
        organization_name="Acme Renewals",
        organization_domain="acme.example",
        captured_at="2026-04-01T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="slack",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "renewals",
                            "messages": [
                                {
                                    "ts": "1712017800",
                                    "user": "alice",
                                    "text": "CASE-456 renewal blocked by missing legal review",
                                },
                                {
                                    "ts": "1712021400",
                                    "user": "bob",
                                    "thread_ts": "1712017800",
                                    "text": "Will pull legal in tomorrow",
                                },
                            ],
                        }
                    ],
                    "users": [
                        {"id": "alice", "email": "alice@acme.example", "name": "Alice"},
                        {"id": "bob", "email": "bob@acme.example", "name": "Bob"},
                    ],
                },
            ),
            ContextSourceResult(
                provider="granola",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "transcripts": [
                        {
                            "id": "G-1",
                            "title": "CASE-456 weekly sync",
                            "body": "Discussed CASE-456 renewal blocker; legal sign-off pending.",
                            "owner": "alice@acme.example",
                            "updated_at": "2026-03-30T15:00:00Z",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="github",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "issues": [
                        {
                            "number": 42,
                            "title": "CASE-456 follow-up doc",
                            "body": "Track the renewal review SOP",
                            "author": "alice",
                            "updated_at": "2026-03-29T12:00:00Z",
                            "comments": [
                                {
                                    "id": 100,
                                    "body": "Blocking on legal review",
                                    "author": "bob",
                                    "created_at": "2026-03-29T13:00:00Z",
                                },
                                {
                                    "id": 101,
                                    "body": "Legal sign-off pending",
                                    "author": "carol",
                                    "created_at": "2026-03-30T09:00:00Z",
                                },
                                {
                                    "id": 102,
                                    "body": "Updated draft renewal doc",
                                    "author": "alice",
                                    "created_at": "2026-03-31T11:00:00Z",
                                },
                            ],
                        }
                    ]
                },
            ),
        ],
    )


def _write_snapshot_to(path: Path) -> Path:
    snapshot = _build_synthetic_snapshot()
    snapshot_path = path / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    return snapshot_path


def _curated_skill() -> CompanySkill:
    return CompanySkill(
        skill_id="SK-RENEWAL-1",
        title="Renewal review coordination",
        summary="Coordinate legal sign-off before sending renewal updates externally.",
        candidate_type="flagship_skill",
        domain="renewal_ops",
        usefulness_score=0.85,
        trigger=SkillTrigger(
            description="CASE-456 appears in renewal mail/Slack/docs.",
            signals=["CASE-456"],
        ),
        goal="Prepare a cited internal renewal-risk update.",
        steps=[
            SkillStep(
                step_id="step-1",
                instruction="Review case timeline.",
                read_only=True,
            )
        ],
        evidence_refs=[
            SkillEvidenceRef(ref_type="event", ref_id="evt-1", surface="slack")
        ],
    )


# ---------------------------------------------------------------------------
# Floor projection tests (no curated overlays)
# ---------------------------------------------------------------------------


def test_wiki_builds_seven_pages_from_context(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    assert wiki.organization_name == "Acme Renewals"
    page_kinds = [page.page_kind for page in wiki.pages]
    assert page_kinds == [
        "overview",
        "recent_changes",
        "cases",
        "people",
        "knowledge",
        "skills",
        "evidence_index",
    ]
    overview = wiki.page_by_kind("overview")
    assert overview is not None
    assert overview.next_steps  # empty curated layers should produce hints
    assert any("knowledge" in hint.lower() for hint in overview.next_steps)
    assert any("skill" in hint.lower() for hint in overview.next_steps)


def test_wiki_knowledge_page_projects_from_canonical_events(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    knowledge_page = wiki.page_by_kind("knowledge")
    assert knowledge_page is not None
    section_ids = {section.section_id for section in knowledge_page.sections}
    assert "knowledge-projected" in section_ids
    # No curated section when no KnowledgeStore is present.
    assert "knowledge-curated" not in section_ids
    projected_section = next(
        section
        for section in knowledge_page.sections
        if section.section_id == "knowledge-projected"
    )
    # Citations should all be projected from canonical events.
    assert all(
        citation.authority == "projected" for citation in projected_section.citations
    )
    assert any(
        citation.metadata.get("kind") == "transcript"
        for citation in projected_section.citations
    )


def test_wiki_knowledge_thread_aggregation_count_is_correct(tmp_path: Path) -> None:
    """The synthetic snapshot has 2 Slack messages on one thread.

    Thread aggregation should report exactly 2 messages, not 3 (off-by-one).
    """
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)
    knowledge_page = wiki.page_by_kind("knowledge")
    assert knowledge_page is not None
    projected_section = next(
        section
        for section in knowledge_page.sections
        if section.section_id == "knowledge-projected"
    )
    slack_entries = [
        citation
        for citation in projected_section.citations
        if citation.surface == "slack"
    ]
    assert slack_entries
    # The title should say "(2 messages)" for the 2-message thread.
    slack_title = slack_entries[0].title
    assert "(2 messages)" in slack_title, f"Expected '(2 messages)' in: {slack_title}"
    assert "(3 messages)" not in slack_title


def test_wiki_skills_page_derives_candidates_when_no_skill_map(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    skills_page = wiki.page_by_kind("skills")
    assert skills_page is not None
    assert skills_page.next_steps  # should hint to run `vei skillmap build`
    # Either we get candidates derived from relations, or the empty-state section.
    section_ids = {section.section_id for section in skills_page.sections}
    assert section_ids & {"skills-candidates", "skills-empty"}
    assert "skills-curated" not in section_ids


def test_wiki_cases_page_splits_by_confidence(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    cases_page = wiki.page_by_kind("cases")
    assert cases_page is not None
    section_ids = [section.section_id for section in cases_page.sections]
    assert section_ids == ["clear-cases", "probable-cases"]


def test_wiki_overview_includes_structural_hints(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    overview = wiki.page_by_kind("overview")
    assert overview is not None
    section_ids = {section.section_id for section in overview.sections}
    assert "overview-summary" in section_ids


def test_wiki_people_page_includes_actors(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    people_page = wiki.page_by_kind("people")
    assert people_page is not None
    actor_section = next(
        section
        for section in people_page.sections
        if section.section_id == "people-actors"
    )
    assert actor_section.citations
    assert all(
        citation.source == "structure_entity" for citation in actor_section.citations
    )


def test_wiki_people_page_separates_primary_actors_from_identifiers(
    tmp_path: Path,
) -> None:
    """Primary actors (event.actor_ref) should be cleanly separated from
    structure entities that appear only as participants (channel names, doc
    IDs, ticket numbers, etc.)."""

    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)
    people_page = wiki.page_by_kind("people")
    assert people_page is not None

    section_ids = {section.section_id for section in people_page.sections}
    assert "people-actors" in section_ids

    primary_section = next(
        section
        for section in people_page.sections
        if section.section_id == "people-actors"
    )
    primary_ids = {citation.ref_id for citation in primary_section.citations}
    # Real human-like actors should be present.
    assert "actor:alice@helix.example" in primary_ids or "actor:alice" in primary_ids

    if "people-other-identifiers" in section_ids:
        secondary_section = next(
            section
            for section in people_page.sections
            if section.section_id == "people-other-identifiers"
        )
        secondary_ids = {citation.ref_id for citation in secondary_section.citations}
        # Channels / doc IDs / ticket numbers should NOT appear in the primary
        # section -- they live in "other identifiers".
        for noisy in ("actor:renewals", "actor:42", "actor:G-1"):
            assert noisy not in primary_ids, f"{noisy} should be filtered from primary"
        # And only secondary identifiers should land in this section.
        assert primary_ids.isdisjoint(secondary_ids)


def test_wiki_evidence_index_counts_match_sources(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    page = wiki.page_by_kind("evidence_index")
    assert page is not None
    counts_section = next(
        section for section in page.sections if section.section_id == "evidence-counts"
    )
    assert "Canonical events" in counts_section.body_md


# ---------------------------------------------------------------------------
# Overlay enrichment tests
# ---------------------------------------------------------------------------


def test_wiki_knowledge_page_overlays_curated_assets(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _write_snapshot_to(workspace)

    store = empty_store()
    asset = KnowledgeAsset(
        asset_id="KA-RENEWAL-1",
        kind="sop",
        title="Renewal review SOP",
        body="Always pull legal before responding externally.",
        summary="Renewal SOP for legal reviews.",
        provenance=KnowledgeProvenance(
            source="manual",
            source_id="manual",
            captured_at="2026-03-31T00:00:00Z",
            authority=1.0,
        ),
        linked_object_refs=["case:CASE-456"],
    )
    register_asset(store, asset, now_ms=utc_now_ms())
    knowledge_dir = workspace / ".artifacts" / "knowledge"
    knowledge_dir.mkdir(parents=True)
    (knowledge_dir / "knowledge_snapshot.json").write_text(
        store.model_dump_json(indent=2), encoding="utf-8"
    )

    wiki = build_wiki_from_workspace(workspace)
    knowledge_page = wiki.page_by_kind("knowledge")
    assert knowledge_page is not None
    section_ids = {section.section_id for section in knowledge_page.sections}
    assert "knowledge-curated" in section_ids
    assert "knowledge-projected" in section_ids
    curated = next(
        section
        for section in knowledge_page.sections
        if section.section_id == "knowledge-curated"
    )
    assert any(
        citation.authority == "curated" and citation.ref_id == "KA-RENEWAL-1"
        for citation in curated.citations
    )


def test_wiki_skills_page_overlays_curated_skill_map(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _write_snapshot_to(workspace)

    skill_map = CompanySkillMap(
        organization_name="Acme Renewals",
        generated_at="2026-04-01T00:00:00Z",
        source_ref="unit-test",
        skill_count=1,
        skills=[_curated_skill()],
    )
    skillmap_dir = workspace / ".artifacts" / "skillmap"
    skillmap_dir.mkdir(parents=True)
    (skillmap_dir / "company_skill_map.json").write_text(
        skill_map.model_dump_json(indent=2), encoding="utf-8"
    )

    wiki = build_wiki_from_workspace(workspace)
    skills_page = wiki.page_by_kind("skills")
    assert skills_page is not None
    section_ids = {section.section_id for section in skills_page.sections}
    assert "skills-curated" in section_ids
    curated = next(
        section
        for section in skills_page.sections
        if section.section_id == "skills-curated"
    )
    assert any(
        citation.authority == "curated" and citation.source == "skill"
        for citation in curated.citations
    )
    # No empty-state section now that a curated skill exists.
    assert "skills-empty" not in section_ids


# ---------------------------------------------------------------------------
# Query + persistence tests
# ---------------------------------------------------------------------------


def test_query_returns_title_matches(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)

    result = query_wiki(wiki, "knowledge")
    assert result.total_hits >= 1
    assert all(
        hit.matched_field in ("page_title", "section_title") for hit in result.hits
    )

    # Empty query is allowed and returns no hits.
    empty = query_wiki(wiki, "")
    assert empty.total_hits == 0


def test_write_and_load_wiki_artifacts(tmp_path: Path) -> None:
    snapshot_path = _write_snapshot_to(tmp_path)
    wiki = build_wiki_from_context_path(snapshot_path)
    output_dir = tmp_path / "wiki"

    report: CompanyWikiBuildReport = write_wiki_artifacts(wiki, output_dir)
    assert report.status == "ok"
    assert report.page_count == 7
    assert (output_dir / "company_wiki.json").exists()
    assert (output_dir / "index.md").exists()
    for page in wiki.pages:
        assert (output_dir / "pages" / f"{page.page_id}.md").exists()
    assert (output_dir / "wiki_build_report.json").exists()

    loaded = load_wiki_snapshot(output_dir)
    assert loaded is not None
    assert loaded.organization_name == wiki.organization_name
    assert [page.page_id for page in loaded.pages] == [
        page.page_id for page in wiki.pages
    ]


def test_refresh_wiki_writes_to_workspace_artifacts(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _write_snapshot_to(workspace)

    report = refresh_wiki(workspace)
    assert report.status in ("ok", "partial")
    assert report.page_count == 7
    artifacts = workspace / ".artifacts" / "wiki"
    assert (artifacts / "company_wiki.json").exists()
    loaded = load_wiki_snapshot(artifacts)
    assert loaded is not None


def test_empty_context_produces_seven_pages_with_zero_counts(tmp_path: Path) -> None:
    snapshot = ContextSnapshot(
        organization_name="EmptyCo",
        organization_domain="empty.example",
        captured_at="2026-04-01T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[],
    )
    snapshot_path = tmp_path / "context_snapshot.json"
    snapshot_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")

    wiki = build_wiki_from_context_path(snapshot_path)
    assert len(wiki.pages) == 7
    overview = wiki.page_by_kind("overview")
    assert overview is not None
    assert "0" in overview.summary  # "0 events across 0 source(s)" or similar
    # Every page should have at least one section, even when empty.
    for page in wiki.pages:
        assert page.sections, f"page {page.page_id} has no sections"
