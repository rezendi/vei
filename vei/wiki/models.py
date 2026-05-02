"""Pydantic schema for the Company Wiki materialized read model."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

WikiPageKind = Literal[
    "overview",
    "recent_changes",
    "cases",
    "people",
    "knowledge",
    "skills",
    "evidence_index",
]

WikiCitationSource = Literal[
    "canonical_event",
    "knowledge_asset",
    "skill",
    "structure_entity",
    "structure_case",
    "structure_hypothesis",
    "control_event",
]

WikiEntryAuthority = Literal["projected", "curated"]

WikiBuildStatus = Literal["ok", "partial", "error"]

CASE_CONFIDENCE_THRESHOLD = 0.7


class CompanyWikiCitation(BaseModel):
    """A typed citation back to the canonical source of a wiki claim."""

    source: WikiCitationSource
    ref_id: str
    surface: str = ""
    title: str = ""
    snippet: str = ""
    timestamp: str = ""
    authority: WikiEntryAuthority = "projected"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CompanyWikiSection(BaseModel):
    """A titled section within a wiki page."""

    section_id: str
    title: str
    body_md: str = ""
    citations: List[CompanyWikiCitation] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CompanyWikiPage(BaseModel):
    """A single wiki page with sections, citations, and freshness metadata."""

    page_id: str
    page_kind: WikiPageKind
    title: str
    summary: str = ""
    sections: List[CompanyWikiSection] = Field(default_factory=list)
    citations: List[CompanyWikiCitation] = Field(default_factory=list)
    last_event_ts_ms: int = 0
    citation_count: int = 0
    next_steps: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CompanyWikiSnapshot(BaseModel):
    """The full materialized wiki for a company."""

    schema_version: Literal["company_wiki_v1"] = "company_wiki_v1"
    organization_name: str
    organization_domain: str = ""
    built_at: str
    source_ref: str = ""
    source_providers: List[str] = Field(default_factory=list)
    pages: List[CompanyWikiPage] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def page_by_id(self, page_id: str) -> Optional[CompanyWikiPage]:
        for page in self.pages:
            if page.page_id == page_id:
                return page
        return None

    def page_by_kind(self, kind: WikiPageKind) -> Optional[CompanyWikiPage]:
        for page in self.pages:
            if page.page_kind == kind:
                return page
        return None


class CompanyWikiQueryHit(BaseModel):
    """A single search hit -- a page or section title match."""

    page_id: str
    page_title: str
    section_id: str = ""
    section_title: str = ""
    score: float = 0.0
    matched_field: Literal["page_title", "section_title"] = "page_title"
    citations: List[CompanyWikiCitation] = Field(default_factory=list)


class CompanyWikiQueryResult(BaseModel):
    """The full result of a wiki keyword search."""

    query: str
    hits: List[CompanyWikiQueryHit] = Field(default_factory=list)
    total_hits: int = 0


class CompanyWikiBuildReport(BaseModel):
    """Status report from a wiki build/refresh."""

    status: WikiBuildStatus = "ok"
    page_count: int = 0
    citation_count: int = 0
    elapsed_ms: int = 0
    built_at: str = ""
    output_dir: str = ""
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    overlays_present: Dict[str, bool] = Field(default_factory=dict)


__all__ = [
    "CASE_CONFIDENCE_THRESHOLD",
    "CompanyWikiBuildReport",
    "CompanyWikiCitation",
    "CompanyWikiPage",
    "CompanyWikiQueryHit",
    "CompanyWikiQueryResult",
    "CompanyWikiSection",
    "CompanyWikiSnapshot",
    "WikiBuildStatus",
    "WikiCitationSource",
    "WikiEntryAuthority",
    "WikiPageKind",
]
