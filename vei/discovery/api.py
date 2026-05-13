from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, Sequence

from vei.context.api import (
    CanonicalHistoryBundle,
    CanonicalHistoryIndex,
    CanonicalHistoryIndexRow,
    ContextSnapshot,
    build_canonical_history_bundle,
    canonical_history_paths,
)

from .models import (
    CompanyLineage,
    CompanyLineageSource,
    DiscoveryEvidenceRef,
    DiscoveryManifest,
    DiscoveryResult,
    DiscoverySourceInput,
    DiscoverySourceRole,
    GeneratedSkill,
    GeneratedSkillMap,
    WorkflowExample,
    WorkflowFamily,
)

COMPANY_LINEAGE_FILE = "company_lineage.json"
DISCOVERY_MANIFEST_FILE = "discovery_manifest.json"
SKILL_MAP_FILE = "skill_map.json"
SKILL_MAP_MARKDOWN_FILE = "skill_map.md"
WORKFLOW_FAMILIES_FILE = "workflow_families.json"
WORKFLOW_FAMILIES_MARKDOWN_FILE = "workflow_families.md"


@dataclass(frozen=True)
class _WorkflowSignature:
    key: str
    title: str
    domain: str
    keywords: tuple[str, ...]
    objective: str
    trigger: str
    canonical_steps: tuple[str, ...]
    skill_title: str
    skill_goal: str
    skill_steps: tuple[str, ...]
    outputs: tuple[str, ...]
    verification: tuple[str, ...]
    business_value_weight: float = 0.7


@dataclass
class _LoadedSource:
    source: CompanyLineageSource
    bundle: CanonicalHistoryBundle


@dataclass
class _AnnotatedRow:
    row: CanonicalHistoryIndexRow
    source_id: str
    source_role: DiscoverySourceRole
    source_label: str


_SIGNATURES: tuple[_WorkflowSignature, ...] = (
    _WorkflowSignature(
        key="partner_pipeline",
        title="Partner And Customer Pipeline",
        domain="commercial",
        keywords=(
            "partner",
            "partnership",
            "pilot",
            "demo",
            "proposal",
            "intro",
            "introduction",
            "deal",
            "sales",
            "customer",
            "client",
            "launch",
            "renewal",
        ),
        objective="Move partner, customer, and pilot opportunities from first signal to next committed step.",
        trigger="A partner, prospect, or customer thread asks for a demo, pilot, proposal, launch, or renewal next step.",
        canonical_steps=(
            "Identify the external account, ask, owner, and current stage.",
            "Collect prior context from mail, docs, tickets, and meeting/chat follow-ups.",
            "Summarize the opportunity, blockers, promised next step, and deadline.",
            "Prepare the next outbound artifact or internal decision request.",
            "Verify that the next step is logged back to the source system.",
        ),
        skill_title="Partner Pipeline Briefing",
        skill_goal="Turn scattered partner/customer evidence into a concise next-step brief.",
        skill_steps=(
            "Gather cited events for the account or thread.",
            "Extract the ask, owner, stage, last commitment, and unresolved blockers.",
            "Draft the next-step brief with explicit evidence references.",
            "Flag missing commercial, legal, or product context before recommending action.",
        ),
        outputs=("Opportunity brief", "Next-step checklist", "Open blockers"),
        verification=(
            "Every recommended next step cites at least one source event.",
            "The brief names an owner and a deadline when the evidence supports one.",
            "No external commitment is suggested without approval-gate language.",
        ),
        business_value_weight=0.95,
    ),
    _WorkflowSignature(
        key="privacy_provenance_review",
        title="Privacy, Consent, And Provenance Review",
        domain="governance",
        keywords=(
            "privacy",
            "consent",
            "gdpr",
            "pii",
            "data protection",
            "provenance",
            "legal",
            "compliance",
            "permission",
            "disclosure",
            "claim",
            "dpa",
        ),
        objective="Check that data, claims, and external-facing work are safe, consented, and evidence-backed.",
        trigger="A thread references privacy, consent, legal/compliance review, claims, or provenance-sensitive material.",
        canonical_steps=(
            "Identify the asset, claim, data subject, or permission boundary.",
            "Gather source evidence and prior decisions from the canonical spine.",
            "Classify the privacy/provenance risk and missing context.",
            "Prepare a review note with required approvals or caveats.",
            "Record the accepted boundary or blocker for future reuse.",
        ),
        skill_title="Privacy And Evidence Gate",
        skill_goal="Turn privacy/provenance-sensitive evidence into an approval-ready review note.",
        skill_steps=(
            "Collect cited events and assets that support the claim or data use.",
            "Separate observed evidence from proposed interpretation.",
            "List missing consent, legal, or provenance evidence.",
            "Generate an approval-gated recommendation with caveats.",
        ),
        outputs=("Privacy/provenance review note", "Evidence checklist"),
        verification=(
            "Every claim has a cited evidence reference.",
            "Missing consent or provenance is surfaced as a blocker.",
            "The skill never marks external publication as approved by itself.",
        ),
        business_value_weight=1.0,
    ),
    _WorkflowSignature(
        key="study_research_ops",
        title="Study And Research Operations",
        domain="research_ops",
        keywords=(
            "study",
            "interview",
            "participant",
            "diary",
            "research",
            "survey",
            "respondent",
            "recruit",
            "sample",
            "screener",
            "community",
            "session",
            "consent",
        ),
        objective="Run research/study work from participant signal through scheduling, evidence capture, and follow-up.",
        trigger="A thread or chat references a study, participant, interview, diary, survey, or research sample.",
        canonical_steps=(
            "Identify the study, target participant or cohort, and current stage.",
            "Check consent/privacy state and scheduling requirements.",
            "Gather study materials, participant context, and outstanding questions.",
            "Prepare the scheduling, reminder, or follow-up artifact.",
            "Confirm evidence capture and update the study status.",
        ),
        skill_title="Research Study Coordinator",
        skill_goal="Coordinate study/interview work with cited participant, timing, and consent context.",
        skill_steps=(
            "Find all cited events for the study or participant case.",
            "Extract scheduling state, required materials, consent signals, and blockers.",
            "Produce a participant-safe coordination note or follow-up checklist.",
            "Highlight any missing consent/privacy evidence before proceeding.",
        ),
        outputs=("Study coordination note", "Participant follow-up checklist"),
        verification=(
            "Consent/privacy evidence is checked before any participant-facing recommendation.",
            "Scheduling state is separated from research-content state.",
            "The generated note avoids inventing participant commitments.",
        ),
        business_value_weight=0.9,
    ),
    _WorkflowSignature(
        key="tester_marketplace_ops",
        title="Tester Marketplace And Candidate Ops",
        domain="marketplace_ops",
        keywords=(
            "tester",
            "qa tester",
            "application",
            "candidate",
            "job",
            "role",
            "next steps",
            "onboarding",
            "marketplace",
            "panel",
        ),
        objective="Move tester/candidate work from application or signal to qualification, onboarding, and next action.",
        trigger="A thread references tester applications, candidate next steps, onboarding, or marketplace operations.",
        canonical_steps=(
            "Identify the candidate/tester and the role or study context.",
            "Collect qualification, availability, and prior communication evidence.",
            "Determine the next stage and missing information.",
            "Prepare the next-step note, task update, or follow-up.",
            "Confirm status has been updated in the operating system.",
        ),
        skill_title="Tester Pipeline Operator",
        skill_goal="Summarize tester/candidate state and generate the next operational step.",
        skill_steps=(
            "Gather candidate/tester evidence by case, thread, and task.",
            "Extract qualification, status, and missing information.",
            "Draft the next-step checklist or follow-up note.",
            "Flag cases that require human judgment or private data review.",
        ),
        outputs=("Candidate status summary", "Next-step checklist"),
        verification=(
            "The status summary cites current and historical evidence separately.",
            "The next step does not expose private participant data unnecessarily.",
            "Missing qualification data is listed explicitly.",
        ),
        business_value_weight=0.85,
    ),
    _WorkflowSignature(
        key="product_support_qa",
        title="Product Support, QA, And Test Case Triage",
        domain="product_ops",
        keywords=(
            "bug",
            "error",
            "issue",
            "support",
            "test",
            "test case",
            "steps",
            "qa",
            "dashboard",
            "extension",
            "chrome",
            "android",
            "release",
            "string",
            "ui",
            "field",
            "crash",
            "fix",
        ),
        objective="Turn product/support/test-case evidence into a reproducible issue, owner, and verification path.",
        trigger="A thread, ticket, or chat references a bug, test case, support issue, release, extension, or product field.",
        canonical_steps=(
            "Identify the affected product area, user/customer, and symptom.",
            "Collect reproduction steps, screenshots/docs, chat context, and related tickets.",
            "Classify severity, owner, and likely system boundary.",
            "Prepare a concise triage packet and verification checklist.",
            "Record the fix or follow-up outcome when visible.",
        ),
        skill_title="Product Triage Packet Builder",
        skill_goal="Build a reproducible product/support triage packet from scattered evidence.",
        skill_steps=(
            "Gather cited support, chat, ticket, and doc events for the issue.",
            "Extract symptom, affected object, reproduction steps, and observed impact.",
            "Propose owner/severity only when supported by evidence.",
            "Produce a verification checklist tied to the original report.",
        ),
        outputs=("Triage packet", "Reproduction checklist", "Verification checklist"),
        verification=(
            "The packet contains reproduction evidence or marks it missing.",
            "Severity and ownership are evidence-backed or left as open questions.",
            "The verification checklist references the original user-visible symptom.",
        ),
        business_value_weight=0.92,
    ),
    _WorkflowSignature(
        key="delivery_task_execution",
        title="Delivery, Task, And Status Execution",
        domain="execution",
        keywords=(
            "task",
            "clickup",
            "status",
            "update",
            "daily",
            "blocker",
            "blocked",
            "due",
            "urgent",
            "assigned",
            "todo",
            "complete",
            "done",
            "sprint",
        ),
        objective="Keep operating tasks moving by summarizing status, blockers, owners, and next actions.",
        trigger="A task, ticket, or thread includes status updates, blockers, assignments, due dates, or urgent execution work.",
        canonical_steps=(
            "Identify the task, owner, due date, and current status.",
            "Gather recent comments, related threads, and linked documents.",
            "Separate completed work from blockers and open decisions.",
            "Prepare a status update with next actions and owner handoffs.",
            "Verify that the operating system reflects the latest state.",
        ),
        skill_title="Execution Status Synthesizer",
        skill_goal="Produce a concise status/update packet for recurring delivery work.",
        skill_steps=(
            "Gather recent task, comment, mail, and doc evidence.",
            "Extract owner, due date, current state, blockers, and next actions.",
            "Summarize changes since the prior visible update.",
            "Flag stale, ownerless, or blocked work.",
        ),
        outputs=("Status brief", "Blocker list", "Next-action list"),
        verification=(
            "The brief separates done, blocked, and next-action items.",
            "Every blocker is linked to an evidence event.",
            "Ownerless work is surfaced rather than silently assigned.",
        ),
        business_value_weight=0.82,
    ),
    _WorkflowSignature(
        key="finance_commercial_ops",
        title="Finance And Commercial Operations",
        domain="finance",
        keywords=(
            "invoice",
            "payment",
            "budget",
            "quote",
            "price",
            "contract",
            "po",
            "procurement",
            "billing",
            "cost",
        ),
        objective="Resolve commercial or finance operations with evidence of status, ownership, and approval boundary.",
        trigger="A thread references invoices, contracts, budget, payment, billing, quote, or procurement state.",
        canonical_steps=(
            "Identify the commercial object and the requested action.",
            "Collect amount, counterparty, due date, and approval evidence.",
            "Classify whether the next step is read-only, approval-gated, or blocked.",
            "Prepare the finance/commercial summary and missing-evidence list.",
            "Record the follow-up or outcome when visible.",
        ),
        skill_title="Commercial Evidence Summarizer",
        skill_goal="Summarize commercial/finance threads without crossing approval boundaries.",
        skill_steps=(
            "Gather commercial evidence by counterparty, contract, invoice, or thread.",
            "Extract amounts, deadlines, requested action, and approval evidence.",
            "Mark any action that requires approval before execution.",
            "Produce a finance-safe summary and follow-up checklist.",
        ),
        outputs=("Commercial summary", "Approval checklist"),
        verification=(
            "Amounts and deadlines are quoted only from cited evidence.",
            "Payment, contract, or procurement actions remain approval-gated.",
            "Missing approval evidence is a blocker.",
        ),
        business_value_weight=0.88,
    ),
    _WorkflowSignature(
        key="internal_coordination_handoff",
        title="Internal Coordination And Handoff",
        domain="coordination",
        keywords=(
            "meeting",
            "schedule",
            "call",
            "follow up",
            "tomorrow",
            "agenda",
            "sync",
            "notes",
            "action",
            "next step",
            "handoff",
        ),
        objective="Turn internal coordination chatter into owners, decisions, and follow-up actions.",
        trigger="A chat or thread contains meeting, scheduling, handoff, or follow-up signals.",
        canonical_steps=(
            "Identify the coordination topic, participants, and requested follow-up.",
            "Collect related chat, mail, and document context.",
            "Extract decisions, owners, and unresolved questions.",
            "Prepare a follow-up note or task list.",
            "Mark low-signal chatter as non-actionable when no work is visible.",
        ),
        skill_title="Thread-To-Action Distiller",
        skill_goal="Distill coordination threads into evidence-backed actions and open questions.",
        skill_steps=(
            "Collect the relevant thread events and nearby cited context.",
            "Extract decisions, action items, owners, deadlines, and open questions.",
            "Suppress greetings and acknowledgements unless they carry commitments.",
            "Produce a concise follow-up note with citations.",
        ),
        outputs=("Action summary", "Open-question list"),
        verification=(
            "Greetings and acknowledgements are not treated as workflow evidence by themselves.",
            "Every action item names the source event that supports it.",
            "Unclear ownership is listed as an open question.",
        ),
        business_value_weight=0.65,
    ),
)

_SIGNATURE_BY_KEY = {signature.key: signature for signature in _SIGNATURES}
_SIGNATURE_PRIORITY = {
    signature.key: priority for priority, signature in enumerate(_SIGNATURES)
}
_TOKEN_SIGNATURE_INDEX: dict[str, list[str]] = defaultdict(list)
_PHRASE_SIGNATURE_INDEX: list[tuple[str, str]] = []
for _signature in _SIGNATURES:
    for _keyword in _signature.keywords:
        _normalized_keyword = _keyword.lower()
        if " " in _normalized_keyword:
            _PHRASE_SIGNATURE_INDEX.append((_normalized_keyword, _signature.key))
        else:
            _TOKEN_SIGNATURE_INDEX[_normalized_keyword].append(_signature.key)
_GENERIC_SIGNATURE = _WorkflowSignature(
    key="general_repeatable_work",
    title="General Repeatable Work",
    domain="general_ops",
    keywords=(),
    objective="Capture recurring company work that does not yet fit a named workflow family.",
    trigger="Multiple cases share similar terms, actors, or source-system patterns.",
    canonical_steps=(
        "Identify the recurring case pattern and source systems.",
        "Gather cited examples and actors.",
        "Extract the recurring objective, handoffs, and visible outcomes.",
        "List missing context before turning the pattern into an operational skill.",
    ),
    skill_title="Recurring Work Pattern Summarizer",
    skill_goal="Summarize a recurring but not-yet-classified company work pattern.",
    skill_steps=(
        "Gather cited examples for the recurring pattern.",
        "Extract common actors, systems, trigger language, and outcomes.",
        "Name the likely workflow boundary and open questions.",
        "Produce an evidence-backed summary for future promotion.",
    ),
    outputs=("Recurring pattern brief", "Open questions"),
    verification=(
        "The pattern cites multiple cases or explains why a single case is high-value.",
        "The skill records what is unknown instead of inventing a workflow boundary.",
    ),
    business_value_weight=0.45,
)

_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "but",
    "can",
    "com",
    "content",
    "email",
    "for",
    "from",
    "has",
    "have",
    "html",
    "http",
    "https",
    "image",
    "into",
    "more",
    "need",
    "not",
    "only",
    "our",
    "please",
    "re",
    "that",
    "the",
    "their",
    "there",
    "this",
    "text",
    "to",
    "with",
    "www",
    "you",
    "your",
}
_CHAT_NOISE = {
    "hi",
    "hello",
    "hey",
    "thanks",
    "thank",
    "sure",
    "ok",
    "okay",
    "yes",
    "no",
    "yep",
    "great",
    "cool",
    "done",
    "fine",
}


def discover_company_workflows_and_skills(
    sources: Sequence[DiscoverySourceInput],
    *,
    company_name: str = "",
    company_domain: str = "",
    aliases: Sequence[str] | None = None,
    output: str | Path | None = None,
    workflow_limit: int = 25,
    skill_limit: int = 12,
) -> DiscoveryResult:
    """Discover repeatable workflows and generate company skills from history.

    This is intentionally autonomous: callers provide evidence sources, not
    workflow labels or skill definitions. Generated artifacts remain descriptive
    and evidence-backed; live activation stays a separate governance boundary.
    """

    if not sources:
        raise ValueError("at least one discovery source is required")
    loaded_sources = [_load_source(source) for source in sources]
    lineage = _build_lineage(
        loaded_sources,
        company_name=company_name,
        company_domain=company_domain,
        aliases=list(aliases or []),
    )
    rows, rejected_count = _combined_rows(loaded_sources)
    workflows = _discover_workflow_families(
        rows,
        lineage=lineage,
        limit=workflow_limit,
    )
    skills = _generate_skills(
        workflows,
        lineage=lineage,
        limit=skill_limit,
    )
    workflows_by_id = {workflow.workflow_id: workflow for workflow in workflows}
    for skill in skills:
        for workflow_id in skill.supported_workflow_ids:
            workflow = workflows_by_id.get(workflow_id)
            if workflow is None:
                continue
            workflow.supported_skill_ids.append(skill.skill_id)

    generated_at = _now_iso()
    skill_map = GeneratedSkillMap(
        organization_name=lineage.canonical_name,
        organization_domain=lineage.canonical_domain,
        generated_at=generated_at,
        source_ref="company_lineage",
        skill_count=len(skills),
        skills=skills,
        metadata={
            "aliases": lineage.aliases,
            "backend": "deterministic_semantic_v1",
        },
    )
    manifest = DiscoveryManifest(
        generated_at=generated_at,
        workflow_count=len(workflows),
        skill_count=len(skills),
        source_count=len(lineage.sources),
        event_count=lineage.event_count,
        rejected_event_count=rejected_count,
        caveats=_caveats(lineage=lineage, workflows=workflows),
        metadata={
            "workflow_limit": workflow_limit,
            "skill_limit": skill_limit,
            "no_supervision": True,
            "activation_boundary": (
                "Generated skills are draft artifacts. Live-system action remains "
                "approval-gated outside this discovery pass."
            ),
        },
    )
    result = DiscoveryResult(
        lineage=lineage,
        workflows=workflows,
        skill_map=skill_map,
        manifest=manifest,
    )
    if output is not None:
        result = write_discovery_outputs(result, output)
    return result


def write_discovery_outputs(
    result: DiscoveryResult, output: str | Path
) -> DiscoveryResult:
    output_dir = Path(output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / COMPANY_LINEAGE_FILE).write_text(
        result.lineage.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / WORKFLOW_FAMILIES_FILE).write_text(
        json.dumps(
            [workflow.model_dump(mode="json") for workflow in result.workflows],
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / WORKFLOW_FAMILIES_MARKDOWN_FILE).write_text(
        render_workflow_families_markdown(result),
        encoding="utf-8",
    )
    skills_dir = output_dir / "skills"
    skills_dir.mkdir(parents=True, exist_ok=True)
    current_skill_slugs = {_skill_name(skill) for skill in result.skill_map.skills}
    for existing_skill_dir in skills_dir.iterdir():
        if (
            existing_skill_dir.is_dir()
            and existing_skill_dir.name not in current_skill_slugs
            and (existing_skill_dir / "SKILL.md").is_file()
        ):
            shutil.rmtree(existing_skill_dir)
    updated_skills: list[GeneratedSkill] = []
    for skill in result.skill_map.skills:
        slug = _skill_name(skill)
        skill_dir = skills_dir / slug
        skill_dir.mkdir(parents=True, exist_ok=True)
        skill_path = skill_dir / "SKILL.md"
        skill_with_path = skill.model_copy(
            update={"skill_path": str(skill_path)},
            deep=True,
        )
        skill_path.write_text(render_skill_markdown(skill_with_path), encoding="utf-8")
        updated_skills.append(skill_with_path)
    skill_map = result.skill_map.model_copy(
        update={"skills": updated_skills, "skill_count": len(updated_skills)},
        deep=True,
    )
    (output_dir / SKILL_MAP_FILE).write_text(
        skill_map.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / SKILL_MAP_MARKDOWN_FILE).write_text(
        render_skill_map_markdown(skill_map),
        encoding="utf-8",
    )
    manifest = result.manifest.model_copy(
        update={
            "metadata": {
                **result.manifest.metadata,
                "files": [
                    COMPANY_LINEAGE_FILE,
                    WORKFLOW_FAMILIES_FILE,
                    WORKFLOW_FAMILIES_MARKDOWN_FILE,
                    SKILL_MAP_FILE,
                    SKILL_MAP_MARKDOWN_FILE,
                    DISCOVERY_MANIFEST_FILE,
                    "skills/*/SKILL.md",
                ],
            }
        },
        deep=True,
    )
    (output_dir / DISCOVERY_MANIFEST_FILE).write_text(
        manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return result.model_copy(
        update={
            "skill_map": skill_map,
            "manifest": manifest,
            "output_dir": str(output_dir),
        },
        deep=True,
    )


def render_workflow_families_markdown(result: DiscoveryResult) -> str:
    lines = [
        f"# {result.lineage.canonical_name} Workflow Families",
        "",
        f"- Events: {result.lineage.event_count}",
        f"- Cases: {result.lineage.case_count}",
        f"- Sources: {len(result.lineage.sources)}",
        "",
    ]
    for workflow in result.workflows:
        lines.extend(
            [
                f"## {workflow.title}",
                "",
                workflow.objective,
                "",
                f"- Repeatability: {workflow.repetition_score:.2f}",
                f"- Confidence: {workflow.confidence:.2f}",
                f"- Cases: {workflow.case_count}",
                f"- Events: {workflow.event_count}",
                f"- Surfaces: {', '.join(workflow.surfaces) or 'unknown'}",
                f"- Source roles: {', '.join(role.value for role in workflow.source_roles)}",
                "",
                "### Layout",
                "",
            ]
        )
        for idx, step in enumerate(workflow.canonical_steps, start=1):
            lines.append(f"{idx}. {step}")
        lines.extend(["", "### Evidence", ""])
        for ref in workflow.evidence_refs[:5]:
            label = _evidence_label(ref)
            lines.append(
                f"- `{ref.event_id}` ({ref.source_role.value}, {ref.surface}, {ref.provider}): {label}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_skill_map_markdown(skill_map: GeneratedSkillMap) -> str:
    lines = [
        f"# {skill_map.organization_name} Generated Skills",
        "",
        f"- Skills: {skill_map.skill_count}",
        f"- Generated at: {skill_map.generated_at}",
        "",
    ]
    for skill in skill_map.skills:
        lines.extend(
            [
                f"## {skill.title}",
                "",
                skill.summary,
                "",
                f"- Usefulness: {skill.usefulness_score:.2f}",
                f"- Confidence: {skill.confidence:.2f}",
                f"- Activation: {skill.activation}",
                (
                    f"- Path: `{skill.skill_path}`"
                    if skill.skill_path
                    else "- Path: not written"
                ),
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_skill_markdown(skill: GeneratedSkill) -> str:
    description = _skill_description(skill)
    lines = [
        "---",
        f"name: {json.dumps(_skill_name(skill))}",
        f"description: {json.dumps(description)}",
        "---",
        "",
        f"# {skill.title}",
        "",
        skill.summary,
        "",
        "## Goal",
        "",
        skill.goal,
        "",
        "## Prerequisites",
        "",
    ]
    lines.extend(f"- {item}" for item in skill.prerequisites)
    lines.extend(["", "## Procedure", ""])
    lines.extend(f"{idx}. {step}" for idx, step in enumerate(skill.steps, start=1))
    lines.extend(["", "## Outputs", ""])
    lines.extend(f"- {item}" for item in skill.output_artifacts)
    lines.extend(["", "## Verification", ""])
    lines.extend(f"- {item}" for item in skill.verification_checks)
    lines.extend(["", "## Boundaries", ""])
    lines.extend(f"- Allowed: {item}" for item in skill.allowed_actions)
    lines.extend(f"- Blocked: {item}" for item in skill.blocked_actions)
    lines.extend(["", "## Evidence", ""])
    for ref in skill.evidence_refs[:8]:
        label = _evidence_label(ref)
        lines.append(
            f"- `{ref.event_id}` ({ref.source_role.value}, {ref.surface}, {ref.provider}): {label}"
        )
    return "\n".join(lines).rstrip() + "\n"


def _load_source(source_input: DiscoverySourceInput) -> _LoadedSource:
    path = Path(source_input.path).expanduser().resolve()
    bundle = _load_or_build_bundle(path)
    source_id = _stable_id(source_input.role.value, path, prefix="src")
    source = CompanyLineageSource(
        source_id=source_id,
        path=str(path),
        role=source_input.role,
        label=source_input.label,
        organization_name=bundle.index.organization_name,
        organization_domain=bundle.index.organization_domain,
        captured_at=bundle.index.captured_at,
        source_providers=bundle.index.source_providers,
        event_count=bundle.index.event_count,
        case_count=bundle.index.case_count,
        surface_counts=bundle.index.surface_counts,
    )
    return _LoadedSource(source=source, bundle=bundle)


def _load_or_build_bundle(path: Path) -> CanonicalHistoryBundle:
    paths = canonical_history_paths(path)
    if paths.index_path.is_file():
        index = CanonicalHistoryIndex.model_validate_json(
            paths.index_path.read_text(encoding="utf-8")
        )
        return CanonicalHistoryBundle(paths=paths, events=[], index=index)
    if not paths.snapshot_path.is_file():
        raise FileNotFoundError(f"context_snapshot.json not found at {path}")
    snapshot = ContextSnapshot.model_validate_json(
        paths.snapshot_path.read_text(encoding="utf-8")
    )
    bundle = build_canonical_history_bundle(snapshot)
    return bundle.model_copy(update={"paths": paths}, deep=True)


def _build_lineage(
    loaded_sources: Sequence[_LoadedSource],
    *,
    company_name: str,
    company_domain: str,
    aliases: list[str],
) -> CompanyLineage:
    preferred_source = _preferred_identity_source(loaded_sources)
    canonical_name = company_name or preferred_source.source.organization_name
    canonical_domain = company_domain or preferred_source.source.organization_domain
    alias_set = {
        alias.strip()
        for alias in aliases
        if alias.strip() and alias.strip() != canonical_name
    }
    for loaded in loaded_sources:
        name = loaded.source.organization_name.strip()
        domain = loaded.source.organization_domain.strip()
        if name and name != canonical_name:
            alias_set.add(name)
        if domain and domain != canonical_domain:
            alias_set.add(domain)
    source_providers = sorted(
        {
            provider
            for loaded in loaded_sources
            for provider in loaded.source.source_providers
            if provider
        }
    )
    surface_counts: dict[str, int] = {}
    for loaded in loaded_sources:
        for surface, count in loaded.source.surface_counts.items():
            surface_counts[surface] = surface_counts.get(surface, 0) + count
    case_ids = {
        row.case_id
        for loaded in loaded_sources
        for row in loaded.bundle.index.rows
        if row.case_id
    }
    return CompanyLineage(
        canonical_name=canonical_name or "Unknown Company",
        canonical_domain=canonical_domain,
        aliases=sorted(alias_set),
        sources=[loaded.source for loaded in loaded_sources],
        source_providers=source_providers,
        event_count=sum(loaded.source.event_count for loaded in loaded_sources),
        case_count=len(case_ids),
        surface_counts=surface_counts,
        metadata={
            "lineage_version": "company_lineage_v1",
            "identity_rule": "explicit override, then live source, then last source",
        },
    )


def _preferred_identity_source(
    loaded_sources: Sequence[_LoadedSource],
) -> _LoadedSource:
    live_sources = [
        loaded
        for loaded in loaded_sources
        if loaded.source.role == DiscoverySourceRole.LIVE
    ]
    return live_sources[-1] if live_sources else loaded_sources[-1]


def _combined_rows(
    loaded_sources: Sequence[_LoadedSource],
) -> tuple[list[_AnnotatedRow], int]:
    seen_event_ids: set[str] = set()
    rows: list[_AnnotatedRow] = []
    rejected_count = 0
    for loaded in loaded_sources:
        for row in loaded.bundle.index.rows:
            if row.event_id in seen_event_ids:
                rejected_count += 1
                continue
            seen_event_ids.add(row.event_id)
            annotated = _AnnotatedRow(
                row=row,
                source_id=loaded.source.source_id,
                source_role=loaded.source.role,
                source_label=loaded.source.label,
            )
            if _is_noise_row(annotated):
                rejected_count += 1
                continue
            rows.append(annotated)
    rows.sort(key=lambda item: (item.row.ts_ms, item.row.event_id))
    return rows, rejected_count


def _discover_workflow_families(
    rows: Sequence[_AnnotatedRow],
    *,
    lineage: CompanyLineage,
    limit: int,
) -> list[WorkflowFamily]:
    grouped: dict[str, list[_AnnotatedRow]] = defaultdict(list)
    for annotated in rows:
        grouped[_signature_key_for_row(annotated)].append(annotated)

    families: list[WorkflowFamily] = []
    for key, family_rows in grouped.items():
        if len(family_rows) < 2:
            continue
        signature = _signature_for_key(key)
        if signature.key.startswith("general:") and not _accept_generic_family(
            family_rows
        ):
            continue
        family = _workflow_family_from_rows(
            signature=signature,
            rows=family_rows,
            lineage=lineage,
        )
        if family.case_count < 2 and family.event_count < 6:
            continue
        families.append(family)

    families.sort(
        key=lambda item: (
            item.repetition_score,
            item.business_value_score,
            item.confidence,
            item.event_count,
        ),
        reverse=True,
    )
    return families[: max(1, limit)]


def _accept_generic_family(rows: Sequence[_AnnotatedRow]) -> bool:
    # Generic token clusters are useful as diagnostics, but they are not good
    # enough to publish as "best workflows" without an LLM or reviewer naming
    # pass. Keep v1 precise: emit named semantic families only.
    del rows
    return False


def _signature_for_key(key: str) -> _WorkflowSignature:
    signature = _SIGNATURE_BY_KEY.get(key)
    if signature is not None:
        return signature
    if key.startswith("general:"):
        label = key.split(":", 1)[1].replace("_", " ").strip()
        if label:
            display = label.title()
            return _WorkflowSignature(
                key=key,
                title=f"Recurring {display} Work",
                domain=_GENERIC_SIGNATURE.domain,
                keywords=(),
                objective=(
                    "Capture recurring company work around "
                    f"{label.replace('_', ' ')} that does not yet fit a named family."
                ),
                trigger=_GENERIC_SIGNATURE.trigger,
                canonical_steps=_GENERIC_SIGNATURE.canonical_steps,
                skill_title=_GENERIC_SIGNATURE.skill_title,
                skill_goal=_GENERIC_SIGNATURE.skill_goal,
                skill_steps=_GENERIC_SIGNATURE.skill_steps,
                outputs=_GENERIC_SIGNATURE.outputs,
                verification=_GENERIC_SIGNATURE.verification,
                business_value_weight=_GENERIC_SIGNATURE.business_value_weight,
            )
    return _GENERIC_SIGNATURE


def _workflow_family_from_rows(
    *,
    signature: _WorkflowSignature,
    rows: Sequence[_AnnotatedRow],
    lineage: CompanyLineage,
) -> WorkflowFamily:
    case_ids = _case_ids(rows)
    surfaces = sorted({item.row.surface for item in rows if item.row.surface})
    providers = sorted({item.row.provider for item in rows if item.row.provider})
    source_ids = sorted({item.source_id for item in rows})
    source_roles = sorted(
        {item.source_role for item in rows}, key=lambda role: role.value
    )
    actor_ids = _top_values(
        (item.row.actor_id for item in rows if item.row.actor_id), 12
    )
    examples = _workflow_examples(rows, signature=signature)
    evidence_refs = [
        _evidence_ref(item)
        for item in _representative_rows(rows, signature=signature, limit=10)
    ]
    repetition_score = _repetition_score(
        rows=rows,
        case_count=len(case_ids),
        surface_count=len(surfaces),
        source_role_count=len(source_roles),
    )
    business_value_score = round(
        min(
            1.0,
            signature.business_value_weight + (0.05 if len(source_roles) > 1 else 0.0),
        ),
        4,
    )
    confidence = round(
        min(
            1.0,
            0.25
            + (0.35 * min(1.0, len(case_ids) / 8.0))
            + (0.2 * min(1.0, len(rows) / 40.0))
            + (0.1 * min(1.0, len(surfaces) / 3.0))
            + (0.1 if len(source_roles) > 1 else 0.0),
        ),
        4,
    )
    workflow_id = _stable_id(
        lineage.canonical_domain,
        signature.key,
        sorted(case_ids)[:20],
        prefix="wfam",
    )
    variants = _variants(rows, signature=signature)
    return WorkflowFamily(
        workflow_id=workflow_id,
        title=signature.title,
        objective=signature.objective,
        trigger=signature.trigger,
        domain=signature.domain,
        canonical_steps=list(signature.canonical_steps),
        variants=variants,
        source_roles=source_roles,
        source_ids=source_ids,
        source_providers=providers,
        surfaces=surfaces,
        actor_ids=actor_ids,
        case_count=len(case_ids),
        event_count=len(rows),
        repetition_score=repetition_score,
        business_value_score=business_value_score,
        confidence=confidence,
        examples=examples,
        evidence_refs=evidence_refs,
        metadata={
            "signature_key": signature.key,
            "lineage_company": lineage.canonical_name,
            "case_ids_sample": sorted(case_ids)[:20],
        },
    )


def _generate_skills(
    workflows: Sequence[WorkflowFamily],
    *,
    lineage: CompanyLineage,
    limit: int,
) -> list[GeneratedSkill]:
    skills: list[GeneratedSkill] = []
    seen_titles: set[str] = set()
    for workflow in workflows:
        signature = _signature_for_key(
            str(workflow.metadata.get("signature_key") or "")
        )
        if signature.skill_title in seen_titles:
            continue
        seen_titles.add(signature.skill_title)
        evidence_refs = workflow.evidence_refs[:8]
        usefulness_score = round(
            min(
                1.0,
                0.35 * workflow.repetition_score
                + 0.35 * workflow.business_value_score
                + 0.2 * workflow.confidence
                + 0.1 * min(1.0, workflow.case_count / 12.0),
            ),
            4,
        )
        skill_id = _stable_id(
            lineage.canonical_domain,
            signature.key,
            workflow.workflow_id,
            prefix="skill",
        )
        skills.append(
            GeneratedSkill(
                skill_id=skill_id,
                title=signature.skill_title,
                summary=(
                    f"Generated from the discovered workflow family "
                    f"`{workflow.title}` across {workflow.case_count} case(s) "
                    f"and {workflow.event_count} event(s)."
                ),
                activation=signature.trigger[0].lower() + signature.trigger[1:],
                goal=signature.skill_goal,
                supported_workflow_ids=[workflow.workflow_id],
                prerequisites=[
                    "Canonical event evidence for the active case or thread.",
                    "Current source-system status before any live action.",
                    "Human or policy approval for external writes or commitments.",
                ],
                steps=list(signature.skill_steps),
                output_artifacts=list(signature.outputs),
                verification_checks=list(signature.verification),
                allowed_actions=[
                    "Read and summarize cited company evidence.",
                    "Draft internal briefs, checklists, and review notes.",
                    "Flag missing context and approval boundaries.",
                ],
                blocked_actions=[
                    "Send external messages without approval.",
                    "Change source-system state without an explicit activation policy.",
                    "Infer consent, approval, amounts, deadlines, or commitments without evidence.",
                ],
                usefulness_score=usefulness_score,
                confidence=workflow.confidence,
                evidence_refs=evidence_refs,
                tags=[workflow.domain, "generated", "company_lineage"],
                metadata={
                    "generated_by": "company_discovery",
                    "workflow_title": workflow.title,
                    "source_roles": [role.value for role in workflow.source_roles],
                },
            )
        )
        if len(skills) >= limit:
            break
    return skills


def _workflow_examples(
    rows: Sequence[_AnnotatedRow],
    *,
    signature: _WorkflowSignature,
) -> list[WorkflowExample]:
    case_groups: dict[str, list[_AnnotatedRow]] = defaultdict(list)
    for item in rows:
        case_groups[_row_case_key(item.row)].append(item)
    scored_groups = sorted(
        case_groups.items(),
        key=lambda entry: (len(entry[1]), max(item.row.ts_ms for item in entry[1])),
        reverse=True,
    )
    examples: list[WorkflowExample] = []
    for case_id, group_rows in scored_groups[:5]:
        group_rows = sorted(
            group_rows, key=lambda item: (item.row.ts_ms, item.row.event_id)
        )
        examples.append(
            WorkflowExample(
                example_id=_stable_id(signature.key, case_id, prefix="wex"),
                case_id=case_id,
                title=_best_title(group_rows),
                source_id=group_rows[-1].source_id,
                source_role=group_rows[-1].source_role,
                event_ids=[item.row.event_id for item in group_rows[:20]],
                surfaces=sorted(
                    {item.row.surface for item in group_rows if item.row.surface}
                ),
                start_ts_ms=min((item.row.ts_ms for item in group_rows), default=0),
                end_ts_ms=max((item.row.ts_ms for item in group_rows), default=0),
                evidence_refs=[
                    _evidence_ref(item)
                    for item in _ranked_rows(group_rows, signature=signature)[:5]
                ],
            )
        )
    return examples


def _representative_rows(
    rows: Sequence[_AnnotatedRow],
    *,
    signature: _WorkflowSignature,
    limit: int,
) -> list[_AnnotatedRow]:
    selected: list[_AnnotatedRow] = []
    seen_cases: set[str] = set()

    ranked_rows = _ranked_rows(rows, signature=signature)
    for role in (
        DiscoverySourceRole.LIVE,
        DiscoverySourceRole.HISTORICAL,
        DiscoverySourceRole.SNAPSHOT,
    ):
        for item in ranked_rows:
            if item.source_role != role:
                continue
            if _row_signal_score(item, signature=signature) <= 0:
                continue
            case_id = _row_case_key(item.row)
            if case_id in seen_cases:
                continue
            seen_cases.add(case_id)
            selected.append(item)
            break
        if len(selected) >= limit:
            return selected

    for item in ranked_rows:
        case_id = _row_case_key(item.row)
        if case_id in seen_cases:
            continue
        seen_cases.add(case_id)
        selected.append(item)
        if len(selected) >= limit:
            return selected
    return selected


def _ranked_rows(
    rows: Sequence[_AnnotatedRow],
    *,
    signature: _WorkflowSignature,
) -> list[_AnnotatedRow]:
    return sorted(
        rows,
        key=lambda item: (
            _row_signal_score(item, signature=signature),
            item.row.ts_ms,
            item.row.event_id,
        ),
        reverse=True,
    )


def _signature_key_for_row(item: _AnnotatedRow) -> str:
    text = _searchable_text(item.row)
    score_by_key: dict[str, int] = defaultdict(int)
    for token in set(_keyword_tokens(text)):
        for signature_key in _TOKEN_SIGNATURE_INDEX.get(token, []):
            score_by_key[signature_key] += 2
    for phrase, signature_key in _PHRASE_SIGNATURE_INDEX:
        if phrase in text:
            score_by_key[signature_key] += 3
    scores = [
        (score, -_SIGNATURE_PRIORITY.get(signature_key, 999), signature_key)
        for signature_key, score in score_by_key.items()
        if score > 0
    ]
    if scores:
        scores.sort(reverse=True)
        return scores[0][2]
    generic_text = " ".join([item.row.subject, item.row.snippet]).lower()
    tokens = _tokens(generic_text)
    if not tokens:
        return _GENERIC_SIGNATURE.key
    top = "_".join(tokens[:2])
    return f"general:{top}"


def _row_signal_score(
    item: _AnnotatedRow,
    *,
    signature: _WorkflowSignature,
) -> float:
    searchable_text = _searchable_text(item.row)
    body_text = " ".join([item.row.subject, item.row.snippet]).strip().lower()
    body_tokens = _tokens(body_text)
    score = float(_signature_match_score(searchable_text, signature=signature))
    if len(body_tokens) >= 12:
        score += 1.2
    elif len(body_tokens) >= 6:
        score += 0.8
    elif len(body_tokens) >= 3:
        score += 0.25
    if item.row.surface in {"docs", "tickets"}:
        score += 0.6
    if item.source_role == DiscoverySourceRole.LIVE:
        score += 0.2
    if _is_low_signal_evidence_text(body_text) and score < 4:
        score -= 2.0
    if "http://" in body_text or "https://" in body_text:
        score -= 0.5
    return score


def _signature_match_score(
    text: str,
    *,
    signature: _WorkflowSignature,
) -> int:
    score = 0
    keyword_tokens = set(_keyword_tokens(text))
    for keyword in signature.keywords:
        normalized = keyword.lower()
        if " " in normalized:
            if normalized in text:
                score += 3
        elif normalized in keyword_tokens:
            score += 2
    return score


def _is_noise_row(item: _AnnotatedRow) -> bool:
    body_text = " ".join([item.row.subject, item.row.snippet]).lower()
    body_tokens = _tokens(body_text)
    if not body_tokens:
        return True
    if len(body_tokens) <= 2 and set(body_tokens).issubset(_CHAT_NOISE):
        return True
    text = _searchable_text(item.row)
    tokens = _tokens(text)
    if not tokens:
        return True
    if len(tokens) <= 2 and set(tokens).issubset(_CHAT_NOISE):
        return True
    if len(text) <= 8 and set(tokens).issubset(_CHAT_NOISE):
        return True
    if item.row.surface in {"teams", "slack"} and set(tokens).issubset(_CHAT_NOISE):
        return True
    return False


def _is_low_signal_evidence_text(text: str) -> bool:
    stripped = _strip_reply_prefix(text.strip().lower())
    if not stripped:
        return True
    if re.match(r"^(hi|hello|hey|dear)\b[,\s]+", stripped):
        return True
    if stripped in {"hi", "hello", "hey", "dear"}:
        return True
    tokens = _tokens(stripped)
    if len(tokens) <= 2 and set(tokens).issubset(_CHAT_NOISE):
        return True
    if stripped.startswith(("http://", "https://")):
        return True
    return False


def _strip_reply_prefix(text: str) -> str:
    stripped = text.strip()
    while True:
        updated = re.sub(r"^(re|fw|fwd)\s*:\s*", "", stripped, flags=re.IGNORECASE)
        if updated == stripped:
            return stripped
        stripped = updated.strip()


def _searchable_text(row: CanonicalHistoryIndexRow) -> str:
    parts = [
        row.subject,
        row.normalized_subject,
        row.snippet,
        " ".join(row.search_terms),
        str(row.metadata.get("title", "")),
    ]
    return " ".join(part for part in parts if part).lower()


def _tokens(text: str) -> list[str]:
    counts = Counter(
        token
        for token in re.findall(r"[a-z0-9]+", text.lower())
        if len(token) > 2 and token not in _STOPWORDS and token not in _CHAT_NOISE
    )
    return [token for token, _count in counts.most_common(8)]


def _keyword_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        if len(token) < 2 or token in _STOPWORDS:
            continue
        tokens.append(token)
        if len(token) > 3 and token.endswith("s"):
            tokens.append(token[:-1])
    return tokens


def _case_ids(rows: Sequence[_AnnotatedRow]) -> set[str]:
    return {_row_case_key(item.row) for item in rows if _row_case_key(item.row)}


def _row_case_key(row: CanonicalHistoryIndexRow) -> str:
    return row.case_id or row.thread_ref or row.conversation_anchor or row.event_id


def _evidence_ref(item: _AnnotatedRow) -> DiscoveryEvidenceRef:
    return DiscoveryEvidenceRef(
        event_id=item.row.event_id,
        source_id=item.source_id,
        source_role=item.source_role,
        case_id=_row_case_key(item.row),
        timestamp=item.row.timestamp,
        ts_ms=item.row.ts_ms,
        surface=item.row.surface,
        provider=item.row.provider,
        kind=item.row.kind,
        actor_id=item.row.actor_id,
        subject=item.row.subject,
        snippet=_trim(item.row.snippet, 240),
    )


def _evidence_label(ref: DiscoveryEvidenceRef) -> str:
    for candidate in (ref.subject, ref.snippet, ref.event_id):
        label = _clean_evidence_label_candidate(candidate)
        if label and not _is_low_signal_evidence_text(label):
            return label
    return ref.event_id


def _clean_evidence_label_candidate(value: str) -> str:
    label = _strip_reply_prefix(_trim(value, 220))
    label = re.sub(
        r"^(hi|hello|hey|dear)\b[^,:\n]{0,80}[,:]\s*",
        "",
        label,
        flags=re.IGNORECASE,
    ).strip()
    label = re.sub(r"^[\s{\[\(\"']+", "", label).strip()
    return _trim(label, 180)


def _repetition_score(
    *,
    rows: Sequence[_AnnotatedRow],
    case_count: int,
    surface_count: int,
    source_role_count: int,
) -> float:
    return round(
        min(
            1.0,
            0.45 * min(1.0, case_count / 12.0)
            + 0.25 * min(1.0, len(rows) / 80.0)
            + 0.2 * min(1.0, surface_count / 3.0)
            + (0.1 if source_role_count > 1 else 0.0),
        ),
        4,
    )


def _variants(
    rows: Sequence[_AnnotatedRow],
    *,
    signature: _WorkflowSignature,
) -> list[str]:
    surfaces = sorted({item.row.surface for item in rows if item.row.surface})
    variants = [f"{signature.title} on {surface}" for surface in surfaces[:4]]
    role_names = {
        item.source_role.value
        for item in rows
        if item.source_role
        in {DiscoverySourceRole.HISTORICAL, DiscoverySourceRole.LIVE}
    }
    if len(role_names) > 1:
        variants.append("Historical pattern with live-company continuation")
    return variants


def _best_title(rows: Sequence[_AnnotatedRow]) -> str:
    titles = [
        item.row.subject.strip()
        for item in rows
        if item.row.subject.strip() and len(item.row.subject.strip()) > 2
    ]
    if not titles:
        snippets = [
            item.row.snippet.strip()
            for item in rows
            if item.row.snippet.strip() and len(item.row.snippet.strip()) > 2
        ]
        titles = snippets
    if not titles:
        return "Observed example"
    return Counter(titles).most_common(1)[0][0]


def _top_values(values: Iterable[str], limit: int) -> list[str]:
    counts = Counter(value for value in values if value)
    return [value for value, _count in counts.most_common(limit)]


def _caveats(
    *,
    lineage: CompanyLineage,
    workflows: Sequence[WorkflowFamily],
) -> list[str]:
    caveats: list[str] = []
    roles = {source.role for source in lineage.sources}
    if DiscoverySourceRole.HISTORICAL not in roles:
        caveats.append(
            "No historical source was provided; durability may be overstated."
        )
    if DiscoverySourceRole.LIVE not in roles:
        caveats.append(
            "No live source was provided; recency-sensitive skills may be stale."
        )
    if not workflows:
        caveats.append("No repeatable workflows passed the minimum evidence threshold.")
    if lineage.event_count < 100:
        caveats.append("Small event volume; treat rankings as directional.")
    return caveats


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _stable_id(*parts: object, prefix: str) -> str:
    payload = json.dumps([str(part) for part in parts], sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:80]


def _skill_name(skill: GeneratedSkill) -> str:
    slug = _slugify(skill.title) or _slugify(skill.skill_id) or "generated-skill"
    return slug[:64].strip("-") or "generated-skill"


def _skill_description(skill: GeneratedSkill) -> str:
    return _trim(
        f"{skill.summary} Use when {skill.activation}",
        900,
    )


def _trim(value: str, limit: int) -> str:
    stripped = " ".join(str(value or "").split())
    if len(stripped) <= limit:
        return stripped
    return stripped[: max(0, limit - 3)].rstrip() + "..."
