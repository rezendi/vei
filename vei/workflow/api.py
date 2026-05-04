from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from vei.contract.api import ContractSpec
from vei.events.api import (
    CanonicalEvent,
    canonical_event_paths,
    load_canonical_events_jsonl,
)
from vei.scenario_engine.api import WorkflowScenarioSpec

from .models import (
    BusinessTaskSpec,
    BusinessTaskStatus,
    EvaluationLevel,
    WorkflowCandidate,
    WorkflowEnvironmentPackageManifest,
    WorkflowEvidenceRef,
    WorkflowLabel,
    WorkflowLabelKind,
    WorkflowMiningResult,
    WorkflowObservedExample,
    WorkflowReferencePath,
    WorkflowRefreshReport,
    evaluation_level_at_least,
)

MINING_RESULT_FILE = "workflow_candidates.json"
LABELS_FILE = "workflow_labels.json"
REFRESH_REPORT_FILE = "workflow_refresh_report.json"

_BOUNDARY_EXPORTS = (
    BusinessTaskSpec,
    EvaluationLevel,
    WorkflowCandidate,
    WorkflowMiningResult,
)

_ESCALATION_TERMS = {
    "approval",
    "approve",
    "blocked",
    "blocker",
    "customer",
    "deadline",
    "escalate",
    "escalation",
    "legal",
    "risk",
    "urgent",
}

_BUSINESS_TERMS = {
    "application",
    "contract",
    "customer",
    "deal",
    "demo",
    "intro",
    "introduction",
    "invoice",
    "partner",
    "pilot",
    "proposal",
    "sales",
    "vendor",
}

_PATTERN_STOPWORDS = {
    "about",
    "after",
    "also",
    "from",
    "have",
    "into",
    "more",
    "need",
    "only",
    "over",
    "please",
    "re",
    "that",
    "their",
    "there",
    "this",
    "with",
    "your",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_id(*parts: object, prefix: str) -> str:
    payload = json.dumps([str(part) for part in parts], sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _stable_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _resolve_source(source_dir: str | Path) -> tuple[Path, Path]:
    path = Path(source_dir).expanduser().resolve()
    if path.is_file():
        return path.parent, path
    context_path = path / "context_snapshot.json"
    if context_path.is_file():
        return path, context_path
    workspace_context_path = path / "workspace" / "context_snapshot.json"
    if workspace_context_path.is_file():
        return workspace_context_path.parent, workspace_context_path
    raise FileNotFoundError(f"context_snapshot.json not found at {path}")


def _load_context_payload(context_path: Path) -> dict[str, Any]:
    return json.loads(context_path.read_text(encoding="utf-8"))


def _company_from_context(payload: dict[str, Any]) -> tuple[str, str]:
    company_name = str(
        payload.get("organization_name")
        or payload.get("company_name")
        or payload.get("org")
        or ""
    )
    company_domain = str(
        payload.get("organization_domain")
        or payload.get("company_domain")
        or payload.get("domain")
        or ""
    )
    return company_name, company_domain


def _load_events(root: Path) -> list[CanonicalEvent]:
    paths = canonical_event_paths(root)
    if not paths and (root / "canonical_events.jsonl").is_file():
        paths = [root / "canonical_events.jsonl"]
    events: list[CanonicalEvent] = []
    for path in paths:
        events.extend(load_canonical_events_jsonl(path))
    return events


def _delta_data(event: CanonicalEvent) -> dict[str, Any]:
    if event.delta is None:
        return {}
    return dict(event.delta.data or {})


def _surface(event: CanonicalEvent) -> str:
    data = _delta_data(event)
    if data.get("surface"):
        return str(data["surface"])
    if event.object_refs:
        ref = event.object_refs[0]
        return ref.kind or ref.domain or str(event.domain.value)
    return str(event.domain.value)


def _thread_ref(event: CanonicalEvent) -> str:
    data = _delta_data(event)
    for key in ("thread_ref", "conversation_anchor", "thread_id", "case_ref"):
        value = data.get(key)
        if value:
            return str(value)
    if event.object_refs:
        return str(event.object_refs[0].object_id)
    return event.case_id or event.event_id


def _group_key(event: CanonicalEvent) -> str:
    if event.case_id:
        return str(event.case_id)
    data = _delta_data(event)
    for key in ("thread_ref", "conversation_anchor", "thread_id", "case_ref"):
        value = data.get(key)
        if value:
            return f"{_surface(event)}:{value}"
    if event.object_refs:
        ref = event.object_refs[0]
        return f"{ref.domain}:{ref.kind}:{ref.object_id}"
    return f"{event.domain.value}:{event.kind}:{event.actor_ref.actor_id if event.actor_ref else ''}"


def _clean_text(value: object, *, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _event_title(event: CanonicalEvent) -> str:
    data = _delta_data(event)
    for key in ("subject", "title", "name", "summary"):
        if data.get(key):
            return _clean_text(data[key], limit=120)
    for ref in event.object_refs:
        if ref.label:
            return _clean_text(ref.label, limit=120)
    return _clean_text(event.kind.replace(".", " "), limit=120)


def _event_snippet(event: CanonicalEvent) -> str:
    data = _delta_data(event)
    for key in ("snippet", "summary", "body", "text", "description", "title"):
        if data.get(key):
            return _clean_text(data[key], limit=240)
    for ref in event.object_refs:
        if ref.label:
            return _clean_text(ref.label, limit=240)
    return ""


def _actor_ids(events: Iterable[CanonicalEvent]) -> list[str]:
    actors: set[str] = set()
    for event in events:
        if event.actor_ref is not None and event.actor_ref.actor_id:
            actors.add(event.actor_ref.actor_id)
        actors.update(
            participant.actor_id
            for participant in event.participants
            if participant.actor_id
        )
    return sorted(actors)


def _object_refs(events: Iterable[CanonicalEvent]) -> list[str]:
    refs: set[str] = set()
    for event in events:
        refs.update(ref.object_id for ref in event.object_refs if ref.object_id)
    return sorted(refs)


def _case_ids(events: Iterable[CanonicalEvent]) -> list[str]:
    return sorted({event.case_id for event in events if event.case_id})


def _event_ids(events: Iterable[CanonicalEvent]) -> list[str]:
    return [event.event_id for event in events]


def _evidence_refs(
    events: list[CanonicalEvent], *, limit: int = 8
) -> list[WorkflowEvidenceRef]:
    refs: list[WorkflowEvidenceRef] = []
    for event in events[:limit]:
        actor_id = event.actor_ref.actor_id if event.actor_ref is not None else ""
        refs.append(
            WorkflowEvidenceRef(
                event_id=event.event_id,
                case_id=event.case_id,
                ts_ms=event.ts_ms,
                surface=_surface(event),
                kind=event.kind,
                actor_id=actor_id,
                object_refs=[ref.object_id for ref in event.object_refs],
                snippet=_event_snippet(event),
            )
        )
    return refs


def _title_from_events(events: list[CanonicalEvent]) -> str:
    titles = [_event_title(event) for event in events if _event_title(event)]
    if not titles:
        return "Recurring work pattern"
    counts = Counter(titles)
    return counts.most_common(1)[0][0]


def _pattern_tokens(text: str) -> list[str]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9]+", text.lower())
        if len(token) > 2 and token not in _PATTERN_STOPWORDS
    ]
    business_tokens = sorted({token for token in tokens if token in _BUSINESS_TERMS})
    if business_tokens:
        return business_tokens[:5]
    counts = Counter(tokens)
    return [token for token, _count in counts.most_common(5)]


def _event_kind_family(kind: str) -> str:
    parts = [part for part in kind.lower().split(".") if part]
    return parts[0] if parts else kind.lower()


def _candidate_pattern_key(candidate: WorkflowCandidate) -> str:
    text = " ".join([candidate.title, *candidate.snippets])
    surfaces = ",".join(candidate.surfaces[:4]) or "unknown"
    kind_families = ",".join(
        sorted({_event_kind_family(kind) for kind in candidate.event_kinds})[:4]
    )
    tokens = ",".join(_pattern_tokens(text)) or "generic"
    return f"surfaces={surfaces}|kinds={kind_families}|tokens={tokens}"


def _annotate_repetition(
    candidates: list[WorkflowCandidate],
) -> list[WorkflowCandidate]:
    pattern_keys = {
        candidate.candidate_id: _candidate_pattern_key(candidate)
        for candidate in candidates
    }
    pattern_counts = Counter(pattern_keys.values())
    annotated: list[WorkflowCandidate] = []
    for candidate in candidates:
        pattern_key = pattern_keys[candidate.candidate_id]
        similar_group_count = pattern_counts[pattern_key]
        updated_metadata = {
            **candidate.metadata,
            "event_count": len(candidate.source_event_ids),
            "pattern_key": pattern_key,
            "similar_group_count": similar_group_count,
        }
        repetition_bonus = min(2.0, max(0, similar_group_count - 1) * 0.4)
        annotated.append(
            candidate.model_copy(
                update={
                    "repetition_count": similar_group_count,
                    "rank_score": round(candidate.rank_score + repetition_bonus, 4),
                    "metadata": updated_metadata,
                },
                deep=True,
            )
        )
    return annotated


def _pattern_summary(
    title: str, events: list[CanonicalEvent], surfaces: list[str]
) -> str:
    surface_text = ", ".join(surfaces) if surfaces else "canonical events"
    return (
        f"{title} appears across {len(events)} event"
        f"{'' if len(events) == 1 else 's'} on {surface_text}."
    )


def _candidate_scores(
    events: list[CanonicalEvent], snippets: list[str]
) -> dict[str, float]:
    surfaces = {_surface(event) for event in events}
    actor_count = len(_actor_ids(events))
    object_count = len(_object_refs(events))
    text = " ".join(snippets).lower()
    escalation_hits = sum(1 for term in _ESCALATION_TERMS if term in text)
    business_hits = sum(1 for term in _BUSINESS_TERMS if term in text)
    evidence_density = min(1.0, (len(events) + actor_count + object_count) / 20.0)
    cross_surface_score = min(1.0, len(surfaces) / 3.0)
    escalation_score = min(1.0, escalation_hits / 4.0)
    labelability_score = min(1.0, (len(snippets) + business_hits + actor_count) / 12.0)
    rank_score = round(
        (len(events) * 0.15)
        + evidence_density
        + cross_surface_score
        + escalation_score
        + labelability_score
        + min(1.0, business_hits / 4.0),
        4,
    )
    return {
        "evidence_density": round(evidence_density, 4),
        "cross_surface_score": round(cross_surface_score, 4),
        "escalation_score": round(escalation_score, 4),
        "labelability_score": round(labelability_score, 4),
        "rank_score": rank_score,
    }


def _draft_spec_for_candidate(
    *,
    candidate_id: str,
    title: str,
    company_name: str,
    company_domain: str,
    group_key: str,
    events: list[CanonicalEvent],
    surfaces: list[str],
    snippets: list[str],
    scores: dict[str, float],
) -> BusinessTaskSpec:
    event_ids = _event_ids(events)
    case_ids = _case_ids(events)
    example = WorkflowObservedExample(
        example_id=_stable_id(candidate_id, "observed-example", prefix="wex"),
        case_id=case_ids[0] if case_ids else None,
        thread_ref=_thread_ref(events[0]) if events else "",
        summary=_pattern_summary(title, events, surfaces),
        event_ids=event_ids,
        surfaces=surfaces,
        actor_ids=_actor_ids(events),
        object_refs=_object_refs(events),
        start_ts_ms=min((event.ts_ms for event in events if event.ts_ms), default=None),
        end_ts_ms=max((event.ts_ms for event in events if event.ts_ms), default=None),
    )
    reference_path = WorkflowReferencePath(
        path_id=_stable_id(candidate_id, "reference-path", prefix="wrp"),
        title=f"Observed example: {title}",
        description="Evidence-backed example path; not a mandated script.",
        event_ids=event_ids,
        case_ids=case_ids,
        evidence_refs=_evidence_refs(events),
    )
    required_evidence = [snippet for snippet in snippets[:5] if snippet] or [
        "Canonical events for the observed case or thread."
    ]
    return BusinessTaskSpec(
        task_id=_stable_id(company_domain, group_key, title, prefix="bts"),
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        objective=f"Understand and improve the recurring business work around {title}.",
        business_context=_pattern_summary(title, events, surfaces),
        context_requirements=[
            "Thread or case history",
            "Relevant actors and recipients",
            "Source event timestamps",
        ],
        required_evidence=required_evidence,
        permitted_tools=[],
        constraints=[],
        policies=[],
        acceptable_outputs=[],
        accept_reject_criteria=[],
        evaluation_rubric=[],
        escalation_paths=[],
        observed_examples=[example],
        reference_paths=[reference_path],
        source_event_ids=event_ids,
        source_case_ids=case_ids,
        labels=[],
        open_questions=[
            "What outcome marks this task as accepted or rejected?",
            "Which context is required before an agent can act safely?",
            "When should this workflow escalate to a human reviewer?",
        ],
        spec_confidence=max(0.1, min(0.85, scores["evidence_density"])),
        status=BusinessTaskStatus.DRAFT,
        evaluation_level=EvaluationLevel.DESCRIPTIVE,
        metadata={
            "candidate_id": candidate_id,
            "group_key": group_key,
            "generated_by": "workflow_mining_v1",
            "claim_boundary": "descriptive evidence summary, not a deterministic workflow",
        },
    )


def _candidate_from_group(
    *,
    group_key: str,
    events: list[CanonicalEvent],
    company_name: str,
    company_domain: str,
) -> WorkflowCandidate:
    events = sorted(events, key=lambda event: (event.ts_ms or 0, event.event_id))
    title = _title_from_events(events)
    event_ids = _event_ids(events)
    candidate_id = _stable_id(company_domain, group_key, prefix="wfc")
    surfaces = sorted({_surface(event) for event in events if _surface(event)})
    kinds = sorted({event.kind for event in events if event.kind})
    snippets = [_event_snippet(event) for event in events if _event_snippet(event)]
    scores = _candidate_scores(events, snippets)
    draft_spec = _draft_spec_for_candidate(
        candidate_id=candidate_id,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        group_key=group_key,
        events=events,
        surfaces=surfaces,
        snippets=snippets,
        scores=scores,
    )
    fingerprint = _stable_hash(
        {
            "event_ids": event_ids,
            "surfaces": surfaces,
            "kinds": kinds,
            "title": title,
        }
    )
    return WorkflowCandidate(
        candidate_id=candidate_id,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        group_key=group_key,
        source_case_ids=_case_ids(events),
        source_event_ids=event_ids,
        thread_refs=sorted(
            {_thread_ref(event) for event in events if _thread_ref(event)}
        ),
        surfaces=surfaces,
        event_kinds=kinds,
        actor_ids=_actor_ids(events),
        object_refs=_object_refs(events),
        start_ts_ms=min((event.ts_ms for event in events if event.ts_ms), default=None),
        end_ts_ms=max((event.ts_ms for event in events if event.ts_ms), default=None),
        repetition_count=1,
        evidence_density=scores["evidence_density"],
        cross_surface_score=scores["cross_surface_score"],
        escalation_score=scores["escalation_score"],
        labelability_score=scores["labelability_score"],
        rank_score=scores["rank_score"],
        summary=_pattern_summary(title, events, surfaces),
        snippets=snippets[:8],
        draft_task_spec=draft_spec,
        metadata={"fingerprint": fingerprint},
    )


def mine_workflows(
    source_dir: str | Path,
    *,
    output: str | Path | None = None,
    limit: int = 25,
) -> WorkflowMiningResult:
    """Mine recurring workflow candidates from canonical events.

    The result is descriptive. It produces candidates and draft task specs, not
    deterministic step graphs.
    """

    root, context_path = _resolve_source(source_dir)
    context_payload = _load_context_payload(context_path)
    company_name, company_domain = _company_from_context(context_payload)
    events = _load_events(root)
    groups: dict[str, list[CanonicalEvent]] = defaultdict(list)
    for event in events:
        groups[_group_key(event)].append(event)

    candidates = _annotate_repetition(
        [
            _candidate_from_group(
                group_key=group_key,
                events=group_events,
                company_name=company_name,
                company_domain=company_domain,
            )
            for group_key, group_events in groups.items()
            if group_events
        ]
    )
    candidates.sort(
        key=lambda candidate: (
            candidate.rank_score,
            candidate.repetition_count,
            candidate.start_ts_ms or 0,
        ),
        reverse=True,
    )
    result = WorkflowMiningResult(
        source_dir=str(context_path),
        company_name=company_name,
        company_domain=company_domain,
        event_count=len(events),
        candidate_count=min(len(candidates), limit),
        candidates=candidates[:limit],
        metadata={
            "source_root": str(root),
            "mining_version": "workflow_mining_v1",
            "generated_at": _now_iso(),
        },
    )
    if output is not None:
        save_mining_result(result, output)
    return result


def save_mining_result(result: WorkflowMiningResult, output_root: str | Path) -> Path:
    root = Path(output_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / MINING_RESULT_FILE
    path.write_text(result.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return path


def load_mining_result(root: str | Path) -> WorkflowMiningResult:
    path = Path(root).expanduser().resolve() / MINING_RESULT_FILE
    return WorkflowMiningResult.model_validate_json(path.read_text(encoding="utf-8"))


def load_workflow_labels(root: str | Path) -> list[WorkflowLabel]:
    path = Path(root).expanduser().resolve() / LABELS_FILE
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [WorkflowLabel.model_validate(item) for item in payload.get("labels", [])]


def save_workflow_labels(root: str | Path, labels: list[WorkflowLabel]) -> Path:
    destination = Path(root).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / LABELS_FILE
    payload = {
        "schema_version": 1,
        "labels": [label.model_dump(mode="json") for label in labels],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def add_workflow_label(
    root: str | Path,
    *,
    candidate_id: str,
    label: WorkflowLabelKind | str,
    note: str = "",
    event_ids: Iterable[str] | None = None,
    example_ids: Iterable[str] | None = None,
) -> WorkflowLabel:
    mining_result = load_mining_result(root)
    candidate_ids = {candidate.candidate_id for candidate in mining_result.candidates}
    if candidate_id not in candidate_ids:
        raise KeyError(f"unknown workflow candidate: {candidate_id}")
    labels = load_workflow_labels(root)
    created = WorkflowLabel(
        label_id=_stable_id(candidate_id, label, note, len(labels), prefix="wfl"),
        candidate_id=candidate_id,
        label=WorkflowLabelKind(label),
        note=note,
        event_ids=list(event_ids or []),
        example_ids=list(example_ids or []),
        created_at=_now_iso(),
    )
    labels.append(created)
    save_workflow_labels(root, labels)
    return created


def _labels_for_candidate(
    labels: list[WorkflowLabel], candidate_id: str
) -> list[WorkflowLabel]:
    return [label for label in labels if label.candidate_id == candidate_id]


def promote_workflow_candidate(
    root: str | Path,
    *,
    candidate_id: str,
    output: str | Path,
) -> BusinessTaskSpec:
    mining_result = load_mining_result(root)
    candidate = next(
        (
            item
            for item in mining_result.candidates
            if item.candidate_id == candidate_id
        ),
        None,
    )
    if candidate is None:
        raise KeyError(f"unknown workflow candidate: {candidate_id}")
    labels = _labels_for_candidate(load_workflow_labels(root), candidate_id)
    level = (
        EvaluationLevel.LABELED
        if labels
        else candidate.draft_task_spec.evaluation_level
    )
    spec = candidate.draft_task_spec.model_copy(
        update={
            "labels": labels,
            "evaluation_level": level,
            "metadata": {
                **candidate.draft_task_spec.metadata,
                "promoted_from_candidate_id": candidate_id,
                "promoted_at": _now_iso(),
            },
        },
        deep=True,
    )
    path = Path(output).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(spec.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return spec


def business_task_from_workflow(
    workflow: WorkflowScenarioSpec,
    *,
    contract: ContractSpec | None = None,
    company_name: str = "",
    company_domain: str = "",
) -> BusinessTaskSpec:
    tools = sorted(
        {
            step.tool
            for step in workflow.steps
            if step.tool is not None and step.tool.strip()
        }
    )
    graph_tools = sorted(
        {
            f"vei.graph_action:{step.graph_domain}.{step.graph_action}"
            for step in workflow.steps
            if step.graph_domain is not None and step.graph_action is not None
        }
    )
    criteria = [
        assertion.description
        or f"{assertion.kind}:{assertion.field or assertion.focus or ''}".rstrip(":")
        for assertion in workflow.success_assertions
    ]
    metadata = {
        "source_workflow_name": workflow.name,
        "source_workflow_tags": list(workflow.tags),
        "source_kind": "WorkflowScenarioSpec",
        "reference_path_semantics": "example-only",
    }
    if contract is not None:
        metadata["contract"] = contract.model_dump(mode="json")
    level = (
        EvaluationLevel.CONTRACT_EVALUABLE
        if contract is not None
        else (
            EvaluationLevel.RUBRIC_EVALUABLE
            if criteria
            else EvaluationLevel.DESCRIPTIVE
        )
    )
    return BusinessTaskSpec(
        task_id=_stable_id(company_domain, workflow.name, prefix="bts"),
        title=workflow.name.replace("_", " ").replace("-", " ").title(),
        company_name=company_name,
        company_domain=company_domain,
        objective=workflow.objective.statement,
        business_context="Converted from an existing deterministic workflow scenario.",
        context_requirements=[
            f"{key}: {value}" for key, value in sorted(workflow.world.items())
        ],
        required_evidence=list(workflow.objective.success),
        permitted_tools=tools + graph_tools,
        constraints=[constraint.description for constraint in workflow.constraints],
        policies=[
            f"{approval.stage}: approval from {approval.approver}"
            for approval in workflow.approvals
        ],
        acceptable_outputs=list(workflow.objective.success),
        accept_reject_criteria=criteria,
        evaluation_rubric=criteria,
        escalation_paths=[
            f"{path.name}: {path.notes or 'recover via referenced steps'}"
            for path in workflow.failure_paths
        ]
        + [
            f"{approval.stage}: request approval from {approval.approver}"
            for approval in workflow.approvals
        ],
        observed_examples=[],
        reference_paths=[
            WorkflowReferencePath(
                path_id=_stable_id(workflow.name, "workflow-reference", prefix="wrp"),
                title=f"Example path from {workflow.name}",
                description=(
                    "Existing workflow steps are preserved as an example path, "
                    "not as required ordered Business Task Spec steps."
                ),
                metadata={
                    "example_step_ids": [step.step_id for step in workflow.steps],
                    "source": "WorkflowScenarioSpec",
                },
            )
        ],
        source_event_ids=[],
        source_case_ids=[],
        labels=[],
        open_questions=[],
        spec_confidence=0.8 if contract is not None else 0.55,
        status=(
            BusinessTaskStatus.REVIEWED
            if contract is not None
            else BusinessTaskStatus.DRAFT
        ),
        evaluation_level=level,
        metadata=metadata,
    )


def contract_from_business_task(spec: BusinessTaskSpec) -> ContractSpec:
    if not evaluation_level_at_least(
        spec.evaluation_level, EvaluationLevel.CONTRACT_EVALUABLE
    ):
        raise ValueError(
            "BusinessTaskSpec is not contract-evaluable; promote it with deterministic "
            "success/failure predicates first."
        )
    contract_payload = spec.metadata.get("contract")
    if not isinstance(contract_payload, dict):
        raise ValueError(
            "BusinessTaskSpec is contract-evaluable but does not carry a contract payload."
        )
    return ContractSpec.model_validate(contract_payload)


def _events_by_id(events: list[CanonicalEvent]) -> dict[str, CanonicalEvent]:
    return {event.event_id: event for event in events}


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _package_splits(case_ids: list[str]) -> dict[str, list[str]]:
    splits: dict[str, list[str]] = {"train": [], "validation": [], "test": []}
    for case_id in sorted(case_ids):
        bucket = int(hashlib.sha256(case_id.encode("utf-8")).hexdigest()[:8], 16) % 10
        if bucket == 0:
            splits["test"].append(case_id)
        elif bucket == 1:
            splits["validation"].append(case_id)
        else:
            splits["train"].append(case_id)
    return splits


def package_workflow_environment(
    *,
    spec_path: str | Path,
    source_dir: str | Path,
    output: str | Path,
) -> WorkflowEnvironmentPackageManifest:
    spec = BusinessTaskSpec.model_validate_json(
        Path(spec_path).expanduser().resolve().read_text(encoding="utf-8")
    )
    if spec.evaluation_level != EvaluationLevel.RL_PACKAGED:
        raise ValueError(
            "workflow package-env requires evaluation_level=rl_packaged. "
            "Descriptive, labeled, and rubric-evaluable specs remain useful but "
            "cannot be exported as RL environments; promote through a reviewed "
            "contract-evaluable spec first."
        )
    contract = contract_from_business_task(spec)
    root, _context_path = _resolve_source(source_dir)
    events = _load_events(root)
    events_by_id = _events_by_id(events)
    output_root = Path(output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    package_id = _stable_id(spec.task_id, contract.name, prefix="wenv")
    selected_event_ids = set(spec.source_event_ids)
    selected_events = [
        events_by_id[event_id].model_dump(mode="json")
        for event_id in spec.source_event_ids
        if event_id in events_by_id
    ]
    reset_cases = [
        {
            "case_id": case_id,
            "event_ids": [
                event.event_id
                for event in events
                if event.case_id == case_id and event.event_id in selected_event_ids
            ],
        }
        for case_id in spec.source_case_ids
    ]
    splits = _package_splits(spec.source_case_ids)
    tool_schema: dict[str, object] = {"type": "string"}
    if spec.permitted_tools:
        tool_schema["enum"] = spec.permitted_tools

    files = {
        "task_spec.json": spec.model_dump(mode="json"),
        "contract.json": contract.model_dump(mode="json"),
        "observation_schema.json": {
            "schema_version": 1,
            "type": "object",
            "required": ["case_id", "events"],
            "properties": {
                "case_id": {"type": "string"},
                "events": {"type": "array", "items": {"type": "object"}},
                "context": {"type": "object"},
            },
        },
        "action_schema.json": {
            "schema_version": 1,
            "type": "object",
            "required": ["tool", "args"],
            "properties": {
                "tool": tool_schema,
                "args": {"type": "object"},
                "evidence": {"type": "array", "items": {"type": "string"}},
            },
        },
        "reward_spec.json": {
            "schema_version": 1,
            "contract_name": contract.name,
            "claim_boundary": "process/compliance reward only",
            "reward_terms": [
                term.model_dump(mode="json") for term in contract.reward_terms
            ],
            "success_predicates": [
                predicate.model_dump(mode="json")
                for predicate in contract.success_predicates
            ],
            "forbidden_predicates": [
                predicate.model_dump(mode="json")
                for predicate in contract.forbidden_predicates
            ],
        },
        "splits.json": splits,
        "environment_manifest.json": {
            "schema_version": 1,
            "package_id": package_id,
            "task_id": spec.task_id,
            "contract_name": contract.name,
            "evaluation_level": spec.evaluation_level.value,
            "source_dir": str(source_dir),
            "generated_at": _now_iso(),
        },
    }
    for filename, payload in files.items():
        _write_json(output_root / filename, payload)

    with (output_root / "reset_cases.jsonl").open("w", encoding="utf-8") as handle:
        for item in reset_cases:
            handle.write(json.dumps(item, sort_keys=True) + "\n")
    with (output_root / "example_traces.jsonl").open("w", encoding="utf-8") as handle:
        for example in spec.observed_examples:
            payload = example.model_dump(mode="json")
            payload["events"] = selected_events
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
    readme = (
        f"# {spec.title}\n\n"
        "This package is an RL/eval environment for process and compliance "
        "properties only. Rewards are shaped by the included deterministic "
        "contract; they are not claims about real-world business outcomes.\n\n"
        f"- Task spec: `{spec.task_id}`\n"
        f"- Contract: `{contract.name}`\n"
        f"- Evaluation level: `{spec.evaluation_level.value}`\n"
    )
    (output_root / "README.md").write_text(readme, encoding="utf-8")

    file_names = sorted(
        [*files.keys(), "README.md", "example_traces.jsonl", "reset_cases.jsonl"]
    )
    return WorkflowEnvironmentPackageManifest(
        package_id=package_id,
        task_id=spec.task_id,
        evaluation_level=spec.evaluation_level,
        output_root=str(output_root),
        files=file_names,
        claim_boundaries=[
            "Rewards are deterministic process/compliance predicates.",
            "The package does not claim to optimize business outcomes.",
            "Held-out splits are stable by case/thread hash.",
        ],
        metadata={"contract_name": contract.name},
    )


def refresh_workflows(
    *,
    source_dir: str | Path,
    workspace: str | Path,
    output: str | Path,
    limit: int = 25,
    refresh_wiki_artifacts: bool = False,
    refresh_skillmap_artifacts: bool = False,
) -> WorkflowRefreshReport:
    output_root = Path(output).expanduser().resolve()
    previous_result: WorkflowMiningResult | None = None
    if (output_root / MINING_RESULT_FILE).is_file():
        previous_result = load_mining_result(output_root)
    labels = load_workflow_labels(output_root)
    new_result = mine_workflows(source_dir, output=output_root, limit=limit)

    previous_candidates = (
        {candidate.candidate_id: candidate for candidate in previous_result.candidates}
        if previous_result is not None
        else {}
    )
    current_candidates = {
        candidate.candidate_id: candidate for candidate in new_result.candidates
    }
    previous_ids = set(previous_candidates)
    current_ids = set(current_candidates)
    shared_ids = previous_ids & current_ids
    changed = sorted(
        candidate_id
        for candidate_id in shared_ids
        if previous_candidates[candidate_id].metadata.get("fingerprint")
        != current_candidates[candidate_id].metadata.get("fingerprint")
    )
    unchanged = sorted(shared_ids - set(changed))
    if labels:
        save_workflow_labels(output_root, labels)

    wiki_status = "not_requested"
    if refresh_wiki_artifacts:
        try:
            from vei.wiki.api import refresh_wiki

            report = refresh_wiki(
                workspace,
                context_path=source_dir,
                output_dir=output_root / "wiki",
            )
            wiki_status = report.status
        except Exception as exc:  # noqa: BLE001
            wiki_status = f"error:{exc}"

    skillmap_status = "not_requested"
    if refresh_skillmap_artifacts:
        try:
            from vei.skillmap.api import (
                build_company_skill_map_from_workspace,
                write_company_skill_map_outputs,
            )

            skill_map = build_company_skill_map_from_workspace(
                workspace,
                context_path=source_dir,
                include_replay=False,
                provider="codex",
            )
            write_company_skill_map_outputs(skill_map, output_root / "skill_map")
            skillmap_status = "ok"
        except Exception as exc:  # noqa: BLE001
            skillmap_status = f"error:{exc}"

    report = WorkflowRefreshReport(
        source_dir=str(source_dir),
        workspace=str(workspace),
        output=str(output_root),
        new_candidate_ids=sorted(current_ids - previous_ids),
        changed_candidate_ids=changed,
        retired_candidate_ids=sorted(previous_ids - current_ids),
        unchanged_candidate_ids=unchanged,
        label_count=len(labels),
        wiki_refresh_status=wiki_status,
        skillmap_refresh_status=skillmap_status,
        metadata={"candidate_count": new_result.candidate_count},
    )
    (output_root / REFRESH_REPORT_FILE).write_text(
        report.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    return report


__all__ = [
    "BusinessTaskSpec",
    "BusinessTaskStatus",
    "EvaluationLevel",
    "LABELS_FILE",
    "MINING_RESULT_FILE",
    "REFRESH_REPORT_FILE",
    "WorkflowCandidate",
    "WorkflowEnvironmentPackageManifest",
    "WorkflowLabel",
    "WorkflowLabelKind",
    "WorkflowMiningResult",
    "WorkflowRefreshReport",
    "add_workflow_label",
    "business_task_from_workflow",
    "contract_from_business_task",
    "load_mining_result",
    "load_workflow_labels",
    "mine_workflows",
    "package_workflow_environment",
    "promote_workflow_candidate",
    "refresh_workflows",
    "save_mining_result",
    "save_workflow_labels",
]
