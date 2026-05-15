from __future__ import annotations

import csv
import hashlib
import json
import re
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
MINING_MANIFEST_FILE = "workflow_mining_manifest.json"
LABELS_FILE = "workflow_labels.json"
REFRESH_REPORT_FILE = "workflow_refresh_report.json"

_BOUNDARY_EXPORTS = (
    BusinessTaskSpec,
    EvaluationLevel,
    WorkflowCandidate,
    WorkflowMiningResult,
)

# English stopwords used when tokenizing event text for skill/world-model
# matching. Not a vocabulary signal — just filler-word filtering.
_WORKFLOW_TEXT_STOPWORDS = {
    "about",
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
    "able",
    "action",
    "actions",
    "all",
    "and",
    "any",
    "are",
    "app",
    "before",
    "been",
    "being",
    "build",
    "but",
    "can",
    "check",
    "could",
    "company",
    "data",
    "day",
    "done",
    "each",
    "event",
    "flow",
    "for",
    "gate",
    "get",
    "has",
    "its",
    "may",
    "new",
    "not",
    "one",
    "path",
    "review",
    "run",
    "should",
    "state",
    "status",
    "step",
    "task",
    "test",
    "the",
    "through",
    "until",
    "use",
    "user",
    "when",
    "will",
    "would",
    "work",
}

_NOISY_WORKFLOW_TITLE_RE = re.compile(
    r"^(?:hi|hello|hey|ok|okay|sure|yes|no|thanks|thank you|done|cool|great|"
    r"chat/|https?://|www\\.)",
    re.IGNORECASE,
)
_LOW_SIGNAL_EVIDENCE_RE = re.compile(
    r"^(?:hi|hello|hey|ok|okay|sure|yes|no|thanks|thank you|done|cool|great|"
    r"relevant recordings|daily updates?)\\b[\\s.!?,:;-]*$",
    re.IGNORECASE,
)
_EMAIL_TEXT_RE = re.compile(
    r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE
)
_PHONE_TEXT_RE = re.compile(
    r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}\b"
)
_CONNECTION_STRING_RE = re.compile(r"\b[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
_URL_RE = re.compile(r"\bhttps?://\S+", re.IGNORECASE)
_API_KEY_RE = re.compile(
    r"(?i)\b(?:api[_-]?key|token|secret|password|passwd|pwd|cvv)\s*[=:]\s*[^\s,;]+"
)
_ACCOUNT_ID_RE = re.compile(
    r"(?i)\b(account\s*id|accountid|account_id)\s*[:=]?\s*[a-f0-9]{12,}\b"
)

_SKILLMAP_GENERATOR = "skillmap_semantic_v1"
_WORLD_MODEL_OPPORTUNITY_GENERATOR = "world_model_skill_opportunity_v1"


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


def _autodiscover_skill_map_path(root: Path, output: str | Path | None) -> Path | None:
    candidates: list[Path] = [
        root / "skill_map" / "company_skill_map.json",
        root / ".artifacts" / "skillmap" / "company_skill_map.json",
    ]
    if output is not None:
        output_root = Path(output).expanduser().resolve()
        candidates.extend(
            [
                output_root / "skill_map" / "company_skill_map.json",
                output_root.parent / "skill_map" / "company_skill_map.json",
            ]
        )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _load_skill_map(path: str | Path | None) -> Any | None:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"skill map not found: {resolved}")
    from vei.skillmap.api import CompanySkillMap

    return CompanySkillMap.model_validate_json(resolved.read_text(encoding="utf-8"))


def _resolve_world_model_report_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    if resolved.is_dir():
        for name in (
            "strategic_state_point_results.csv",
            "strategic_state_point_results.json",
        ):
            candidate = resolved / name
            if candidate.is_file():
                return candidate
    if resolved.is_file():
        return resolved
    raise FileNotFoundError(f"world-model report not found: {resolved}")


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
    events: list[CanonicalEvent], *, limit: int = 8, redact: bool = False
) -> list[WorkflowEvidenceRef]:
    refs: list[WorkflowEvidenceRef] = []
    for event in events[:limit]:
        actor_id = event.actor_ref.actor_id if event.actor_ref is not None else ""
        snippet = _event_snippet(event)
        if redact:
            snippet = _redact_workflow_text(snippet)
        refs.append(
            WorkflowEvidenceRef(
                event_id=event.event_id,
                case_id=event.case_id,
                ts_ms=event.ts_ms,
                surface=_surface(event),
                kind=event.kind,
                actor_id=actor_id,
                object_refs=[ref.object_id for ref in event.object_refs],
                snippet=snippet,
            )
        )
    return refs


def _tokens_for_match(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", text.lower())
        if len(token) > 2 and token not in _WORKFLOW_TEXT_STOPWORDS
    }


def _redact_workflow_text(text: str) -> str:
    redacted = text
    redacted = _CONNECTION_STRING_RE.sub(
        lambda match: (
            "[REDACTED_CONNECTION_STRING]"
            if "@" in match.group(0)
            else _URL_RE.sub("[REDACTED_URL]", match.group(0))
        ),
        redacted,
    )
    redacted = _URL_RE.sub("[REDACTED_URL]", redacted)
    redacted = _EMAIL_TEXT_RE.sub("[REDACTED_EMAIL]", redacted)
    redacted = _PHONE_TEXT_RE.sub("[REDACTED_PHONE]", redacted)
    redacted = _API_KEY_RE.sub("[REDACTED_SECRET]", redacted)
    redacted = _ACCOUNT_ID_RE.sub(
        lambda match: f"{match.group(1)} [REDACTED_ID]", redacted
    )
    return redacted


def _skill_ref_text(ref: Any) -> str:
    return _clean_text(
        getattr(ref, "title", "") or getattr(ref, "snippet", ""), limit=240
    )


def _is_low_signal_evidence(text: str) -> bool:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return True
    if _LOW_SIGNAL_EVIDENCE_RE.match(cleaned):
        return True
    tokens = _tokens_for_match(cleaned)
    if len(tokens) < 2 and "[REDACTED_" not in cleaned:
        return True
    return False


def _evidence_text_score(
    text: str, reference_tokens: set[str]
) -> tuple[float, int, str]:
    tokens = _tokens_for_match(text)
    overlap = tokens & reference_tokens
    score = len(overlap) * 2.0
    score += min(3.0, len(tokens) / 3.0)
    if "[REDACTED_" in text:
        score += 0.35
    return score, len(tokens), text


def _skill_evidence_texts(
    skill: Any,
    events: list[CanonicalEvent],
    *,
    limit: int = 8,
) -> list[str]:
    reference_tokens = _tokens_for_match(_skill_reference_text(skill))
    raw_texts: list[str] = []
    for ref in getattr(skill, "evidence_refs", []) or []:
        raw_texts.append(_skill_ref_text(ref))
    raw_texts.extend(_event_snippet(event) for event in events)

    seen: set[str] = set()
    scored: list[tuple[float, int, str]] = []
    for raw_text in raw_texts:
        text = _clean_text(_redact_workflow_text(raw_text), limit=240)
        if _is_low_signal_evidence(text):
            continue
        if text in seen:
            continue
        seen.add(text)
        scored.append(_evidence_text_score(text, reference_tokens))
    scored.sort(reverse=True)
    return [text for _score, _token_count, text in scored[:limit]]


def _ranked_skill_events(
    skill: Any, events: list[CanonicalEvent], *, limit: int = 10
) -> list[CanonicalEvent]:
    reference_tokens = _tokens_for_match(_skill_reference_text(skill))
    scored: list[tuple[float, int, str, CanonicalEvent]] = []
    for event in events:
        text = _clean_text(_redact_workflow_text(_event_snippet(event)), limit=240)
        if _is_low_signal_evidence(text):
            continue
        score, token_count, _ = _evidence_text_score(text, reference_tokens)
        scored.append((score, token_count, event.event_id, event))
    scored.sort(reverse=True)
    ranked = [event for _score, _token_count, _event_id, event in scored[:limit]]
    if ranked:
        return ranked
    return events[:limit]


def _skill_evidence_event_ids(skill: Any) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for ref in getattr(skill, "evidence_refs", []) or []:
        ref_type = str(getattr(ref, "ref_type", "") or "")
        ref_id = str(getattr(ref, "ref_id", "") or "")
        if ref_type == "event" and ref_id and ref_id not in seen:
            ids.append(ref_id)
            seen.add(ref_id)
        metadata = getattr(ref, "metadata", {}) or {}
        if isinstance(metadata, dict):
            for event_id in metadata.get("event_ids") or []:
                text_id = str(event_id)
                if text_id and text_id not in seen:
                    ids.append(text_id)
                    seen.add(text_id)
    return ids


def _skill_reference_text(skill: Any) -> str:
    trigger = getattr(skill, "trigger", None)
    trigger_text = ""
    if trigger is not None:
        trigger_text = " ".join(
            [
                str(getattr(trigger, "description", "") or ""),
                " ".join(str(item) for item in getattr(trigger, "signals", []) or []),
            ]
        )
    step_text = " ".join(
        str(getattr(step, "instruction", "") or "")
        for step in getattr(skill, "steps", []) or []
    )
    output_text = " ".join(
        str(getattr(output, "title", "") or "")
        for output in getattr(skill, "output_artifacts", []) or []
    )
    return " ".join(
        [
            str(getattr(skill, "title", "") or ""),
            str(getattr(skill, "summary", "") or ""),
            str(getattr(skill, "goal", "") or ""),
            str(getattr(skill, "usefulness_rationale", "") or ""),
            trigger_text,
            step_text,
            output_text,
            " ".join(str(item) for item in getattr(skill, "allowed_actions", []) or []),
            " ".join(str(item) for item in getattr(skill, "blocked_actions", []) or []),
            " ".join(str(item) for item in getattr(skill, "tags", []) or []),
        ]
    )


def _skill_is_promotable_workflow(skill: Any) -> bool:
    if str(getattr(skill, "status", "") or "") in {"gap", "retired"}:
        return False
    if str(getattr(skill, "candidate_type", "") or "") == "gap":
        return False
    title = str(getattr(skill, "title", "") or "").strip()
    if not title or _NOISY_WORKFLOW_TITLE_RE.search(title):
        return False
    if not str(getattr(skill, "goal", "") or "").strip():
        return False
    if getattr(skill, "trigger", None) is None:
        return False
    if len(_skill_evidence_event_ids(skill)) < 1 and not getattr(
        skill, "evidence_refs", []
    ):
        return False
    has_operational_shape = bool(getattr(skill, "steps", [])) or bool(
        getattr(skill, "output_artifacts", [])
    )
    return has_operational_shape


def _read_world_model_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "candidates", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _world_model_alignment(
    skill: Any, world_model_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    if not world_model_rows:
        return {"score": 0.0, "matches": []}
    skill_tokens = _tokens_for_match(_skill_reference_text(skill))
    if not skill_tokens:
        return {"score": 0.0, "matches": []}
    matches: list[dict[str, Any]] = []
    for row in world_model_rows:
        text = " ".join(
            str(row.get(key, "") or "")
            for key in (
                "decision_point",
                "decision_question",
                "why_this_decision_was_proposed",
                "counterfactual_action",
                "candidate_label",
                "candidate_type",
                "success_observable",
                "failure_observable",
                "next_decision_trigger",
            )
        )
        row_tokens = _tokens_for_match(text)
        if not row_tokens:
            continue
        overlap = skill_tokens & row_tokens
        if not overlap:
            continue
        jaccard = len(overlap) / max(1, len(skill_tokens | row_tokens))
        supported = row.get("supported_target_score") or row.get("supported_score")
        try:
            supported_score = float(supported) if supported not in (None, "") else 0.0
        except (TypeError, ValueError):
            supported_score = 0.0
        alignment_score = min(1.0, (jaccard * 2.4) + (supported_score * 0.15))
        matches.append(
            {
                "score": round(alignment_score, 4),
                "overlap_terms": sorted(overlap)[:12],
                "decision_point": row.get("decision_point", ""),
                "candidate_label": row.get("candidate_label", ""),
                "candidate_type": row.get("candidate_type", ""),
                "supported_target_score": supported,
            }
        )
    matches.sort(key=lambda item: item["score"], reverse=True)
    score = matches[0]["score"] if matches else 0.0
    return {"score": score, "matches": matches[:3]}


def _readiness_score(value: str) -> float:
    return {
        "activation_candidate": 1.0,
        "shadow_ready": 0.85,
        "needs_review": 0.55,
        "not_tested": 0.35,
    }.get(value, 0.4)


def _semantic_workflow_quality(
    skill: Any,
    *,
    event_count: int,
    world_model_alignment_score: float,
) -> dict[str, float]:
    evidence_refs = getattr(skill, "evidence_refs", []) or []
    steps = getattr(skill, "steps", []) or []
    outputs = getattr(skill, "output_artifacts", []) or []
    allowed_actions = getattr(skill, "allowed_actions", []) or []
    blocked_actions = getattr(skill, "blocked_actions", []) or []
    trigger = getattr(skill, "trigger", None)
    try:
        usefulness = float(getattr(skill, "usefulness_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        usefulness = 0.0
    try:
        confidence = float(getattr(skill, "confidence", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    evidence_coverage = min(1.0, max(len(evidence_refs), event_count) / 8.0)
    operational_shape = min(
        1.0,
        (
            (0.25 if trigger is not None else 0.0)
            + (0.25 if steps else 0.0)
            + (0.2 if outputs else 0.0)
            + (0.15 if allowed_actions else 0.0)
            + (0.15 if blocked_actions else 0.0)
        ),
    )
    readiness = _readiness_score(str(getattr(skill, "deployment_readiness", "") or ""))
    total = (
        (0.28 * usefulness)
        + (0.18 * confidence)
        + (0.18 * evidence_coverage)
        + (0.16 * operational_shape)
        + (0.1 * readiness)
        + (0.1 * world_model_alignment_score)
    )
    return {
        "usefulness": round(usefulness, 4),
        "confidence": round(confidence, 4),
        "evidence_coverage": round(evidence_coverage, 4),
        "operational_shape": round(operational_shape, 4),
        "readiness": round(readiness, 4),
        "world_model_alignment": round(world_model_alignment_score, 4),
        "quality": round(min(1.0, total), 4),
    }


def _semantic_spec_for_skill(
    *,
    candidate_id: str,
    skill: Any,
    company_name: str,
    company_domain: str,
    group_key: str,
    events: list[CanonicalEvent],
    quality: dict[str, float],
    world_model_alignment: dict[str, Any],
) -> BusinessTaskSpec:
    title = str(getattr(skill, "title", "") or "Evidence-backed workflow").strip()
    event_ids = _event_ids(events)
    case_ids = _case_ids(events)
    surfaces = sorted({_surface(event) for event in events if _surface(event)})
    trigger = getattr(skill, "trigger", None)
    trigger_description = str(getattr(trigger, "description", "") or "")
    trigger_signals = [
        str(signal)
        for signal in (getattr(trigger, "signals", []) if trigger is not None else [])
        if str(signal).strip()
    ]
    output_titles = [
        str(getattr(output, "title", "") or "").strip()
        for output in getattr(skill, "output_artifacts", []) or []
        if str(getattr(output, "title", "") or "").strip()
    ]
    approval_steps = [
        str(getattr(step, "instruction", "") or "").strip()
        for step in getattr(skill, "steps", []) or []
        if bool(getattr(step, "requires_approval", False))
        and str(getattr(step, "instruction", "") or "").strip()
    ]
    permitted_tools = sorted(
        {
            str(getattr(step, "tool", "") or "").strip()
            for step in getattr(skill, "steps", []) or []
            if str(getattr(step, "tool", "") or "").strip()
        }
    )
    graph_tools = sorted(
        {
            f"vei.graph_action:{getattr(step, 'graph_domain')}.{getattr(step, 'graph_action')}"
            for step in getattr(skill, "steps", []) or []
            if str(getattr(step, "graph_domain", "") or "").strip()
            and str(getattr(step, "graph_action", "") or "").strip()
        }
    )
    evidence_titles = _skill_evidence_texts(skill, events)
    ranked_evidence_events = _ranked_skill_events(skill, events)
    example = WorkflowObservedExample(
        example_id=_stable_id(candidate_id, "semantic-observed-example", prefix="wex"),
        case_id=case_ids[0] if case_ids else None,
        thread_ref=_thread_ref(events[0]) if events else "",
        summary=str(getattr(skill, "summary", "") or title),
        event_ids=event_ids,
        surfaces=surfaces,
        actor_ids=_actor_ids(events),
        object_refs=_object_refs(events),
        start_ts_ms=min((event.ts_ms for event in events if event.ts_ms), default=None),
        end_ts_ms=max((event.ts_ms for event in events if event.ts_ms), default=None),
    )
    reference_path = WorkflowReferencePath(
        path_id=_stable_id(candidate_id, "semantic-reference-path", prefix="wrp"),
        title=f"Observed evidence for {title}",
        description=(
            "Citation-backed operating pattern synthesized from the company skill map."
        ),
        event_ids=event_ids,
        case_ids=case_ids,
        evidence_refs=_evidence_refs(ranked_evidence_events, limit=10, redact=True),
        metadata={
            "source_skill_id": getattr(skill, "skill_id", ""),
            "source": _SKILLMAP_GENERATOR,
        },
    )
    accept_reject_criteria = [
        *[
            str(item)
            for item in getattr(skill, "replay_checks", []) or []
            if str(item).strip()
        ],
        *[f"Output artifact produced: {title}" for title in output_titles[:4]],
    ]
    if not accept_reject_criteria and output_titles:
        accept_reject_criteria = [f"Produce {output_titles[0]} with cited evidence."]
    if not accept_reject_criteria:
        accept_reject_criteria = [
            "A human reviewer can verify the owner, trigger, evidence, and output."
        ]
    open_questions = []
    if not str(getattr(skill, "owner", "") or "").strip():
        open_questions.append("Who owns this workflow?")
    if not str(getattr(skill, "reviewer", "") or "").strip():
        open_questions.append("Who reviews it before activation?")
    if str(getattr(skill, "review_status", "") or "") == "unreviewed":
        open_questions.append(
            "Which cited examples should be reviewed before activation?"
        )
    return BusinessTaskSpec(
        task_id=_stable_id(company_domain, group_key, title, prefix="bts"),
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        objective=str(
            getattr(skill, "goal", "") or getattr(skill, "summary", "") or title
        ),
        business_context=str(getattr(skill, "summary", "") or ""),
        context_requirements=[
            item
            for item in [
                trigger_description,
                *[f"Signal: {signal}" for signal in trigger_signals[:6]],
                *[
                    str(item)
                    for item in getattr(skill, "prerequisites", []) or []
                    if str(item).strip()
                ],
            ]
            if item
        ],
        required_evidence=evidence_titles[:8]
        or ["Cited events from the source skill map."],
        permitted_tools=permitted_tools + graph_tools,
        constraints=[
            *[
                str(item)
                for item in getattr(skill, "negative_triggers", []) or []
                if str(item).strip()
            ],
            *[
                f"Blocked: {item}"
                for item in getattr(skill, "blocked_actions", []) or []
                if str(item).strip()
            ],
        ],
        policies=[
            (
                "Approval required before live writes."
                if str(getattr(skill, "execution_mode", "") or "") == "approval_gated"
                else "Shadow/read-only execution until reviewed."
            ),
            *[f"Approval step: {item}" for item in approval_steps[:4]],
        ],
        acceptable_outputs=output_titles,
        accept_reject_criteria=accept_reject_criteria,
        evaluation_rubric=accept_reject_criteria,
        escalation_paths=approval_steps,
        observed_examples=[example] if events else [],
        reference_paths=[reference_path] if events else [],
        source_event_ids=event_ids,
        source_case_ids=case_ids,
        labels=[],
        open_questions=open_questions,
        spec_confidence=quality["quality"],
        status=BusinessTaskStatus.DRAFT,
        evaluation_level=(
            EvaluationLevel.RUBRIC_EVALUABLE
            if output_titles or accept_reject_criteria
            else EvaluationLevel.DESCRIPTIVE
        ),
        metadata={
            "candidate_id": candidate_id,
            "group_key": group_key,
            "generated_by": _SKILLMAP_GENERATOR,
            "source_skill_id": getattr(skill, "skill_id", ""),
            "claim_boundary": (
                "semantic workflow candidate synthesized from citation-backed "
                "skill evidence; requires human review before activation"
            ),
            "quality": quality,
            "world_model_alignment": world_model_alignment,
        },
    )


def _candidate_from_skill(
    *,
    skill: Any,
    events_by_id: dict[str, CanonicalEvent],
    company_name: str,
    company_domain: str,
    world_model_rows: list[dict[str, Any]],
) -> WorkflowCandidate | None:
    if not _skill_is_promotable_workflow(skill):
        return None
    evidence_ids = _skill_evidence_event_ids(skill)
    events = [
        events_by_id[event_id] for event_id in evidence_ids if event_id in events_by_id
    ]
    if not events:
        return None
    events.sort(key=lambda event: (event.ts_ms or 0, event.event_id))
    alignment = _world_model_alignment(skill, world_model_rows)
    quality = _semantic_workflow_quality(
        skill,
        event_count=len(events),
        world_model_alignment_score=float(alignment["score"]),
    )
    title = str(getattr(skill, "title", "") or "Evidence-backed workflow").strip()
    group_key = f"skill:{getattr(skill, 'skill_id', title)}"
    candidate_id = _stable_id(company_domain, group_key, title, prefix="wfc")
    surfaces = sorted({_surface(event) for event in events if _surface(event)})
    kinds = sorted({event.kind for event in events if event.kind})
    draft_spec = _semantic_spec_for_skill(
        candidate_id=candidate_id,
        skill=skill,
        company_name=company_name,
        company_domain=company_domain,
        group_key=group_key,
        events=events,
        quality=quality,
        world_model_alignment=alignment,
    )
    fingerprint = _stable_hash(
        {
            "source_skill_id": getattr(skill, "skill_id", ""),
            "event_ids": evidence_ids,
            "quality": quality,
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
        source_event_ids=_event_ids(events),
        thread_refs=sorted(
            {_thread_ref(event) for event in events if _thread_ref(event)}
        ),
        surfaces=surfaces,
        event_kinds=kinds,
        actor_ids=_actor_ids(events),
        object_refs=_object_refs(events),
        start_ts_ms=min((event.ts_ms for event in events if event.ts_ms), default=None),
        end_ts_ms=max((event.ts_ms for event in events if event.ts_ms), default=None),
        repetition_count=max(1, len(_case_ids(events))),
        evidence_density=quality["evidence_coverage"],
        cross_surface_score=min(1.0, len(surfaces) / 3.0) if surfaces else 0.0,
        escalation_score=quality["readiness"],
        labelability_score=quality["operational_shape"],
        rank_score=round(quality["quality"] * 100.0, 4),
        summary=str(getattr(skill, "summary", "") or ""),
        snippets=_skill_evidence_texts(skill, events),
        draft_task_spec=draft_spec,
        metadata={
            "fingerprint": fingerprint,
            "generated_by": _SKILLMAP_GENERATOR,
            "source_skill_id": getattr(skill, "skill_id", ""),
            "source_candidate_type": getattr(skill, "candidate_type", ""),
            "deployment_readiness": getattr(skill, "deployment_readiness", ""),
            "review_status": getattr(skill, "review_status", ""),
            "event_count": len(events),
            "evidence_ref_count": len(getattr(skill, "evidence_refs", []) or []),
            "quality": quality,
            "world_model_alignment": alignment,
        },
    )


def _gap_evidence_event_ids(gap: Any) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for ref in getattr(gap, "evidence_refs", []) or []:
        ref_type = str(getattr(ref, "ref_type", "") or "")
        ref_id = str(getattr(ref, "ref_id", "") or "")
        if ref_type == "event" and ref_id and ref_id not in seen:
            ids.append(ref_id)
            seen.add(ref_id)
        metadata = getattr(ref, "metadata", {}) or {}
        if isinstance(metadata, dict):
            for event_id in metadata.get("event_ids") or []:
                text_id = str(event_id)
                if text_id and text_id not in seen:
                    ids.append(text_id)
                    seen.add(text_id)
    return ids


def _candidate_from_world_model_gap(
    *,
    gap: Any,
    events_by_id: dict[str, CanonicalEvent],
    company_name: str,
    company_domain: str,
) -> WorkflowCandidate | None:
    metadata = getattr(gap, "metadata", {}) or {}
    if metadata.get("opportunity_source") != _WORLD_MODEL_OPPORTUNITY_GENERATOR:
        return None
    evidence_ids = _gap_evidence_event_ids(gap)
    events = [
        events_by_id[event_id] for event_id in evidence_ids if event_id in events_by_id
    ]
    if not events:
        return None
    events.sort(key=lambda event: (event.ts_ms or 0, event.event_id))
    title = str(getattr(gap, "title", "") or "World-model skill opportunity").strip()
    group_key = f"world_model_gap:{getattr(gap, 'gap_id', title)}"
    candidate_id = _stable_id(company_domain, group_key, title, prefix="wfc")
    event_ids = _event_ids(events)
    case_ids = _case_ids(events)
    surfaces = sorted({_surface(event) for event in events if _surface(event)})
    kinds = sorted({event.kind for event in events if event.kind})
    priority_score = _safe_float(metadata.get("priority_score"), default=0.5)
    coverage_score = _safe_float(
        metadata.get("existing_skill_coverage_score"), default=0.0
    )
    snippets = _gap_evidence_texts(gap, events)
    summary = " ".join(
        str(item).strip()
        for item in [
            getattr(gap, "reason", ""),
            getattr(gap, "recommendation", ""),
        ]
        if str(item).strip()
    )
    observed = WorkflowObservedExample(
        example_id=_stable_id(candidate_id, "world-model-gap-example", prefix="wex"),
        case_id=case_ids[0] if case_ids else None,
        thread_ref=_thread_ref(events[0]) if events else "",
        summary=summary or title,
        event_ids=event_ids,
        surfaces=surfaces,
        actor_ids=_actor_ids(events),
        object_refs=_object_refs(events),
        start_ts_ms=min((event.ts_ms for event in events if event.ts_ms), default=None),
        end_ts_ms=max((event.ts_ms for event in events if event.ts_ms), default=None),
    )
    reference_path = WorkflowReferencePath(
        path_id=_stable_id(candidate_id, "world-model-gap-reference", prefix="wrp"),
        title=f"Cited evidence for {title}",
        description=(
            "World-model counterfactual opportunity grounded by canonical events."
        ),
        event_ids=event_ids,
        case_ids=case_ids,
        evidence_refs=_evidence_refs(events, limit=10, redact=True),
        metadata={
            "source_gap_id": getattr(gap, "gap_id", ""),
            "source": _WORLD_MODEL_OPPORTUNITY_GENERATOR,
        },
    )
    draft_spec = BusinessTaskSpec(
        task_id=_stable_id(company_domain, group_key, title, prefix="bts"),
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        objective=str(
            metadata.get("counterfactual_action")
            or getattr(gap, "recommendation", "")
            or title
        ),
        business_context=summary,
        context_requirements=[
            item
            for item in [
                str(metadata.get("decision_point") or ""),
                str(metadata.get("next_decision_trigger") or ""),
            ]
            if item
        ],
        required_evidence=snippets[:8]
        or ["Cited canonical events supporting the opportunity."],
        policies=[
            "Draft/review only until a human owner promotes this opportunity.",
            "Do not activate without cited examples, owner, reviewer, and replay checks.",
        ],
        acceptable_outputs=[
            "Draft skill or workflow spec with cited evidence and review owner"
        ],
        accept_reject_criteria=[
            "Every proposed skill/workflow cites canonical event evidence.",
            "The proposed output directly addresses the world-model counterfactual action.",
            "A reviewer can reject it if cited evidence does not support the action area.",
        ],
        evaluation_rubric=[
            "citation_coverage",
            "counterfactual_action_alignment",
            "human_review_readiness",
        ],
        observed_examples=[observed],
        reference_paths=[reference_path],
        source_event_ids=event_ids,
        source_case_ids=case_ids,
        open_questions=[
            "Should this become a new skill, an upgrade to an existing skill, or a workflow review item?",
            "Who owns and reviews the capability before activation?",
        ],
        spec_confidence=min(
            1.0, (0.65 * priority_score) + (0.35 * (1 - coverage_score))
        ),
        status=BusinessTaskStatus.DRAFT,
        evaluation_level=EvaluationLevel.RUBRIC_EVALUABLE,
        metadata={
            "candidate_id": candidate_id,
            "group_key": group_key,
            "generated_by": _WORLD_MODEL_OPPORTUNITY_GENERATOR,
            "source_gap_id": getattr(gap, "gap_id", ""),
            "claim_boundary": (
                "world-model counterfactual opportunity grounded in cited events; "
                "requires human review before becoming a skill or workflow"
            ),
            "world_model_opportunity": metadata,
        },
    )
    return WorkflowCandidate(
        candidate_id=candidate_id,
        title=title,
        company_name=company_name,
        company_domain=company_domain,
        group_key=group_key,
        source_case_ids=case_ids,
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
        repetition_count=max(1, len(case_ids)),
        evidence_density=min(1.0, len(events) / 5.0),
        cross_surface_score=min(1.0, len(surfaces) / 3.0) if surfaces else 0.0,
        escalation_score=priority_score,
        labelability_score=min(1.0, 0.65 + (0.35 * len(event_ids) / 4.0)),
        rank_score=round(min(1.0, (0.8 * priority_score) + 0.2) * 100.0, 4),
        summary=summary,
        snippets=snippets,
        draft_task_spec=draft_spec,
        metadata={
            "generated_by": _WORLD_MODEL_OPPORTUNITY_GENERATOR,
            "source_gap_id": getattr(gap, "gap_id", ""),
            "opportunity_kind": "missing_skill",
            "deployment_readiness": "needs_review",
            "priority_score": priority_score,
            "existing_skill_coverage_score": coverage_score,
            "event_count": len(events),
            "evidence_ref_count": len(getattr(gap, "evidence_refs", []) or []),
            "world_model_opportunity": metadata,
        },
    )


def _gap_evidence_texts(
    gap: Any, events: list[CanonicalEvent], *, limit: int = 8
) -> list[str]:
    reference_tokens = _tokens_for_match(
        " ".join(
            str(item)
            for item in [
                getattr(gap, "title", ""),
                getattr(gap, "reason", ""),
                getattr(gap, "recommendation", ""),
                getattr(gap, "metadata", {}),
            ]
        )
    )
    raw_texts = [
        _skill_ref_text(ref) for ref in getattr(gap, "evidence_refs", []) or []
    ]
    raw_texts.extend(_event_snippet(event) for event in events)
    seen: set[str] = set()
    scored: list[tuple[float, int, str]] = []
    for raw_text in raw_texts:
        text = _clean_text(_redact_workflow_text(raw_text), limit=240)
        if _is_low_signal_evidence(text) or text in seen:
            continue
        seen.add(text)
        scored.append(_evidence_text_score(text, reference_tokens))
    scored.sort(reverse=True)
    return [text for _score, _token_count, text in scored[:limit]]


def _safe_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _semantic_candidates_from_skill_map(
    *,
    skill_map: Any | None,
    events: list[CanonicalEvent],
    company_name: str,
    company_domain: str,
    world_model_rows: list[dict[str, Any]],
) -> list[WorkflowCandidate]:
    if skill_map is None:
        return []
    events_by_id = _events_by_id(events)
    candidates: list[WorkflowCandidate] = []
    for skill in getattr(skill_map, "skills", []) or []:
        candidate = _candidate_from_skill(
            skill=skill,
            events_by_id=events_by_id,
            company_name=company_name,
            company_domain=company_domain,
            world_model_rows=world_model_rows,
        )
        if candidate is not None:
            candidates.append(candidate)
    for gap in getattr(skill_map, "gaps", []) or []:
        candidate = _candidate_from_world_model_gap(
            gap=gap,
            events_by_id=events_by_id,
            company_name=company_name,
            company_domain=company_domain,
        )
        if candidate is not None:
            candidates.append(candidate)
    candidates.sort(
        key=lambda candidate: (
            candidate.rank_score,
            candidate.metadata.get("evidence_ref_count", 0),
            candidate.repetition_count,
            candidate.title,
        ),
        reverse=True,
    )
    return candidates


def mine_workflows(
    source_dir: str | Path,
    *,
    output: str | Path | None = None,
    limit: int = 25,
    skill_map_path: str | Path | None = None,
    world_model_report_path: str | Path | None = None,
) -> WorkflowMiningResult:
    """Mine semantic workflow candidates from a company skill map.

    A company skill map is required: VEI either accepts an explicit
    `skill_map_path` or discovers one at `<source>/skill_map/company_skill_map.json`.
    If no skill map is available, mining fails fast — produce one with
    `vei knowledge skillmap build` first.

    The result is descriptive. It produces candidates and draft task specs
    grounded in cited canonical evidence, not deterministic step graphs.
    """

    root, context_path = _resolve_source(source_dir)
    context_payload = _load_context_payload(context_path)
    company_name, company_domain = _company_from_context(context_payload)
    events = _load_events(root)

    resolved_skill_map_path = (
        Path(skill_map_path).expanduser().resolve()
        if skill_map_path is not None
        else _autodiscover_skill_map_path(root, output)
    )
    if resolved_skill_map_path is None:
        raise FileNotFoundError(
            "semantic workflow mining requires a company skill map. "
            "Pass --skill-map or place company_skill_map.json at "
            "<source>/skill_map/company_skill_map.json. Build one with "
            "`vei knowledge skillmap build` first."
        )
    skill_map = _load_skill_map(resolved_skill_map_path)
    resolved_world_model_report_path = _resolve_world_model_report_path(
        world_model_report_path
    )
    world_model_rows = _read_world_model_rows(resolved_world_model_report_path)
    semantic_candidates = _semantic_candidates_from_skill_map(
        skill_map=skill_map,
        events=events,
        company_name=company_name,
        company_domain=company_domain,
        world_model_rows=world_model_rows,
    )
    candidates = list(semantic_candidates)
    candidates.sort(
        key=lambda candidate: (
            (
                1
                if candidate.metadata.get("generated_by") == _SKILLMAP_GENERATOR
                or candidate.metadata.get("generated_by")
                == _WORLD_MODEL_OPPORTUNITY_GENERATOR
                else 0
            ),
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
            "mining_version": "workflow_mining_v2_semantic",
            "selected_backend": "semantic",
            "skill_map_path": str(resolved_skill_map_path),
            "world_model_report_path": (
                str(resolved_world_model_report_path)
                if resolved_world_model_report_path is not None
                else ""
            ),
            "semantic_candidate_count": len(semantic_candidates),
            "world_model_opportunity_candidate_count": sum(
                1
                for candidate in semantic_candidates
                if candidate.metadata.get("generated_by")
                == _WORLD_MODEL_OPPORTUNITY_GENERATOR
            ),
            "candidate_policy": (
                "skill-backed semantic workflows and cited world-model "
                "opportunities are the only candidate sources"
            ),
            "generated_at": _now_iso(),
        },
    )
    if output is not None:
        save_mining_result(result, output)
        output_root = Path(output).expanduser().resolve()
        manifest = {
            "schema_version": 1,
            "mining_version": result.metadata["mining_version"],
            "selected_backend": "semantic",
            "source_dir": str(context_path),
            "skill_map_path": result.metadata["skill_map_path"],
            "world_model_report_path": result.metadata["world_model_report_path"],
            "semantic_candidate_count": len(semantic_candidates),
            "world_model_opportunity_candidate_count": result.metadata[
                "world_model_opportunity_candidate_count"
            ],
            "published_candidate_count": result.candidate_count,
            "generated_at": result.metadata["generated_at"],
        }
        (output_root / MINING_MANIFEST_FILE).write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )
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
    selected_events_by_id = {
        event_id: events_by_id[event_id].model_dump(mode="json")
        for event_id in spec.source_event_ids
        if event_id in events_by_id
    }
    selected_events = list(selected_events_by_id.values())
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
            example_events = [
                selected_events_by_id[event_id]
                for event_id in example.event_ids
                if event_id in selected_events_by_id
            ]
            payload["events"] = example_events if example.event_ids else selected_events
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
    skill_map_path: str | Path | None = None,
    world_model_report_path: str | Path | None = None,
    refresh_wiki_artifacts: bool = False,
    refresh_skillmap_artifacts: bool = False,
) -> WorkflowRefreshReport:
    output_root = Path(output).expanduser().resolve()
    previous_result: WorkflowMiningResult | None = None
    if (output_root / MINING_RESULT_FILE).is_file():
        previous_result = load_mining_result(output_root)
    labels = load_workflow_labels(output_root)

    resolved_skill_map_path: str | Path | None = skill_map_path
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
            paths = write_company_skill_map_outputs(
                skill_map, output_root / "skill_map"
            )
            resolved_skill_map_path = paths["json"]
            skillmap_status = "ok"
        except Exception as exc:  # noqa: BLE001
            skillmap_status = f"error:{exc}"

    new_result = mine_workflows(
        source_dir,
        output=output_root,
        limit=limit,
        skill_map_path=resolved_skill_map_path,
        world_model_report_path=world_model_report_path,
    )

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
        metadata={
            "candidate_count": new_result.candidate_count,
            "selected_backend": new_result.metadata.get("selected_backend"),
            "semantic_candidate_count": new_result.metadata.get(
                "semantic_candidate_count"
            ),
        },
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
