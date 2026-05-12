from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Sequence

from .models import (
    WhatIfBenchmarkDatasetRow,
    WhatIfCuratedTargetSet,
    WhatIfEvent,
    WhatIfSemanticTargetLabel,
    WhatIfTargetManifest,
)

CURATED_TARGET_LAYER_VERSION = "curated_target_layer_v0"
TARGET_MANIFEST_VERSION = "target_manifest_v0"
STRUCTURAL_LABEL_FUNCTION_VERSION = "structural_metrics_v0"
CURATION_PACK_LABEL_FUNCTION_VERSION = "curation_pack_keyword_v0"
LLM_SEMANTIC_LABEL_SOURCE_VERSION = "llm_semantic_v1"
PROXY_GLOBAL_HEAD_VERSION = "proxy_global_v1"

STRUCTURAL_HEAD_NAMES: tuple[str, ...] = (
    "cycle_time_ms",
    "handoff_count",
    "time_to_first_response_ms",
    "reopen_rework_count",
    "blocked_duration_ms",
    "participant_fanout",
    "cross_system_spread",
    "owner_ambiguity_count",
    "deadline_sla_miss_count",
    "evidence_completeness",
)

DOMAIN_HEADS_BY_TENANT: dict[str, tuple[str, ...]] = {
    "pyinsights": (
        "onboarding_integrity",
        "user_trust_confusion",
        "release_readiness",
    ),
    "powrofyou": (
        "data_coverage_gap",
        "compliance_privacy_sensitivity",
        "repeated_clarification_loop",
    ),
    "dispatch": (
        "product_readiness_proof",
        "gtm_narrative_consistency",
        "partner_customer_traction",
    ),
}

PROXY_DEBUG_HEAD_NAMES: tuple[str, ...] = (
    "enterprise_risk",
    "commercial_position_proxy",
    "org_strain_proxy",
    "stakeholder_trust",
    "execution_drag",
    "regulatory_exposure",
    "accounting_control_pressure",
    "liquidity_stress",
    "governance_response",
    "evidence_control",
    "external_confidence_pressure",
)
SEMANTIC_LABEL_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "target_id",
        "value",
        "confidence",
        "horizon",
        "evidence_event_ids",
        "supporting_spans",
        "negative_evidence",
        "label_source",
    ],
    "properties": {
        "target_id": {"type": "string"},
        "value": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "horizon": {"type": "string"},
        "evidence_event_ids": {"type": "array", "items": {"type": "string"}},
        "supporting_spans": {"type": "array", "items": {"type": "string"}},
        "negative_evidence": {"type": "array", "items": {"type": "string"}},
        "label_source": {"type": "string"},
    },
}

ALL_DOMAIN_HEAD_NAMES: tuple[str, ...] = tuple(
    sorted({head for heads in DOMAIN_HEADS_BY_TENANT.values() for head in heads})
)
CURATED_TARGET_HEAD_NAMES: tuple[str, ...] = (
    *STRUCTURAL_HEAD_NAMES,
    *ALL_DOMAIN_HEAD_NAMES,
)

HIGHER_IS_BETTER: dict[str, bool] = {
    "cycle_time_ms": False,
    "handoff_count": False,
    "time_to_first_response_ms": False,
    "reopen_rework_count": False,
    "blocked_duration_ms": False,
    "participant_fanout": False,
    "cross_system_spread": False,
    "owner_ambiguity_count": False,
    "deadline_sla_miss_count": False,
    "evidence_completeness": True,
    "onboarding_integrity": True,
    "user_trust_confusion": False,
    "release_readiness": True,
    "data_coverage_gap": False,
    "compliance_privacy_sensitivity": False,
    "repeated_clarification_loop": False,
    "product_readiness_proof": True,
    "gtm_narrative_consistency": True,
    "partner_customer_traction": True,
}


@dataclass(frozen=True)
class _DomainLabelSpec:
    target_id: str
    higher_is_better: bool
    positive_terms: tuple[str, ...]
    risk_terms: tuple[str, ...]


_DOMAIN_LABEL_SPECS: dict[str, tuple[_DomainLabelSpec, ...]] = {
    "pyinsights": (
        _DomainLabelSpec(
            target_id="onboarding_integrity",
            higher_is_better=True,
            positive_terms=(
                "onboarded",
                "onboarding complete",
                "setup complete",
                "activated",
                "welcome flow",
            ),
            risk_terms=(
                "onboarding failed",
                "setup failed",
                "login issue",
                "cannot log in",
                "access problem",
                "confused",
            ),
        ),
        _DomainLabelSpec(
            target_id="user_trust_confusion",
            higher_is_better=False,
            positive_terms=("resolved", "clear", "confirmed", "reassured"),
            risk_terms=(
                "confused",
                "not clear",
                "lost trust",
                "trust issue",
                "privacy concern",
                "why is this happening",
                "unexpected",
            ),
        ),
        _DomainLabelSpec(
            target_id="release_readiness",
            higher_is_better=True,
            positive_terms=(
                "release ready",
                "qa passed",
                "shipped",
                "deploy complete",
                "ready to release",
            ),
            risk_terms=(
                "blocker",
                "regression",
                "rollback",
                "failed qa",
                "not ready",
                "bug",
            ),
        ),
    ),
    "powrofyou": (
        _DomainLabelSpec(
            target_id="data_coverage_gap",
            higher_is_better=False,
            positive_terms=("coverage complete", "sample complete", "data received"),
            risk_terms=(
                "coverage gap",
                "missing data",
                "insufficient sample",
                "data gap",
                "not enough responses",
                "incomplete panel",
            ),
        ),
        _DomainLabelSpec(
            target_id="compliance_privacy_sensitivity",
            higher_is_better=False,
            positive_terms=("approved privacy", "consent confirmed", "dpi approved"),
            risk_terms=(
                "privacy",
                "consent",
                "gdpr",
                "personal data",
                "sensitive data",
                "compliance",
                "legal review",
            ),
        ),
        _DomainLabelSpec(
            target_id="repeated_clarification_loop",
            higher_is_better=False,
            positive_terms=("confirmed", "aligned", "signed off"),
            risk_terms=(
                "clarify",
                "clarification",
                "again",
                "not what we meant",
                "can you explain",
                "still unclear",
            ),
        ),
    ),
    "dispatch": (
        _DomainLabelSpec(
            target_id="product_readiness_proof",
            higher_is_better=True,
            positive_terms=(
                "demo worked",
                "pilot passed",
                "readiness proof",
                "customer proof",
                "product ready",
                "working build",
            ),
            risk_terms=("blocked", "not ready", "bug", "failed demo", "missing proof"),
        ),
        _DomainLabelSpec(
            target_id="gtm_narrative_consistency",
            higher_is_better=True,
            positive_terms=(
                "gtm",
                "positioning aligned",
                "narrative aligned",
                "message consistent",
                "sales story",
            ),
            risk_terms=(
                "narrative mismatch",
                "positioning unclear",
                "mixed message",
                "inconsistent story",
                "unclear pitch",
            ),
        ),
        _DomainLabelSpec(
            target_id="partner_customer_traction",
            higher_is_better=True,
            positive_terms=(
                "customer committed",
                "partner committed",
                "pilot signed",
                "intro booked",
                "paid pilot",
                "traction",
            ),
            risk_terms=(
                "no response",
                "churn",
                "declined",
                "lost partner",
                "no customer",
            ),
        ),
    ),
}

_REWORK_TERMS = (
    "reopen",
    "re-open",
    "rework",
    "redo",
    "again",
    "regression",
    "not fixed",
    "still failing",
    "fix again",
)
_BLOCKED_TERMS = (
    "blocked",
    "waiting",
    "stuck",
    "cannot proceed",
    "can't proceed",
    "on hold",
)
_UNBLOCKED_TERMS = (
    "unblocked",
    "resolved",
    "fixed",
    "done",
    "shipped",
    "closed",
    "complete",
)
_OWNER_AMBIGUITY_TERMS = (
    "who owns",
    "owner?",
    "unclear owner",
    "not sure who",
    "someone needs",
    "anyone able",
    "no owner",
    "unassigned",
)
_DEADLINE_TERMS = (
    "deadline",
    "sla",
    "missed",
    "late",
    "overdue",
    "delayed",
    "by eod",
    "end of day",
)


def normalize_target_tenant_id(tenant_id: str) -> str:
    normalized = "".join(ch for ch in tenant_id.lower() if ch.isalnum())
    aliases = {
        "pyinsights": "pyinsights",
        "pyinsight": "pyinsights",
        "powrofyou": "powrofyou",
        "poy": "powrofyou",
        "powerofyou": "powrofyou",
        "dispatch": "dispatch",
    }
    return aliases.get(normalized, normalized)


def domain_heads_for_tenant(tenant_id: str) -> tuple[str, ...]:
    return DOMAIN_HEADS_BY_TENANT.get(normalize_target_tenant_id(tenant_id), ())


def build_curated_targets(
    *,
    tenant_id: str,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
) -> WhatIfCuratedTargetSet:
    normalized_tenant = normalize_target_tenant_id(tenant_id)
    structural_heads = summarize_structural_heads(
        branch_event=branch_event,
        future_events=future_events,
    )
    semantic_labels = derive_domain_semantic_labels(
        tenant_id=normalized_tenant,
        future_events=future_events,
    )
    domain_heads: dict[str, float] = {}
    head_masks: dict[str, bool] = {}
    head_confidence: dict[str, float] = {}
    head_status: dict[str, str] = {}

    for name in STRUCTURAL_HEAD_NAMES:
        head_masks[name] = bool(future_events)
        head_confidence[name] = 1.0 if future_events else 0.0
        head_status[name] = "supported"

    accepted_by_target = {label.target_id: label for label in semantic_labels}
    for name in domain_heads_for_tenant(normalized_tenant):
        label = accepted_by_target.get(name)
        if label is None:
            domain_heads[name] = 0.0
            head_masks[name] = False
            head_confidence[name] = 0.0
        else:
            domain_heads[name] = label.value
            head_masks[name] = True
            head_confidence[name] = label.confidence
        head_status[name] = "experimental"

    unsupported = set(ALL_DOMAIN_HEAD_NAMES) - set(
        domain_heads_for_tenant(normalized_tenant)
    )
    for name in unsupported:
        head_masks[name] = False
        head_confidence[name] = 0.0
        head_status[name] = "unsupported"

    return WhatIfCuratedTargetSet(
        version=CURATED_TARGET_LAYER_VERSION,
        tenant_id=tenant_id,
        domain_pack_id=(
            f"{normalized_tenant}:domain_pack_v0"
            if domain_heads_for_tenant(normalized_tenant)
            else ""
        ),
        structural_heads=structural_heads,
        domain_heads=domain_heads,
        head_masks=head_masks,
        head_confidence=head_confidence,
        head_status=head_status,
        semantic_labels=semantic_labels,
        label_functions=[
            STRUCTURAL_LABEL_FUNCTION_VERSION,
            CURATION_PACK_LABEL_FUNCTION_VERSION,
        ],
        proxy_head_version=PROXY_GLOBAL_HEAD_VERSION,
    )


def summarize_structural_heads(
    *,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
) -> dict[str, float]:
    ordered = sorted(
        future_events, key=lambda event: (event.timestamp_ms, event.event_id)
    )
    if not ordered:
        return {name: 0.0 for name in STRUCTURAL_HEAD_NAMES}

    first_ts = ordered[0].timestamp_ms
    last_ts = ordered[-1].timestamp_ms
    actors = {
        value
        for event in ordered
        for value in (event.actor_id, event.target_id)
        if value
    }
    handoff_count = 0
    previous_actor = branch_event.actor_id
    for event in ordered:
        if previous_actor and event.actor_id and event.actor_id != previous_actor:
            handoff_count += 1
        previous_actor = event.actor_id or previous_actor

    blocked_duration_ms = 0
    block_start: int | None = None
    for event in ordered:
        text = _event_text(event)
        if block_start is None and _contains_any(text, _BLOCKED_TERMS):
            block_start = event.timestamp_ms
        elif block_start is not None and _contains_any(text, _UNBLOCKED_TERMS):
            blocked_duration_ms += max(0, event.timestamp_ms - block_start)
            block_start = None
    if block_start is not None:
        blocked_duration_ms += max(0, last_ts - block_start)

    return {
        "cycle_time_ms": float(max(0, last_ts - branch_event.timestamp_ms)),
        "handoff_count": float(handoff_count),
        "time_to_first_response_ms": float(
            max(0, first_ts - branch_event.timestamp_ms)
        ),
        "reopen_rework_count": float(_count_events_with_terms(ordered, _REWORK_TERMS)),
        "blocked_duration_ms": float(blocked_duration_ms),
        "participant_fanout": float(len(actors)),
        "cross_system_spread": float(
            len({event.surface for event in ordered if event.surface})
        ),
        "owner_ambiguity_count": float(
            _count_events_with_terms(ordered, _OWNER_AMBIGUITY_TERMS)
        ),
        "deadline_sla_miss_count": float(
            _count_events_with_terms(ordered, _DEADLINE_TERMS)
        ),
        "evidence_completeness": round(_evidence_completeness(ordered), 6),
    }


def derive_domain_semantic_labels(
    *,
    tenant_id: str,
    future_events: Sequence[WhatIfEvent],
) -> list[WhatIfSemanticTargetLabel]:
    normalized_tenant = normalize_target_tenant_id(tenant_id)
    labels: list[WhatIfSemanticTargetLabel] = []
    for spec in _DOMAIN_LABEL_SPECS.get(normalized_tenant, ()):
        label = _derive_label_from_spec(spec, future_events=future_events)
        if label is not None:
            labels.append(label)
    return labels


def validate_semantic_target_label(
    payload: WhatIfSemanticTargetLabel | dict[str, Any],
    *,
    future_events: Sequence[WhatIfEvent],
    supported_target_ids: Sequence[str] | None = None,
) -> WhatIfSemanticTargetLabel:
    label = (
        payload
        if isinstance(payload, WhatIfSemanticTargetLabel)
        else WhatIfSemanticTargetLabel.model_validate(payload)
    )
    supported = set(supported_target_ids or [])
    if supported and label.target_id not in supported:
        raise ValueError(f"unsupported semantic target id: {label.target_id}")
    events_by_id = {event.event_id: event for event in future_events}
    missing = [
        event_id
        for event_id in label.evidence_event_ids
        if event_id not in events_by_id
    ]
    if missing:
        raise ValueError(f"semantic label cites unknown future event id(s): {missing}")
    cited_text = "\n".join(
        _event_text(events_by_id[event_id]) for event_id in label.evidence_event_ids
    )
    normalized_cited_text = cited_text.lower()
    unsupported_spans = [
        span
        for span in label.supporting_spans
        if span.lower() not in normalized_cited_text
    ]
    if unsupported_spans:
        raise ValueError(
            f"semantic label supporting span(s) not found: {unsupported_spans}"
        )
    return label


def semantic_labeling_window_payload(
    *,
    tenant_id: str,
    window_id: str,
    future_events: Sequence[WhatIfEvent],
) -> dict[str, Any]:
    return {
        "window_id": window_id,
        "tenant_id": tenant_id,
        "target_ids": list(domain_heads_for_tenant(tenant_id)),
        "label_source": LLM_SEMANTIC_LABEL_SOURCE_VERSION,
        "schema": SEMANTIC_LABEL_RESPONSE_SCHEMA,
        "requirements": [
            "label only the listed target_ids",
            "cite at least one future event id for every accepted label",
            "include supporting spans copied from the cited future events",
            "return no label when evidence is absent or only inferred",
            "preserve negative evidence separately from supporting evidence",
        ],
        "future_events": [
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "timestamp_ms": event.timestamp_ms,
                "actor_id": event.actor_id,
                "target_id": event.target_id,
                "event_type": event.event_type,
                "thread_id": event.thread_id,
                "case_id": event.case_id,
                "surface": event.surface,
                "subject": event.subject,
                "snippet": event.snippet,
            }
            for event in future_events
        ],
    }


def build_target_manifest(
    *,
    tenant_id: str,
    rows: Sequence[WhatIfBenchmarkDatasetRow],
    reviewed_examples: Sequence[dict[str, Any]] | None = None,
) -> WhatIfTargetManifest:
    normalized_tenant = normalize_target_tenant_id(tenant_id)
    tenant_domain_heads = set(domain_heads_for_tenant(normalized_tenant))
    row_count = len(rows)
    mask_counts: dict[str, int] = defaultdict(int)
    confidence_values: dict[str, list[float]] = defaultdict(list)
    label_functions: set[str] = set()
    examples: list[dict[str, Any]] = []

    for row in rows:
        targets = row.curated_targets
        label_functions.update(targets.label_functions)
        for head_name, enabled in targets.head_masks.items():
            if enabled:
                mask_counts[head_name] += 1
                confidence_values[head_name].append(
                    float(targets.head_confidence.get(head_name, 0.0))
                )
        for label in targets.semantic_labels:
            if len(examples) >= 5:
                break
            examples.append(
                {
                    "row_id": row.row_id,
                    "target_id": label.target_id,
                    "value": label.value,
                    "confidence": label.confidence,
                    "evidence_event_ids": label.evidence_event_ids,
                    "label_source": label.label_source,
                }
            )

    all_supported = list(STRUCTURAL_HEAD_NAMES)
    experimental = sorted(tenant_domain_heads)
    unsupported = sorted(set(ALL_DOMAIN_HEAD_NAMES) - tenant_domain_heads)
    coverage = {
        name: round(mask_counts[name] / row_count, 6) if row_count else 0.0
        for name in CURATED_TARGET_HEAD_NAMES
    }
    confidence = {
        name: (
            round(
                sum(confidence_values[name]) / len(confidence_values[name]),
                6,
            )
            if confidence_values[name]
            else 0.0
        )
        for name in CURATED_TARGET_HEAD_NAMES
    }
    return WhatIfTargetManifest(
        version=TARGET_MANIFEST_VERSION,
        target_layer_version=CURATED_TARGET_LAYER_VERSION,
        tenant_id=tenant_id,
        domain_pack_id=(
            f"{normalized_tenant}:domain_pack_v0" if tenant_domain_heads else ""
        ),
        row_count=row_count,
        supported_heads=all_supported,
        experimental_heads=experimental,
        unsupported_heads=unsupported,
        proxy_debug_heads=list(PROXY_DEBUG_HEAD_NAMES),
        label_functions_used=sorted(label_functions),
        coverage=coverage,
        confidence=confidence,
        reviewed_examples=list(reviewed_examples or []) + examples[:20],
        known_blind_spots=[
            "Domain heads are v0 curated weak labels until offline LLM and human review calibration lands.",
            "Domain heads without cited future events are masked out, not zero-filled.",
            "Proxy global heads remain available only as diagnostics unless explicitly marked experimental.",
        ],
        metadata={
            "structural_head_count": len(STRUCTURAL_HEAD_NAMES),
            "tenant_domain_head_count": len(tenant_domain_heads),
            "all_curated_head_count": len(CURATED_TARGET_HEAD_NAMES),
            "proxy_head_version": PROXY_GLOBAL_HEAD_VERSION,
        },
    )


def curated_target_values_and_mask(
    targets: WhatIfCuratedTargetSet,
) -> tuple[list[float], list[float]]:
    values: list[float] = []
    masks: list[float] = []
    for name in CURATED_TARGET_HEAD_NAMES:
        if name in targets.structural_heads:
            values.append(float(targets.structural_heads.get(name, 0.0)))
        else:
            values.append(float(targets.domain_heads.get(name, 0.0)))
        masks.append(1.0 if targets.head_masks.get(name, False) else 0.0)
    return values, masks


def curated_target_score(
    heads: dict[str, float],
    *,
    masks: dict[str, bool] | None = None,
) -> float | None:
    values: list[float] = []
    for name, raw_value in heads.items():
        if masks is not None and not masks.get(name, False):
            continue
        if name not in HIGHER_IS_BETTER:
            continue
        normalized = _normalize_for_score(name, float(raw_value))
        values.append(normalized if HIGHER_IS_BETTER[name] else 1.0 - normalized)
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def curated_target_score_components(
    heads: dict[str, float],
    *,
    masks: dict[str, bool] | None = None,
) -> dict[str, float]:
    components: dict[str, float] = {}
    for name, raw_value in heads.items():
        if masks is not None and not masks.get(name, False):
            continue
        if name not in HIGHER_IS_BETTER:
            continue
        normalized = _normalize_for_score(name, float(raw_value))
        components[name] = round(
            normalized if HIGHER_IS_BETTER[name] else 1.0 - normalized,
            6,
        )
    return components


def _derive_label_from_spec(
    spec: _DomainLabelSpec,
    *,
    future_events: Sequence[WhatIfEvent],
) -> WhatIfSemanticTargetLabel | None:
    positive_matches = _event_term_matches(future_events, spec.positive_terms)
    risk_matches = _event_term_matches(future_events, spec.risk_terms)
    if not positive_matches and not risk_matches:
        return None

    cited_ids = sorted(set(positive_matches) | set(risk_matches))
    supporting_spans = sorted(
        {
            term
            for matches in (positive_matches, risk_matches)
            for terms in matches.values()
            for term in terms
        }
    )
    positive_count = sum(len(terms) for terms in positive_matches.values())
    risk_count = sum(len(terms) for terms in risk_matches.values())
    if spec.higher_is_better:
        value = 0.5 + min(0.4, positive_count * 0.16) - min(0.4, risk_count * 0.16)
        negative_evidence = sorted(
            {term for terms in risk_matches.values() for term in terms}
        )
    else:
        value = 0.15 + min(0.75, risk_count * 0.22) - min(0.25, positive_count * 0.12)
        negative_evidence = sorted(
            {term for terms in positive_matches.values() for term in terms}
        )
    confidence = min(0.9, 0.55 + (0.08 * len(cited_ids)))
    return WhatIfSemanticTargetLabel(
        target_id=spec.target_id,
        value=round(min(1.0, max(0.0, value)), 6),
        confidence=round(confidence, 6),
        horizon="future_tail",
        evidence_event_ids=cited_ids,
        supporting_spans=supporting_spans,
        negative_evidence=negative_evidence,
        label_source=CURATION_PACK_LABEL_FUNCTION_VERSION,
    )


def _event_term_matches(
    events: Sequence[WhatIfEvent],
    terms: Sequence[str],
) -> dict[str, list[str]]:
    matches: dict[str, list[str]] = {}
    for event in events:
        text = _event_text(event)
        event_matches = [term for term in terms if term in text]
        if event_matches:
            matches[event.event_id] = event_matches
    return matches


def _count_events_with_terms(
    events: Sequence[WhatIfEvent],
    terms: Sequence[str],
) -> int:
    return sum(1 for event in events if _contains_any(_event_text(event), terms))


def _contains_any(text: str, terms: Sequence[str]) -> bool:
    return any(term in text for term in terms)


def _event_text(event: WhatIfEvent) -> str:
    return " ".join(
        [
            event.event_type,
            event.surface,
            event.subject,
            event.snippet,
            event.flags.subject,
            event.flags.folder,
            event.flags.source,
        ]
    ).lower()


def _evidence_completeness(events: Sequence[WhatIfEvent]) -> float:
    if not events:
        return 0.0
    required_field_count = 6
    score = 0
    for event in events:
        score += int(bool(event.event_id))
        score += int(bool(event.timestamp_ms))
        score += int(bool(event.actor_id))
        score += int(bool(event.event_type))
        score += int(bool(event.thread_id or event.case_id))
        score += int(bool(event.surface or event.snippet or event.subject))
    return score / (len(events) * required_field_count)


def _normalize_for_score(name: str, value: float) -> float:
    if name.endswith("_ms"):
        return min(1.0, max(0.0, value / (7 * 86_400_000.0)))
    if name in {
        "handoff_count",
        "reopen_rework_count",
        "owner_ambiguity_count",
        "deadline_sla_miss_count",
    }:
        return min(1.0, max(0.0, value / 5.0))
    if name in {"participant_fanout", "cross_system_spread"}:
        return min(1.0, max(0.0, value / 8.0))
    return min(1.0, max(0.0, value))
