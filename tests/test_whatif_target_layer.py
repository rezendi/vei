from __future__ import annotations

import pytest

from vei.whatif.corpus import event_reference
from vei.whatif.models import (
    WhatIfBenchmarkDatasetRow,
    WhatIfEvent,
    WhatIfPreBranchContract,
    WhatIfSemanticTargetLabel,
)
from vei.whatif.target_layer import (
    ALL_DOMAIN_HEAD_NAMES,
    STRUCTURAL_HEAD_NAMES,
    build_curated_targets,
    build_target_manifest,
    curated_target_values_and_mask,
    validate_semantic_target_label,
)


def test_structural_heads_are_mechanical_and_domain_labels_are_cited() -> None:
    branch = _event(
        event_id="branch",
        timestamp_ms=1_000,
        actor_id="owner@example.com",
        target_id="ops@example.com",
        snippet="Branch decision",
    )
    future = [
        _event(
            event_id="future-1",
            timestamp_ms=2_000,
            actor_id="ops@example.com",
            target_id="user@example.com",
            surface="teams",
            snippet=(
                "User is confused, blocked by a login issue, unclear owner, "
                "deadline missed."
            ),
        ),
        _event(
            event_id="future-2",
            timestamp_ms=4_000,
            actor_id="eng@example.com",
            target_id="ops@example.com",
            surface="outlook",
            snippet="Unblocked and resolved, but this needs rework again.",
        ),
        _event(
            event_id="future-3",
            timestamp_ms=6_000,
            actor_id="eng@example.com",
            target_id="qa@example.com",
            surface="onedrive",
            snippet="QA passed and the release is release ready.",
        ),
    ]

    targets = build_curated_targets(
        tenant_id="pyinsights",
        branch_event=branch,
        future_events=future,
    )

    assert targets.structural_heads["cycle_time_ms"] == 5_000.0
    assert targets.structural_heads["time_to_first_response_ms"] == 1_000.0
    assert targets.structural_heads["handoff_count"] == 2.0
    assert targets.structural_heads["blocked_duration_ms"] == 2_000.0
    assert targets.structural_heads["reopen_rework_count"] == 1.0
    assert targets.structural_heads["participant_fanout"] == 4.0
    assert targets.structural_heads["cross_system_spread"] == 3.0
    assert targets.structural_heads["owner_ambiguity_count"] == 1.0
    assert targets.structural_heads["deadline_sla_miss_count"] == 1.0
    assert targets.structural_heads["evidence_completeness"] == 1.0
    assert all(targets.head_masks[name] for name in STRUCTURAL_HEAD_NAMES)
    assert targets.head_masks["user_trust_confusion"] is True
    assert targets.head_masks["release_readiness"] is True
    assert targets.head_masks["data_coverage_gap"] is False
    assert targets.head_status["data_coverage_gap"] == "unsupported"
    assert {label.target_id for label in targets.semantic_labels} >= {
        "user_trust_confusion",
        "release_readiness",
    }
    assert all(label.evidence_event_ids for label in targets.semantic_labels)
    assert all(label.supporting_spans for label in targets.semantic_labels)


def test_semantic_label_validation_requires_future_event_citations() -> None:
    future = [
        _event(
            event_id="future-1",
            timestamp_ms=2_000,
            snippet="QA passed and release ready.",
        )
    ]

    label = validate_semantic_target_label(
        {
            "target_id": "release_readiness",
            "value": 0.8,
            "confidence": 0.7,
            "horizon": "30d",
            "evidence_event_ids": ["future-1"],
            "supporting_spans": ["release ready"],
            "negative_evidence": ["no rollback mentioned"],
            "label_source": "llm_semantic_v1",
        },
        future_events=future,
        supported_target_ids=["release_readiness"],
    )
    assert label.negative_evidence == ["no rollback mentioned"]

    with pytest.raises(ValueError, match="cited future evidence"):
        WhatIfSemanticTargetLabel(
            target_id="release_readiness",
            value=0.8,
            confidence=0.7,
            evidence_event_ids=[],
            supporting_spans=["release ready"],
        )

    with pytest.raises(ValueError, match="unknown future event"):
        validate_semantic_target_label(
            {
                "target_id": "release_readiness",
                "value": 0.8,
                "confidence": 0.7,
                "evidence_event_ids": ["missing"],
                "supporting_spans": ["release ready"],
            },
            future_events=future,
        )

    with pytest.raises(ValueError, match="supporting span"):
        validate_semantic_target_label(
            {
                "target_id": "release_readiness",
                "value": 0.8,
                "confidence": 0.7,
                "evidence_event_ids": ["future-1"],
                "supporting_spans": ["not in event"],
            },
            future_events=future,
        )


def test_target_manifest_reports_supported_experimental_and_unsupported_heads() -> None:
    branch = _event(event_id="branch", timestamp_ms=1_000)
    future = [
        _event(
            event_id="future-1",
            timestamp_ms=2_000,
            snippet="Customer committed to a paid pilot and the demo worked.",
        ),
        _event(
            event_id="future-2",
            timestamp_ms=3_000,
            snippet="GTM narrative aligned and the sales story is consistent.",
        ),
    ]
    curated = build_curated_targets(
        tenant_id="dispatch",
        branch_event=branch,
        future_events=future,
    )
    row = WhatIfBenchmarkDatasetRow(
        row_id="dispatch:row-1",
        thread_id="dispatch:thread-1",
        branch_event_id="dispatch:branch",
        contract=WhatIfPreBranchContract(
            case_id="dispatch:case-1",
            thread_id="dispatch:thread-1",
            branch_event_id="dispatch:branch",
            branch_event=event_reference(branch),
        ),
        curated_targets=curated,
    )

    manifest = build_target_manifest(tenant_id="dispatch", rows=[row])
    values, masks = curated_target_values_and_mask(curated)
    mask_by_name = dict(zip([*STRUCTURAL_HEAD_NAMES, *ALL_DOMAIN_HEAD_NAMES], masks))

    assert set(STRUCTURAL_HEAD_NAMES).issubset(manifest.supported_heads)
    assert set(manifest.experimental_heads) == {
        "gtm_narrative_consistency",
        "partner_customer_traction",
        "product_readiness_proof",
    }
    assert "onboarding_integrity" in manifest.unsupported_heads
    assert manifest.coverage["cycle_time_ms"] == 1.0
    assert manifest.coverage["product_readiness_proof"] == 1.0
    assert manifest.confidence["product_readiness_proof"] > 0.0
    assert "enterprise_risk" in manifest.proxy_debug_heads
    assert mask_by_name["onboarding_integrity"] == 0.0
    assert values


def _event(
    *,
    event_id: str,
    timestamp_ms: int,
    actor_id: str = "actor@example.com",
    target_id: str = "target@example.com",
    event_type: str = "message",
    surface: str = "mail",
    snippet: str = "",
) -> WhatIfEvent:
    return WhatIfEvent(
        event_id=event_id,
        timestamp=f"1970-01-01T00:00:{timestamp_ms // 1000:02d}Z",
        timestamp_ms=timestamp_ms,
        actor_id=actor_id,
        target_id=target_id,
        event_type=event_type,
        thread_id="thread-1",
        case_id="case-1",
        surface=surface,
        subject="Target layer fixture",
        snippet=snippet,
    )
