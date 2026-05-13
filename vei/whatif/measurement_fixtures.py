"""Freeze the current handwritten keyword bags into a manifest fixture.

This is the regression oracle for the kill-hardcoded-keywords migration. It
walks the term tuples that live in source today and emits a manifest that
*exactly* represents what the runtime applies to a given tenant.
"""

from __future__ import annotations

from typing import Iterable

from .measurement_manifest import (
    ManifestHead,
    ManifestTerm,
    TenantMeasurementManifest,
    now_iso,
)
from .target_layer import (
    CURATED_TARGET_HEAD_NAMES,
    _BLOCKED_TERMS,
    _DEADLINE_TERMS,
    _DOMAIN_LABEL_SPECS,
    _OWNER_AMBIGUITY_TERMS,
    _REWORK_TERMS,
    _UNBLOCKED_TERMS,
    normalize_target_tenant_id,
)

PROXY_DEBUG_TERM_BAGS: dict[str, tuple[str, ...]] = {
    "regulatory_exposure": (
        "sec",
        "regulator",
        "regulatory",
        "investigation",
        "formal investigation",
        "subpoena",
        "consent",
        "permission",
        "privacy",
        "compliance",
    ),
    "accounting_control_pressure": (
        "accounting",
        "audit",
        "auditor",
        "controller",
        "ledger",
        "reconciliation",
        "restatement",
        "q3",
        "third quarter",
        "non-recurring",
        "charge",
        "trial balance",
    ),
    "liquidity_stress": (
        "liquidity",
        "credit",
        "downgrade",
        "moody",
        "fitch",
        "debt",
        "cash",
        "treasury",
        "bankruptcy",
        "stock",
        "runway",
        "fundraising",
        "investor",
    ),
    "governance_response": (
        "board",
        "committee",
        "executive",
        "ceo",
        "cfo",
        "gc",
        "general counsel",
        "outside counsel",
        "legal",
        "approval",
        "gate",
    ),
    "evidence_control": (
        "document",
        "evidence",
        "packet",
        "inventory",
        "review",
        "reconcile",
        "memo",
        "minutes",
        "data room",
        "vsa",
        "soc 2",
        "dashboard",
    ),
    "external_confidence_pressure": (
        "investor",
        "creditor",
        "client",
        "customer",
        "downgrade",
        "stock",
        "press",
        "public",
        "external",
        "trust",
        "confidence",
    ),
}


def _fixture_terms(terms: Iterable[str]) -> list[ManifestTerm]:
    return [ManifestTerm(term=t, source="fixture", confidence=1.0) for t in terms]


def _structural_vocab_heads() -> list[ManifestHead]:
    """Heads that look structural by source layout but read a keyword bag."""
    return [
        ManifestHead(
            name="reopen_rework_count",
            registry_target_id="reopen_rework_count",
            role="structural_vocab",
            rationale="Counts events whose text contains rework-indicator terms.",
            risk_terms=_fixture_terms(_REWORK_TERMS),
        ),
        ManifestHead(
            name="blocked_duration_ms",
            registry_target_id="blocked_duration_ms",
            role="structural_vocab",
            rationale=(
                "Accumulates time between block-entry and block-exit triggers. "
                "Risk terms start the interval; positive terms end it."
            ),
            risk_terms=_fixture_terms(_BLOCKED_TERMS),
            positive_terms=_fixture_terms(_UNBLOCKED_TERMS),
        ),
        ManifestHead(
            name="owner_ambiguity_count",
            registry_target_id="owner_ambiguity_count",
            role="structural_vocab",
            rationale="Counts events whose text signals unclear ownership.",
            risk_terms=_fixture_terms(_OWNER_AMBIGUITY_TERMS),
        ),
        ManifestHead(
            name="deadline_sla_miss_count",
            registry_target_id="deadline_sla_miss_count",
            role="structural_vocab",
            rationale="Counts events whose text references deadline or SLA pressure.",
            risk_terms=_fixture_terms(_DEADLINE_TERMS),
        ),
    ]


def _proxy_debug_heads() -> list[ManifestHead]:
    """Diagnostic-only heads applied globally regardless of tenant fit."""
    heads: list[ManifestHead] = []
    for name, terms in PROXY_DEBUG_TERM_BAGS.items():
        registry_id = name if name in CURATED_TARGET_HEAD_NAMES else ""
        heads.append(
            ManifestHead(
                name=name,
                registry_target_id=registry_id,
                role="proxy_debug",
                rationale=(
                    "Proxy/diagnostic head computed from a global keyword bag in "
                    "benchmark_business.py; applied to every tenant today."
                ),
                positive_terms=_fixture_terms(terms),
            )
        )
    return heads


def _domain_heads_for_tenant(tenant_id: str) -> list[ManifestHead]:
    normalized = normalize_target_tenant_id(tenant_id)
    specs = _DOMAIN_LABEL_SPECS.get(normalized, ())
    heads: list[ManifestHead] = []
    for spec in specs:
        heads.append(
            ManifestHead(
                name=spec.target_id,
                registry_target_id=spec.target_id,
                role="domain",
                rationale=(
                    f"Tenant-specific head for `{normalized}`; positive and risk "
                    "bags are hand-tuned in target_layer.py."
                ),
                positive_terms=_fixture_terms(spec.positive_terms),
                risk_terms=_fixture_terms(spec.risk_terms),
            )
        )
    return heads


def extract_fixture_manifest(
    tenant_id: str,
    *,
    org_context: dict | None = None,
) -> TenantMeasurementManifest:
    """Build a manifest that mirrors what the runtime applies to `tenant_id` today.

    Tenants without a `_DomainLabelSpec` entry (e.g. enron) get only the
    structural-vocab and proxy-debug heads, which is exactly what the runtime
    does for them.
    """
    normalized = normalize_target_tenant_id(tenant_id)
    manifest = TenantMeasurementManifest(
        tenant_id=normalized,
        generated_at=now_iso(),
        org_context=org_context or {},
        provenance={
            "source": "fixture",
            "extractor": "vei.whatif.measurement_fixtures.extract_fixture_manifest",
            "note": (
                "Frozen snapshot of handwritten keyword bags as of extraction. "
                "Treat as a regression oracle, not as ground truth."
            ),
        },
        heads=[
            *_structural_vocab_heads(),
            *_proxy_debug_heads(),
            *_domain_heads_for_tenant(normalized),
        ],
    )
    return manifest


def known_fixture_tenants() -> list[str]:
    return sorted(_DOMAIN_LABEL_SPECS.keys())


__all__ = [
    "extract_fixture_manifest",
    "known_fixture_tenants",
    "PROXY_DEBUG_TERM_BAGS",
]
