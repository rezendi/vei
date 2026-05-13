from __future__ import annotations

from pathlib import Path

import pytest

from vei.whatif.measurement_diff import diff_manifests, render_diff_markdown
from vei.whatif.measurement_fixtures import (
    extract_fixture_manifest,
    known_fixture_tenants,
)
from vei.whatif.measurement_manifest import (
    ManifestHead,
    ManifestTerm,
    TenantMeasurementManifest,
    load_manifest,
    save_manifest,
)
from vei.whatif.target_layer import (
    _BLOCKED_TERMS,
    _DOMAIN_LABEL_SPECS,
    _REWORK_TERMS,
)

pytestmark = pytest.mark.unit


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = TenantMeasurementManifest(
        tenant_id="example",
        generated_at="2026-01-01T00:00:00Z",
        heads=[
            ManifestHead(
                name="rework",
                registry_target_id="reopen_rework_count",
                role="structural_vocab",
                rationale="counts rework events",
                risk_terms=[
                    ManifestTerm(term="rework", source="fixture"),
                    ManifestTerm(term="redo", source="fixture"),
                ],
            )
        ],
    )
    path = tmp_path / "m.json"
    save_manifest(manifest, path)
    reloaded = load_manifest(path)
    assert reloaded.tenant_id == "example"
    assert reloaded.heads[0].name == "rework"
    assert {t.term for t in reloaded.heads[0].risk_terms} == {"rework", "redo"}


def test_fixture_extracts_known_tenants() -> None:
    tenants = known_fixture_tenants()
    assert {"pyinsights", "powrofyou", "dispatch"} <= set(tenants)


@pytest.mark.parametrize("tenant", ["pyinsights", "powrofyou", "dispatch"])
def test_fixture_includes_domain_specs(tenant: str) -> None:
    manifest = extract_fixture_manifest(tenant_id=tenant)
    spec_names = {spec.target_id for spec in _DOMAIN_LABEL_SPECS[tenant]}
    domain_head_names = {h.name for h in manifest.heads if h.role == "domain"}
    assert spec_names == domain_head_names


def test_fixture_for_unknown_tenant_has_no_domain_heads() -> None:
    manifest = extract_fixture_manifest(tenant_id="enron")
    assert manifest.tenant_id == "enron"
    assert not any(h.role == "domain" for h in manifest.heads)
    # Structural-vocab and proxy-debug heads still apply globally.
    roles = {h.role for h in manifest.heads}
    assert {"structural_vocab", "proxy_debug"} <= roles


def test_fixture_rework_terms_match_source() -> None:
    manifest = extract_fixture_manifest(tenant_id="enron")
    rework = manifest.head("reopen_rework_count")
    assert rework is not None
    assert {t.term for t in rework.risk_terms} == set(_REWORK_TERMS)


def test_fixture_blocked_terms_match_source() -> None:
    manifest = extract_fixture_manifest(tenant_id="enron")
    blocked = manifest.head("blocked_duration_ms")
    assert blocked is not None
    assert {t.term for t in blocked.risk_terms} == set(_BLOCKED_TERMS)


def test_diff_self_is_perfect_overlap() -> None:
    manifest = extract_fixture_manifest(tenant_id="pyinsights")
    diff = diff_manifests(manifest, manifest)
    for head in diff.head_diffs:
        for term_diff in head.term_diffs:
            assert term_diff.jaccard == pytest.approx(1.0)
            assert term_diff.only_in_a == ()
            assert term_diff.only_in_b == ()


def test_proposer_term_normalization_splits_slashes_and_normalizes_hyphens() -> None:
    from vei.whatif.measurement_proposer import _coerce_terms

    raw = [
        {"term": "form 10‑K / annual report", "cited_event_ids": ["evt-1"]},
        {"term": "BBB+", "cited_event_ids": ["evt-2"]},
        {"term": "BBB+", "cited_event_ids": ["evt-3"]},  # duplicate after lowercase
        {"term": "cut to junk", "cited_event_ids": []},
        {"term": "  ", "cited_event_ids": []},  # empty after strip
    ]
    terms = _coerce_terms(raw)
    by_term = {t.term: t for t in terms}
    # Slash-collation split.
    assert "form 10-k" in by_term
    assert "annual report" in by_term
    # Hyphen normalization: U+2011 → ASCII -.
    assert "‑" not in by_term["form 10-k"].term
    # Lowercase + duplicate suppression.
    assert "bbb+" in by_term
    assert sum(1 for t in terms if t.term == "bbb+") == 1
    # Whitespace-only entries dropped.
    assert all(t.term.strip() for t in terms)


def test_diff_renders_markdown_with_only_in_markers() -> None:
    a = extract_fixture_manifest(tenant_id="pyinsights")
    b = extract_fixture_manifest(tenant_id="dispatch")
    diff = diff_manifests(a, b)
    rendered = render_diff_markdown(diff)
    assert rendered.startswith("# Manifest diff:")
    # Domain heads are tenant-specific, so they should appear as only-in markers.
    assert "[only in A]" in rendered
    assert "[only in B]" in rendered
