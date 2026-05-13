"""Compare two manifests at head- and term-level.

Output is descriptive: Jaccard, set deltas, and a small per-head summary. We
deliberately do not emit a pass/fail or an overall similarity number, to avoid
tuning prompts toward reproducing a fixture.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .measurement_manifest import (
    ManifestHead,
    ManifestTerm,
    TenantMeasurementManifest,
)


@dataclass(frozen=True)
class TermSetDiff:
    polarity: str
    only_in_a: tuple[str, ...]
    only_in_b: tuple[str, ...]
    shared: tuple[str, ...]
    jaccard: float


@dataclass(frozen=True)
class HeadDiff:
    name: str
    role_a: str
    role_b: str
    in_a: bool
    in_b: bool
    term_diffs: tuple[TermSetDiff, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ManifestDiff:
    tenant_a: str
    tenant_b: str
    head_diffs: tuple[HeadDiff, ...]


def _term_strings(terms: Iterable[ManifestTerm]) -> set[str]:
    return {t.term.lower().strip() for t in terms if t.term.strip()}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return round(len(a & b) / len(union), 4)


def _diff_term_set(
    polarity: str, a_terms: Iterable[ManifestTerm], b_terms: Iterable[ManifestTerm]
) -> TermSetDiff:
    a_set = _term_strings(a_terms)
    b_set = _term_strings(b_terms)
    return TermSetDiff(
        polarity=polarity,
        only_in_a=tuple(sorted(a_set - b_set)),
        only_in_b=tuple(sorted(b_set - a_set)),
        shared=tuple(sorted(a_set & b_set)),
        jaccard=_jaccard(a_set, b_set),
    )


def _diff_head(name: str, a: ManifestHead | None, b: ManifestHead | None) -> HeadDiff:
    return HeadDiff(
        name=name,
        role_a=a.role if a else "",
        role_b=b.role if b else "",
        in_a=a is not None,
        in_b=b is not None,
        term_diffs=(
            _diff_term_set(
                "positive",
                a.positive_terms if a else [],
                b.positive_terms if b else [],
            ),
            _diff_term_set(
                "risk",
                a.risk_terms if a else [],
                b.risk_terms if b else [],
            ),
        ),
    )


def diff_manifests(
    a: TenantMeasurementManifest, b: TenantMeasurementManifest
) -> ManifestDiff:
    a_heads = {h.name: h for h in a.heads}
    b_heads = {h.name: h for h in b.heads}
    all_names = sorted(set(a_heads) | set(b_heads))
    head_diffs = tuple(
        _diff_head(name, a_heads.get(name), b_heads.get(name)) for name in all_names
    )
    return ManifestDiff(
        tenant_a=a.tenant_id, tenant_b=b.tenant_id, head_diffs=head_diffs
    )


def _render_term_diff(td: TermSetDiff) -> list[str]:
    lines: list[str] = [
        f"    {td.polarity}: jaccard={td.jaccard:.3f} "
        f"(shared={len(td.shared)}, only_A={len(td.only_in_a)}, only_B={len(td.only_in_b)})"
    ]
    if td.only_in_a:
        lines.append(f"      only in A: {', '.join(td.only_in_a)}")
    if td.only_in_b:
        lines.append(f"      only in B: {', '.join(td.only_in_b)}")
    return lines


def render_diff_markdown(diff: ManifestDiff) -> str:
    out: list[str] = []
    out.append(f"# Manifest diff: {diff.tenant_a} (A) vs {diff.tenant_b} (B)")
    out.append("")
    out.append(f"Heads compared: {len(diff.head_diffs)}")
    out.append("")
    for hd in diff.head_diffs:
        if hd.in_a and not hd.in_b:
            marker = " [only in A]"
        elif hd.in_b and not hd.in_a:
            marker = " [only in B]"
        else:
            marker = ""
        role = hd.role_a or hd.role_b
        out.append(f"## `{hd.name}` ({role}){marker}")
        for td in hd.term_diffs:
            out.extend(_render_term_diff(td))
        out.append("")
    return "\n".join(out).rstrip() + "\n"


__all__ = [
    "ManifestDiff",
    "HeadDiff",
    "TermSetDiff",
    "diff_manifests",
    "render_diff_markdown",
]
