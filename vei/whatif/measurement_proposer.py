"""Black-box LLM-driven manifest proposer.

Single-pass on purpose: the human-in-the-loop two-pass split lives downstream
of this primitive. The goal of this module is the smallest viable LLM call
that produces a TenantMeasurementManifest in schema, so the comparison
harness can run end-to-end.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Sequence

from .measurement_manifest import (
    ManifestHead,
    ManifestTerm,
    TenantMeasurementManifest,
    now_iso,
)
from .models import WhatIfEvent
from .target_layer import (
    CURATED_TARGET_HEAD_NAMES,
    PROXY_DEBUG_HEAD_NAMES,
)

# Heads that are pure structural math and should not appear in the proposed
# vocabulary at all — they are out of scope for the manifest.
_STRUCTURAL_MATH_ONLY: frozenset[str] = frozenset(
    {
        "cycle_time_ms",
        "handoff_count",
        "time_to_first_response_ms",
        "participant_fanout",
        "cross_system_spread",
        "evidence_completeness",
    }
)


def _vocab_dependent_registry_heads() -> tuple[str, ...]:
    out: list[str] = []
    for name in CURATED_TARGET_HEAD_NAMES:
        if name in _STRUCTURAL_MATH_ONLY:
            continue
        out.append(name)
    for name in PROXY_DEBUG_HEAD_NAMES:
        if name not in out and name not in _STRUCTURAL_MATH_ONLY:
            out.append(name)
    return tuple(out)


PROPOSER_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["heads"],
    "properties": {
        "heads": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["name", "role", "positive_terms", "risk_terms"],
                "properties": {
                    "name": {"type": "string"},
                    "registry_target_id": {"type": "string"},
                    "role": {
                        "type": "string",
                        "enum": ["structural_vocab", "domain", "proxy_debug"],
                    },
                    "rationale": {"type": "string"},
                    "positive_terms": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["term"],
                            "properties": {
                                "term": {"type": "string"},
                                "cited_event_ids": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "supporting_spans": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                    },
                    "risk_terms": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["term"],
                            "properties": {
                                "term": {"type": "string"},
                                "cited_event_ids": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "supporting_spans": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}


SYSTEM_PROMPT = """You are a measurement designer for an analytics platform.

You will be given a sample of canonical business events from one tenant and
some organizational context. Your job is to propose the vocabulary that
measurement metrics should use for *this* tenant.

You must return a JSON object with a `heads` array. Each head is a
measurement concept with:

- `name`: short snake_case identifier
- `registry_target_id`: if the head maps to one of the fixed runtime target
  heads listed below, use that exact name. Otherwise leave empty.
- `role`: one of `structural_vocab` (counts/state transitions over text
  triggers, e.g. rework, blocked, deadline), `domain` (tenant-specific
  business concept that swings on positive/risk language, e.g. release
  readiness), or `proxy_debug` (broad diagnostic head such as regulatory
  exposure).
- `rationale`: one sentence explaining why this head matters for this tenant.
- `positive_terms`: terms whose presence indicates the head's "good" or
  "trigger-end" state. Each term has a `term` string and SHOULD include at
  least one `cited_event_ids` value drawn from the sample below.
- `risk_terms`: terms whose presence indicates the head's "bad" or
  "trigger-start" state. Same shape as positive_terms.

For structural-vocab heads that are just one bag (e.g. counting events that
mention deadlines), populate only `risk_terms`. For balanced heads (e.g.
blocked/unblocked or release readiness) populate both.

Do NOT invent heads that map to structural-math-only registry entries
(cycle_time_ms, handoff_count, time_to_first_response_ms, participant_fanout,
cross_system_spread, evidence_completeness) — those metrics use no vocabulary.

Prefer terms that actually appear in the sample. Cite at least one event id
per term where possible.

Term-emission rules (strict):

- One term per entry. Do NOT pack alternatives into a single string with
  separators such as "/", "|", or "or". Emit each surface form as its own
  entry with its own citation.
- Use ASCII characters only for terms. Replace non-breaking hyphens, en-
  dashes, em-dashes, and similar Unicode punctuation with ASCII "-".
- Lowercase the term unless the term is meaningless without its casing
  (e.g. "SEC", "FERC"). Match-time normalization will lowercase, so prefer
  the simplest casing.
- Each entry must be a short string that could plausibly be matched against
  the raw text of an event with a substring check. Avoid descriptive
  phrasing like "files form 10-K" — emit "form 10-k" instead.
"""


@dataclass
class ProposerResult:
    manifest: TenantMeasurementManifest
    raw_response: dict[str, Any]
    provider: str
    model: str


def _format_event_for_prompt(event: WhatIfEvent) -> str:
    subject = (event.subject or "").strip()
    snippet = (event.snippet or "").strip()
    bits: list[str] = []
    if subject:
        bits.append(f"subject: {subject}")
    if snippet:
        bits.append(f"snippet: {snippet[:400]}")
    body = " | ".join(bits) if bits else "(no text)"
    return f"- event_id={event.event_id} surface={event.surface or '-'} {body}"


def build_user_prompt(
    *,
    tenant_id: str,
    org_context: dict[str, Any],
    events: Sequence[WhatIfEvent],
    max_events: int = 80,
) -> str:
    registry = ", ".join(_vocab_dependent_registry_heads())
    sample = events[:max_events]
    event_block = "\n".join(_format_event_for_prompt(e) for e in sample)
    context_json = (
        json.dumps(org_context, indent=2, sort_keys=True) if org_context else "{}"
    )
    return (
        f"Tenant id: {tenant_id}\n\n"
        f"Organizational context:\n{context_json}\n\n"
        f"Fixed runtime registry heads (use these names for registry_target_id "
        f"when applicable, leave empty otherwise):\n{registry}\n\n"
        f"Canonical event sample ({len(sample)} events):\n{event_block}\n"
    )


# Unicode hyphen-like characters that should normalize to ASCII "-".
_HYPHEN_TRANSLATION = str.maketrans(
    {
        "‐": "-",  # hyphen
        "‑": "-",  # non-breaking hyphen
        "‒": "-",  # figure dash
        "–": "-",  # en dash
        "—": "-",  # em dash
        "−": "-",  # minus sign
    }
)

_SPLIT_DELIMITERS = ("/", "|")


def _normalize_term(term: str) -> str:
    cleaned = term.translate(_HYPHEN_TRANSLATION)
    cleaned = " ".join(cleaned.split())  # collapse internal whitespace
    return cleaned.strip().lower()


def _split_collated(term: str) -> list[str]:
    """Split a term packed with `/` or `|` separators into atomic terms.

    The prompt forbids this, but belt-and-suspenders: a single LLM slip would
    otherwise produce a multi-phrase string that no substring matcher will hit.
    """
    parts = [term]
    for delim in _SPLIT_DELIMITERS:
        nxt: list[str] = []
        for part in parts:
            nxt.extend(piece.strip() for piece in part.split(delim))
        parts = nxt
    return [p for p in parts if p]


def _coerce_terms(raw: Any) -> list[ManifestTerm]:
    if not isinstance(raw, list):
        return []
    out: list[ManifestTerm] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        raw_term = str(item.get("term", "")).strip()
        if not raw_term:
            continue
        cited = [str(x) for x in (item.get("cited_event_ids") or []) if str(x).strip()]
        spans = [str(x) for x in (item.get("supporting_spans") or []) if str(x).strip()]
        for piece in _split_collated(raw_term):
            normalized = _normalize_term(piece)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(
                ManifestTerm(
                    term=normalized,
                    cited_event_ids=cited,
                    supporting_spans=spans,
                    source="llm",
                    confidence=1.0,
                )
            )
    return out


def _coerce_manifest(
    *,
    tenant_id: str,
    org_context: dict[str, Any],
    payload: dict[str, Any],
    provider: str,
    model: str,
) -> TenantMeasurementManifest:
    heads_raw = payload.get("heads") or []
    heads: list[ManifestHead] = []
    for entry in heads_raw:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "")).strip()
        if not name:
            continue
        role = str(entry.get("role", "")).strip() or "domain"
        if role not in {"structural_vocab", "domain", "proxy_debug"}:
            role = "domain"
        heads.append(
            ManifestHead(
                name=name,
                registry_target_id=str(entry.get("registry_target_id", "")).strip(),
                role=role,  # type: ignore[arg-type]
                rationale=str(entry.get("rationale", "")).strip(),
                positive_terms=_coerce_terms(entry.get("positive_terms")),
                risk_terms=_coerce_terms(entry.get("risk_terms")),
            )
        )
    return TenantMeasurementManifest(
        tenant_id=tenant_id,
        generated_at=now_iso(),
        org_context=org_context,
        provenance={
            "source": "llm",
            "provider": provider,
            "model": model,
            "note": "Single-pass black-box proposer. No human review.",
        },
        heads=heads,
    )


def _is_responses_api_model(model: str) -> bool:
    m = (model or "").strip().lower()
    return (
        m.startswith("gpt-5")
        or m.startswith("o1")
        or m.startswith("o3")
        or m.startswith("o4")
    )


async def _call_openai(
    *,
    model: str,
    system: str,
    user: str,
    timeout_s: int,
) -> dict[str, Any]:
    import os

    try:
        from openai import AsyncOpenAI
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "openai SDK not installed; install with extras [llm]"
        ) from exc

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Add it to .env or export it before running."
        )
    base_url = os.environ.get("OPENAI_BASE_URL")
    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    try:
        if _is_responses_api_model(model):
            resp = await asyncio.wait_for(
                client.responses.create(
                    model=model,
                    input=f"[system]\n{system}\n\n[user]\n{user}",
                    text={"format": {"type": "json_object"}},
                    reasoning={"effort": "medium"},
                ),
                timeout=timeout_s,
            )
            raw = getattr(resp, "output_text", None) or ""
        else:
            resp = await asyncio.wait_for(
                client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                ),
                timeout=timeout_s,
            )
            raw = resp.choices[0].message.content or ""
        if not raw.strip():
            raise RuntimeError("LLM returned empty response.")
        return json.loads(raw)
    finally:
        await client.close()


async def propose_manifest_async(
    *,
    tenant_id: str,
    events: Sequence[WhatIfEvent],
    org_context: dict[str, Any] | None = None,
    provider: str = "openai",
    model: str = "gpt-5-mini",
    max_events: int = 80,
    timeout_s: int = 240,
) -> ProposerResult:
    if provider not in {"openai", "auto"}:
        raise RuntimeError(
            f"propose_manifest currently only supports the OpenAI provider; got {provider!r}."
        )
    user_prompt = build_user_prompt(
        tenant_id=tenant_id,
        org_context=org_context or {},
        events=events,
        max_events=max_events,
    )
    payload = await _call_openai(
        model=model,
        system=SYSTEM_PROMPT,
        user=user_prompt,
        timeout_s=timeout_s,
    )
    manifest = _coerce_manifest(
        tenant_id=tenant_id,
        org_context=org_context or {},
        payload=payload,
        provider="openai",
        model=model,
    )
    return ProposerResult(
        manifest=manifest,
        raw_response=payload,
        provider="openai",
        model=model,
    )


def propose_manifest(
    *,
    tenant_id: str,
    events: Sequence[WhatIfEvent],
    org_context: dict[str, Any] | None = None,
    provider: str = "openai",
    model: str = "gpt-5-mini",
    max_events: int = 80,
    timeout_s: int = 240,
) -> ProposerResult:
    return asyncio.run(
        propose_manifest_async(
            tenant_id=tenant_id,
            events=events,
            org_context=org_context,
            provider=provider,
            model=model,
            max_events=max_events,
            timeout_s=timeout_s,
        )
    )


__all__ = [
    "PROPOSER_RESPONSE_SCHEMA",
    "SYSTEM_PROMPT",
    "ProposerResult",
    "build_user_prompt",
    "propose_manifest",
    "propose_manifest_async",
]
