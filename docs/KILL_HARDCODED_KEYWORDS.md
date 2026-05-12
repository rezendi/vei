# Kill Hardcoded Measurement Keywords

This document describes a plan to remove the hardcoded keyword bags that feed VEI's measurement and target-layer metrics, and replace them with corpus-derived (and human-verified) lexicons generated at onboarding, reviewed by a human, and stored as a versioned artifact alongside each tenant's canonical history.

This is not a blanket ban on every string-matching helper in the repository. Some keyword lists are demo topic lenses, ingestion classifiers, benchmark fixtures, or deterministic search affordances. Those need their own treatment and are explicitly inventoried below.

## Why this versus the status quo

VEI today computes a number of curated target metrics — `reopen_rework_count`, `blocked_duration_ms`, `owner_ambiguity_count`, `deadline_sla_miss_count` — and a number of semantic head scores — `onboarding_integrity`, `release_readiness`, others — by counting events that contain words drawn from hand-curated keyword bags. The bags live in source. `_REWORK_TERMS` is a tuple in `vei/whatif/target_layer.py`. The per-tenant `_DomainLabelSpec` entries in `target_layer.py` carry their own `positive_terms` and `risk_terms` lists, baked in source per tenant.

Three concrete problems with this:

1. **Implicit tenant tuning.** Every keyword bag in the codebase is implicitly shaped by the tenants we have built against — Enron, Clearwater service-ops, a small set of named domains. Apply those same bags to a law firm, a hedge fund, a healthcare provider, or a medical practice and the metrics still produce numbers, but the numbers are noisier and less meaningful, and nobody is told this has happened. There is no warning surface when a keyword bag is being applied outside the distribution it was tuned for.

2. **Onboarding requires a code patch.** Adding a new tenant whose vocabulary differs meaningfully from the existing bags requires editing `_DomainLabelSpec` in source, shipping the change, and redeploying. This is hostile to running against arbitrary customers and is the largest single reason the system feels concept-stage rather than product-stage.

3. **Audit is opaque.** A consumer reading `reopen_rework_count = 3` for some window has no surface to ask "which three events were counted, and which terms triggered the count." The answer lives in source as a constant tuple; reproducing the count requires reading code. There is no per-tenant, per-snapshot record of what was actually measured.

The replacement story is short. One LLM pass at tenant onboarding (or refresh) reads a sample of that tenant's events plus organizational context and proposes candidate semantic heads mapped to the fixed target registry. A human reviews. A second LLM pass produces a citation-grounded lexicon for each accepted registry head and for each structural-metric vocabulary the target layer currently encodes as a hardcoded bag. The resulting tenant measurement manifest is persisted alongside the snapshot, versioned, and read at runtime by every target-layer metric whose computation involves vocabulary. Metrics whose computation is pure structural math do not change.

This is the smallest intervention that addresses all three problems simultaneously for the target layer. It removes hand-curated measurement keyword bags from source without pretending that unrelated deterministic string matching has the same product contract.

## Objective

Replace every hardcoded keyword bag used by the curated target layer with tenant-derived lexicons drawn from a per-tenant measurement manifest. The manifest is produced by a two-pass LLM pipeline with a human review between passes, and persisted as a versioned artifact next to the tenant's `context_snapshot.json`. Onboarding a new tenant produces a new manifest; refreshing a tenant regenerates one with version-pinned provenance. Every curated target metric whose definition currently depends on a keyword bag reads its vocabulary from the manifest at runtime. Structural-math metrics with no vocabulary are not changed and not touched.

The objective explicitly excludes:

- Changing the runtime target vector: head names, ordering, masks, and structural-metric names stay stable across this change. Only the vocabulary they consume becomes tenant-derived. Adding, dropping, or renaming runtime heads is a separate target-schema migration.
- Adjudicating whether semantic heads (`stakeholder_trust`, `regulatory_exposure`, and the like) should exist at all as measurements. This plan makes the keyword bags honest; it does not address the deeper question of whether counting concept-vocabulary in a window constitutes a *measurement* of the underlying concept. See **Out of scope** below for what that means.
- Changing JEPA training. The JEPA still trains against whatever target values the metrics emit; we are not touching the model loop.
- A new tenant onboarding UI. The pipeline produces the manifest; how a human reviews it is left for a later UX pass and is not blocking for this work.

## What stays as code, unchanged

A subset of `STRUCTURAL_HEAD_NAMES` is genuinely structural: pure math over event metadata with no vocabulary dependency. These are not touched by this plan.

**Bedrock universal** — the math means the same thing in every tenant:

- `time_to_first_response_ms` — first_ts after the branch minus the branch event's ts.
- `cross_system_spread` — count of distinct surfaces in the window.
- `evidence_completeness` — coverage check over provenance fields.

**Math-universal, interpretively loaded** — the computation is the same everywhere, but the *meaning* the name suggests is company-shape-dependent. These stay as code because there is nothing tenant-specific to derive; readers should understand the metric as the math, not as the implied operational concept:

- `handoff_count` — count of consecutive events whose actor_id differs. In a ticket-driven org an actor change usually *is* a handoff; in a chat-heavy org every reply on a thread looks like one. The math is honest; the interpretation is a choice the metric does not make for the reader.
- `participant_fanout` — count of distinct actors and target_ids in the window. Same shape of caveat.

These five stay as Python functions.

## `cycle_time_ms` earns its name

`cycle_time_ms` is currently `last_ts - first_ts` of the window. That is window duration, not cycle time. Cycle time in the operational sense — the metric ops VPs and CFOs actually use, the one every process-mining tool ships — requires identifying case **closure**: "resolved", "shipped", "won", "closed", "rejected", "delivered". Closure vocabulary is tenant-specific. So `cycle_time_ms` joins this plan's inventory.

Pass 2 produces a `closure` term pack per tenant alongside the other lexicons. The runtime definition of `cycle_time_ms` becomes:

```
closure_event = first event after branch whose text contains any term in manifest.lexicons["closure"]
cycle_time_ms = closure_event.ts - branch_event.ts                if closure_event found
              = last_event_in_window.ts - branch_event.ts         otherwise (fallback to window duration)
```

The fallback exists so the metric is defined on every window, not just windows that happen to contain a closure event. The fallback case is logged so downstream readers know which value they got.

Renaming the metric to `window_duration_ms` was the other option considered and rejected. The whole point of this plan is to make metrics earn their names; renaming away from the load-bearing semantic is a retreat where adding the closure lexicon is the offensive move the plan exists to make.

## What Gets Replaced

Every hardcoded term tuple in the curated target layer, and every curated target metric whose definition depends on one. Initial inventory.

**Term tuples that go away:**

- `vei/whatif/target_layer.py`: `_REWORK_TERMS`, `_BLOCKED_TERMS`, `_UNBLOCKED_TERMS`, `_OWNER_AMBIGUITY_TERMS`, `_DEADLINE_TERMS`.
- `vei/whatif/target_layer.py`: every `_DomainLabelSpec.positive_terms` and `risk_terms` list under `_DOMAIN_LABEL_SPECS` (currently `pyinsights`, `powrofyou`, `dispatch` blocks).

**Metrics currently mis-grouped under `STRUCTURAL_HEAD_NAMES` that are in fact vocabulary-dependent.** They are computed inside the same `_compute_structural_targets` function as the genuinely structural metrics, which is why they got the label, but their definitions reduce to "count events whose text contains terms from a keyword bag." They get reclassified and read their vocabulary from the manifest:

- `cycle_time_ms` — gains a `closure` lexicon (see the section above), with window duration as a fallback when no closure event is present.
- `reopen_rework_count` — counts events containing terms from the rework lexicon.
- `blocked_duration_ms` — measures duration between blocked-state and unblocked-state events, identified by lexicon membership.
- `owner_ambiguity_count` — counts events containing terms from the owner-ambiguity lexicon.
- `deadline_sla_miss_count` — counts events containing terms from the deadline-missed lexicon.

**Per-tenant semantic heads** under `DOMAIN_HEADS_BY_TENANT` — `onboarding_integrity`, `user_trust_confusion`, `release_readiness`, `data_coverage_gap`, `compliance_privacy_sensitivity`, `repeated_clarification_loop`, `product_readiness_proof`, `gtm_narrative_consistency`, `partner_customer_traction` — currently driven by `_DomainLabelSpec` term lists. Each gets a manifest lexicon.

For every entry above, after the plan lands the metric's computation (count, duration, score) stays in code; the vocabulary feeding into the computation comes from the manifest.

## What Does Not Move Into This Manifest

Keyword-like surfaces outside the curated target layer should not be silently folded into this plan.

| Surface | Current role | Disposition |
| --- | --- | --- |
| `vei/workflow/api.py` `_BUSINESS_TERMS`, `_ESCALATION_TERMS`, `_PATTERN_STOPWORDS` | Workflow candidate ranking and pattern keys | Follow `docs/LLM_WORKFLOW_CANDIDATE_MINING.md`: retire keyword scoring and move mining semantics to the LLM mining track. Do not move these terms into `tenant_measurement_manifest.json`. |
| `vei/whatif/ranking.py` outcome-signal terms | Scoring LLM replay branches for objective-pack demos | Separate objective-pack calibration problem. Do not treat as curated target-layer vocabulary in this pass. |
| `vei/whatif/macro_outcomes.py` prompt terms | Heuristic macro demo response to intervention text | Demo baseline calibration. Keep separate from tenant measurement manifests. |
| `vei/whatif/benchmark_business.py` business-head terms | Benchmark/business outcome labels and fixtures | Separate benchmark-labeling calibration problem. Do not migrate in this pass unless the benchmark contract is explicitly changed. |
| Public-history UI/topic keywords | Public demo topic lenses and candidate phrasing | Demo retrieval and presentation logic, not tenant measurement. |
| Corpus/adaptor keyword helpers | Canonicalization flags such as escalation or attachment references | Ingestion normalization. Needs a separate canonical-event extraction review, not this target-layer migration. |

## Architecture

```
canonical events (sample) + org context
       │
       ▼
Pass 1 — head discovery (LLM, expensive)
   reads sample, proposes candidate semantic heads
   one-line rationale per head, cited example events
       │
       ▼
Human review
   accept / reject / rename concepts within the fixed target registry
   small focused review, not per-event annotation
       │
       ▼
Pass 2 — lexicon population (LLM, cheaper, per-concept)
   for each accepted semantic head
     and each structural-metric vocabulary (rework, blocked, etc.)
     propose grounded terms with citation to real event text
   validator drops any uncited term
       │
       ▼
tenant_measurement_manifest.json
   versioned, persisted alongside context_snapshot.json
   schema: { tenant_id, snapshot_version, manifest_version,
             semantic_heads: [{name, rationale, lexicon: [...]}],
             structural_lexicons: {rework: [...], blocked: [...], ...} }
       │
       ▼
runtime metrics
   structural-math metrics: unchanged Python functions
   keyword-bag metrics: deterministic functions of (events + manifest)
```

The pattern reuses the citation-required schema-constrained LLM output infrastructure already in `target_layer.py` (`SEMANTIC_LABEL_RESPONSE_SCHEMA`, `validate_semantic_target_label`). Pass 2's validator is the same shape: for every proposed term, find it in at least N cited events' text, otherwise drop.

The target-vector contract remains fixed for this migration. Pass 1 may propose new candidate concepts for the reviewer, but a concept only becomes a runtime head if it maps to an existing registry entry. New runtime heads require a separate schema-versioned target-layer migration, because `CURATED_TARGET_HEAD_NAMES`, `curated_target_values_and_mask`, saved manifests, training rows, and JEPA target dimensions all depend on stable names and ordering.

## Methodology

### Stage 1 — Inventory

Walk the repository and enumerate hardcoded keyword tuples. Each entry gets: file:line, current name, current consumer, whether the consumer is a curated target-layer metric, workflow-mining helper, benchmark labeler, demo lens, or ingestion helper. Produces two checklists: what Pass 2 has to cover, and what is explicitly out of scope for this migration.

### Stage 2 — Manifest schema and storage path

Define `tenant_measurement_manifest.json` schema and where it lives relative to `context_snapshot.json`. Sidecar pattern, similar to canonical history sidecars. Pin a `manifest_version` field so runtime consumers can refuse to load incompatible versions. Pin a `snapshot_version` field so the manifest is tied to the events it was derived from.

### Stage 3 — Head discovery pass

Build the Pass 1 prompt. Input: a sample of canonical events covering breadth (multiple surfaces, multiple actors, multiple time windows), plus organizational context (industry, size, regulatory regime, stated goals — drawn from `ContextSnapshot.metadata` or supplied at onboarding). Output: list of `{name, registry_target_id, rationale, example_event_ids}` candidates, where `registry_target_id` is either an existing target-layer head or empty when the concept needs a future schema migration. The LLM call is schema-constrained. The output is a draft, not a finalized manifest.

### Stage 4 — Human review surface

A small CLI or web surface (out of scope for this plan to specify in full) where a reviewer sees the draft head list and accepts, rejects, or maps heads to the fixed runtime registry. Output is a finalized registry-aligned head list that goes into Pass 2. Reviewer-added concepts are allowed as notes or future-migration candidates, but not as new runtime dimensions in this pass.

### Stage 5 — Lexicon population pass

Build the Pass 2 prompt. For each accepted semantic head and for each structural-metric vocabulary identified in Stage 1, the LLM proposes a term pack. Each term must be cited to one or more real events from the corpus. The validator confirms presence verbatim. Terms that fail validation are dropped and logged. Output is the finalized manifest.

### Stage 6 — Wire runtime consumers

For each target-layer entry in the Stage 1 inventory, replace the hardcoded tuple read with a manifest read. Manifest absence must not look like a real zero. Vocabulary-dependent heads are masked unsupported or untrusted when the manifest is missing, incompatible, or lacks the required lexicon. Pure structural-math heads continue to compute normally.

### Stage 7 — Refresh story

Refreshing a tenant snapshot regenerates the manifest. Re-running Pass 1 may propose concept drift for human review, but runtime head additions or removals wait for a separate target-schema migration. Re-running Pass 2 may add or remove terms for existing heads (vocabulary drift). The reviewer sees a diff against the previous manifest. The previous manifest is retained for audit; metric outputs cite which manifest version was used.

## Stop conditions

The work is done when:

- Every curated target-layer keyword tuple identified in Stage 1 has been removed from source.
- Every non-target-layer keyword tuple identified in Stage 1 has an explicit disposition: covered by another plan, deliberately retained as demo or ingestion logic, or filed as follow-up work.
- Every tenant in the active set has a `tenant_measurement_manifest.json` next to its snapshot.
- Every target-layer keyword-bag-using metric reads its vocabulary from the manifest, with missing or incompatible vocabulary surfaced through masks/status rather than a trusted zero.
- A new tenant can be onboarded end-to-end without a code patch — Pass 1, review, Pass 2, runtime, all driven by the manifest.
- A regression test confirms that running an existing tenant's pipeline against its persisted manifest reproduces the previous metric values within tolerance.

The plan is not done if any of the above is missing.

## Out of scope

- **The deeper question of whether semantic heads constitute measurements.** A semantic head named `stakeholder_trust` with a citation-grounded lexicon of `{please, thanks, appreciate, confirm, ...}` still counts politeness markers. Every term is real, every citation checks out, every word in the lexicon appears verbatim in the cited events — and what the head produces is a vocabulary density score, not a trust measurement. The same shape of critique applies to `liquidity_stress` (vocabulary about money troubles), `regulatory_exposure` (vocabulary about audits and regulators), `governance_response` (vocabulary about oversight), and similar named axes. Trust, stress, and exposure are the kinds of constructs whose definition is the load-bearing claim, and a count of concept-adjacent vocabulary does not earn the label. This plan fixes one layer of that problem: the keywords stop being a developer's guess and start being a citation-grounded artifact of the tenant's own corpus. It does not fix the deeper layer: whether counting concept-adjacent vocabulary constitutes *measurement* of the concept. Fixing the deeper layer requires either validating each head against external truth (do high-`regulatory_exposure` windows in fact precede real regulatory events?), removing semantic heads entirely (replace them with structural metrics only), or reframing them honestly as vocabulary-density scores rather than measurements. All three of those are out of scope here and are separate decisions this plan should not pre-empt.
- JEPA training changes. Heads still train against whatever target values the metrics emit.
- Cross-tenant lexicon sharing. Each tenant gets its own manifest. Whether to seed a new tenant's manifest from a similar tenant's manifest is a separate question.
- A formal review UX. Stage 4 is described as a surface; its design is left to a follow-up.
- Retiring workflow-mining keywords. That path is owned by `docs/LLM_WORKFLOW_CANDIDATE_MINING.md`, which removes keyword scoring rather than turning those terms into measurement-manifest vocabulary.

## What this lets you say afterwards

After this lands, "VEI computed `release_readiness = 0.43` for this window" is backed by a specific, auditable record: which manifest version was loaded, which terms it contained for that head, which events those terms appeared in, which one-line rationale mapped the concept to the fixed target registry, and which human approved its inclusion. That is a substantively different claim than the current one, which reduces to "we counted occurrences of some words a developer once typed into a source file."
