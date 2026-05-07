# Predictive Operational Analytics — Concept to Prototype

This document describes a plan to migrate the existing whatif stack from a concept-stage system running on fixtures to an end-to-end prototype running on a real customer's event stream and answering a flotilla of operational questions: bus factor, biggest curveball, what-if-we-lose-X, what-if-we-get-a-big-new-client.

The migration replaces the whatif stack's hand-coded taxonomy and keyword-formula labels with three named operational metrics drawn from process-mining vocabulary, swaps the supervised-against-formulae JEPA for a self-supervised next-event predictor, and grounds the system in real ingested data with a real storage substrate. The architecture decisions taken here are deliberately scoped to "minimum viable to run on a real customer's data and produce defensible answers to a handful of useful questions."

## Where we are starting from

The current whatif stack (`vei/whatif/`) is a working concept. It has:

- A canonical event spine (`CanonicalEvent`, surfaces, deltas) and ingestion adapters that turn email, Slack, tickets, and dispatch records into a unified stream.
- A JEPA-shaped model (`JEPAOutcomeModel` in `benchmark_bridge.py`) with a real action-conditioned encoder, a JEPA loss, and an inference pipeline that ranks counterfactual branches against named outcome heads.
- A taxonomy of 22 evidence counters, 5 business heads, 6 future-state heads, and 4 domain-risk heads, plus a hand-weighted `balanced_operator_score`.
- Demonstrations on Enron, Clearwater service-ops, and 1830s public-history fixtures.

It is also stuck at concept stage in three concrete ways. First, every supervised target the JEPA trains against is keyword-formula derived; the model has been taught to mimic the formulae, not to discover its own targets. Second, the named axes carry semantic weight that the mechanics underneath do not earn — `stakeholder_trust` is a clamped sum of keyword counts. Third, nothing in the stack runs on a real customer's bundle today; it runs on fixtures, and the path from "real customer data lands somewhere" to "useful answers come out the other side" is not built.

The prototype migration fixes all three.

## What changes

Three substantive changes plus one deployment change:

1. **The named axes shrink to three operationally legible quantities.** Cycle Time, Rework Rate, and Handoff Count, drawn from process-mining vocabulary. They are pure functions over event metadata, not learned, not weighted, not combined into a single scalar. They are the vocabulary an ops VP or CFO already uses.
2. **The JEPA is retrained as a self-supervised next-event predictor.** Inputs are structural fields only; targets are the next K events given a window of P prior events; calibration is held-out next-event log-likelihood. No keyword features, no business heads, no domain-risk heads.
3. **Counterfactuals are scoped and calibrated.** Drop and inject interventions on the event prefix, JEPA rolled forward, the three metrics recomputed on the rolled stream vs. a no-intervention baseline rollout. Bounded horizon, calibration manifest gates whether counterfactuals ship at all.
4. **The system runs on real-customer event data ingested from real source systems and persisted in real storage**, not on checked-in fixtures. This is the deployment change and is treated as a first-class concern below.

## Objective

Ship a prototype that:

- Ingests a real customer's event stream from a credible set of source systems and lands canonical events in a real storage substrate.
- Computes Cycle Time, Rework Rate, and Handoff Count over that stream with no keyword formulae.
- Trains a self-supervised JEPA over that stream with held-out NLL as the calibration metric.
- Exposes four query primitives — factual, forecast, counterfactual, surprise — and a thin LLM router that turns natural-language questions into typed query payloads.
- Produces defensible answers to at least two of the four sample questions (bus factor, biggest curveball, what-if-we-lose-X, what-if-new-big-client) with a calibration manifest attached.

The objective explicitly excludes: replacing or extending the workflow / RL track, authoring rubrics or contracts, producing a single-scalar operator score, and the full discovery agenda from `DISCOVER_PREDICTIVE_FEATURES.md` (which lands later on this same JEPA encoder).

## Architecture

```
real-customer source systems
  (Slack, Gmail, M365, Jira, Zendesk, ServiceNow, GitHub, calendars, ...)
        │
        ▼
   ingestion workers ── (existing adapters lifted from vei/whatif/, hardened)
        │
        ▼
   object storage ── raw bundles, JEPA checkpoints, analytics artifacts
        │
        ▼
   relational store ── canonical events, manifests, query history
        │
        ├─► metric readouts (pure functions over events)
        │     • Cycle Time, Rework Rate, Handoff Count
        │
        ├─► JEPA training (self-supervised next-event prediction)
        │     • held-out NLL as calibration
        │
        └─► query surface (factual, forecast, counterfactual, surprise)
              • LLM router on top, typed query payloads underneath
```

## The three named metrics

**Cycle Time.** Per-case timestamp delta between the first event of a case and the case's terminal event. Terminal event is the source system's case-closure event when present; otherwise the last event before a configurable inactivity threshold. Reported as median and p90 over a population, broken down by case type, by actor, and by time window.

**Rework Rate.** Per-case fraction of cases where any step recurs. A "step" is a coarse event-kind family. Recurrence is two events of the same family within the same case separated by at least one other family. Captures first-pass yield, reopen rate, revision rate, defect escape rate.

**Handoff Count.** Per-case count of distinct actors. Captures coordination cost.

These three definitions are stable, tenant-agnostic, and computable from event metadata without any model. They are not learned. They are not weighted. They do not combine into a single scalar. A consumer who disagrees with any one definition can swap it for a tenant-specific variant without affecting the others.

## JEPA design and training

A self-supervised next-event predictor over canonical events. Encoder shape is borrowed from the existing `JEPAOutcomeModel` (transformer encoder over typed events, embedding tables for kind/scope/phase, numeric projection for timestamp deltas) but every supervised head except a single next-event head is stripped. Inputs: structural fields only. Targets: next K events given a window of P prior events.

Loss is the sum of three terms: cross-entropy on event-kind, smooth-L1 regression on log-Δt to next event, contrastive loss on the recipient set. No keyword features. No business-head loss. Training data is the customer's canonical event log split by time window — train on early windows, validate on later windows, never split within a case. Calibration: held-out next-event NLL, per surface and overall.

## Counterfactual rollouts

Two intervention types over the event prefix, both rolled forward by the JEPA:

**Drop intervention.** Remove all events satisfying a predicate (e.g., `actor.role == "supervisor_kim"` over a date range). Roll forward N steps. Recompute the three metrics and compare to a no-intervention rollout from the same prefix.

**Inject intervention.** Insert events matching a template (e.g., a new external customer producing typical onboarding events at expected volume). Roll forward, recompute, compare.

Differences in the three metrics under intervention vs. baseline rollout are the counterfactual signal. Rollout horizon is bounded — days to weeks, not quarters — and reported in the manifest. Free fourth readout: surprise score per window = average JEPA NLL on held-out events. Rank windows by surprise to answer "biggest curveball."

## Data ingestion and storage

The prototype's defining constraint is that it runs on a real customer's data, not on checked-in fixtures. This forces a real ingestion and storage story. The plan: object storage for blobs, managed Postgres for canonical events, Supabase as the integrated substrate for the prototype, with a clean migration path off if scale demands it.

### Sources

A short list of source systems covers the operational event surface for most companies:

- **Communication**: Slack export (admin export or Discovery API for live), Gmail / Microsoft 365 mailbox exports, Teams export. These are the highest-signal sources and the most sensitive.
- **Tickets and cases**: Jira, Zendesk, ServiceNow, Linear, GitHub Issues. Most have export APIs or webhook streams.
- **Calendar**: Google Calendar / Outlook via Microsoft Graph.
- **Code**: GitHub / GitLab webhook stream or REST history.
- **Scheduling / dispatch / billing**: tenant-specific. The existing Clearwater fixture demonstrates the pattern; production tenants supply their own adapter or a SQL dump.

For prototype v1, **bundle-based ingestion** is the right choice: customer produces an export bundle (or grants read-only API tokens), we run a one-shot ingestion job, canonical events land in storage. Live streaming via webhooks is v2.

### Storage substrate: Supabase for v1

Recommendation: **Supabase** as the integrated substrate. It bundles managed Postgres, S3-compatible object storage, auth, and row-level security in a single product. For a prototype this collapses several pieces of infrastructure into one and avoids weeks of provisioning work that adds no insight.

Concretely:

- **Object storage (Supabase Storage, S3 API)**: raw customer bundles (encrypted at rest, optionally with customer-managed keys), JEPA model checkpoints, analytics artifacts (rendered reports, manifests). One bucket per tenant, isolated by Storage RLS policies.
- **Relational store (Supabase Postgres)**: canonical events table, ingestion run table, manifest table, query history table. Tenant isolation via Postgres RLS — every row carries a `tenant_id`, every read enforces a session-scoped tenant claim. JSONB columns for `delta` payloads keep the schema flat.
- **Auth (Supabase Auth)**: customer users authenticate; session JWT carries `tenant_id`; RLS reads it. No bespoke auth server.
- **Realtime / queues**: for v1, Postgres LISTEN/NOTIFY or a single worker polling a `pending_jobs` table is enough. No Kafka.

Tradeoffs accepted for v1: vendor lock to Supabase (mitigated because Postgres is portable and the Storage layer is S3-compatible), modest cold-start latency on functions (irrelevant for offline batch), no GPU compute (training runs offline on a separate GPU host and pushes checkpoints to Storage).

### Canonical events table

Single table, partitioned later if needed:

```sql
CREATE TABLE canonical_event (
  event_id        TEXT PRIMARY KEY,
  tenant_id       UUID NOT NULL,
  bundle_id       UUID NOT NULL,
  ts_ms           BIGINT NOT NULL,
  surface         TEXT NOT NULL,
  kind            TEXT NOT NULL,
  case_id         TEXT,
  actor_id        TEXT,
  participants    JSONB NOT NULL DEFAULT '[]',
  object_refs     JSONB NOT NULL DEFAULT '[]',
  delta           JSONB NOT NULL DEFAULT '{}',
  ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON canonical_event (tenant_id, case_id, ts_ms);
CREATE INDEX ON canonical_event (tenant_id, surface, ts_ms);
CREATE INDEX ON canonical_event (tenant_id, ts_ms);
ALTER TABLE canonical_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON canonical_event
  USING (tenant_id = (current_setting('request.jwt.claims', true)::jsonb ->> 'tenant_id')::uuid);
```

Per-tenant volumes for early customers will sit in the millions of events, comfortably handled by a single Supabase instance. If a customer's volume grows past Postgres comfort (tens of millions of events with frequent full-stream scans for JEPA training), the migration is to point training at a columnar export (Parquet on S3) while keeping the live store in Postgres for queries.

### Ingestion pipeline

Each source has an adapter that emits `CanonicalEvent` records. Adapters are lifted from `vei/whatif/` and hardened: paginated, retryable, deterministic (same input bundle → same canonical events with the same event_ids), and capable of redaction at the boundary.

Pipeline:

1. Customer authorizes ingestion (uploads bundle to Supabase Storage or supplies API tokens stored in a secrets table).
2. A worker picks up a `pending_jobs` row, downloads the bundle from Storage or pulls live from APIs.
3. Per-source adapters convert to `CanonicalEvent`, apply tenant-configured redaction, and bulk-insert into `canonical_event`.
4. Ingestion run row records bundle id, source counts, timing, and any rejected events.
5. JEPA training and analytics queries consume the canonical_event table directly.

### Data sensitivity

Two kinds of sensitivity to plan for from day one:

- **Content**: email and Slack bodies contain the most operationally interesting signal but also the most regulatory exposure. The architecture deliberately uses snippets and structural fields for analytics; full content is stored only insofar as the customer's contract allows and is excluded from JEPA training inputs by default.
- **PII / employee data**: every actor reference is internally identified by a stable opaque id; lookup of name / email is via a separate, access-controlled `actor_directory` table. Reports surface roles ("the dispatch supervisor") rather than names by default; named output is a UI permission gate.

Encryption at rest is on by default (Supabase). Customer-managed keys for bundles in Storage are available via Supabase encryption settings for tenants who require it. Region pinning per tenant is configurable.

### Why not S3 + RDS or self-hosted

S3 + RDS gives the same architecture with two more vendor relationships, separate auth, and a custom RLS layer to write. Faster to scale, slower to ship. Right answer at v2; wrong answer at v1.

Self-hosted Postgres + MinIO gives data-residency wins for tenants who require it and is straightforward to migrate to once tenant demand justifies it. Premature for prototype.

The decision tree for v2: stay on Supabase if scale and tenant requirements allow; migrate canonical_event to a partitioned Postgres on AWS/GCP if scale demands; add a dedicated S3 bucket per tenant with customer-managed KMS keys if compliance demands; switch to self-hosted only for a tenant whose contract requires it.

## Methodology

### Stage 1 — Lift the kernel

Lift `CanonicalEvent`, the surfaces taxonomy, and the source adapters from `vei/whatif/` into the new repo's `event_spine/` module. Drop everything else. Write a small migration test that runs an existing fixture through the lifted code and produces an event stream byte-identical to what the existing repo produces. This is the only "preserve compatibility" check in the plan.

### Stage 2 — Wire up storage

Stand up Supabase project. Create the `canonical_event` schema and adjacent tables (`tenant`, `bundle`, `ingestion_run`, `manifest`, `query_history`). Add RLS policies. Write a small ingestion harness that reads a bundle from Storage, runs adapters, and bulk-inserts canonical events with tenant isolation enforced.

### Stage 3 — First real-customer ingestion

Pick a single real customer (or a personal corpus) with a bounded source set — Slack export + Gmail export is enough. Run end-to-end ingestion. Verify event counts, timestamp coverage, actor cardinality, and case-id coverage. This stage exists explicitly to surface the messy parts of real data before they collide with the model.

### Stage 4 — Metric implementation

Implement Cycle Time, Rework Rate, Handoff Count as pure functions over `Sequence[CanonicalEvent]`. Run them on the ingested customer data. Sanity-check distributions against the customer's intuitions — do their sales-cycle medians match what the CRO would say? If yes, metrics are calibrated. If no, the case-boundary detection or step-family mapping needs adjustment.

### Stage 5 — Self-supervised JEPA

Train the next-event predictor on the customer's canonical event stream. Time-windowed split. Held-out NLL is the only metric. If NLL does not improve clearly over a frequency baseline, investigate before continuing.

### Stage 6 — Rollout and counterfactuals

Implement drop / inject interventions and forward sampling. Run factual-vs-rolled metric agreement on a held-out window. If agreement clears the threshold (predicted within 30% of factual), counterfactuals ship. If not, the system reports factual + forecast metrics only and the counterfactual surface stays gated.

### Stage 7 — Question router and demo

Implement the LLM-shaped natural-language question router. Pick the two demo questions most likely to land cleanly on the customer's data (bus factor and biggest curveball are usually safe; what-ifs depend on JEPA quality). Produce a working demo on real data with calibration manifest attached.

## Calibration

Each subsystem has one job and one metric. None validates another:

- **Ingestion**: deterministic re-run produces identical canonical events; per-source coverage logged.
- **Metrics**: definitional. Tested by unit fixtures; validated by sanity-check against customer intuition.
- **JEPA**: held-out next-event NLL, per surface and overall, vs. frequency baseline.
- **Rollout**: factual-vs-rolled metric agreement on held-out windows.
- **Question router**: held-out NL-question / expected-query-payload agreement.

## Deliverables

- The modules above (location depends on the repository decision in "Repository" below).
- Supabase project with documented schema and RLS policies.
- One real-customer canonical event stream ingested end-to-end.
- A calibration manifest showing held-out NLL, factual-vs-rolled agreement, action sensitivity, rollout horizon.
- A working demo that answers at least two of the four sample questions on real data.
- A migration test certifying that the lifted kernel is byte-compatible with the existing `vei/whatif/` event spine.

## Decision points and stop conditions

- **After Stage 3.** If real-customer ingestion surfaces structural problems the existing adapters do not handle (missing `case_id` everywhere, threaded conversations that adapters mis-segment, surfaces the existing taxonomy does not cover), the plan pauses on adapter hardening before touching the model.
- **After Stage 4.** If metric distributions on real data are obviously implausible, definitions or boundary detection need refinement before continuing.
- **After Stage 5.** If JEPA NLL does not beat a frequency baseline by a clear margin, the architecture or training is wrong; investigate before Stage 6.
- **After Stage 6.** If factual-vs-rolled agreement is below threshold, counterfactual queries do not ship in v1; the prototype goes live with factual + forecast + surprise only.
- **After Stage 7.** If neither demo question produces a defensible answer with calibration backing, the prototype is not ready and the plan returns to Stage 5 or 6 to find why.

## Risks and mitigations

- **Real data is messier than fixtures.** Mitigated by treating Stage 3 (real-customer ingestion) as a first-class deliverable that can absorb adapter rework before the model layer is touched.
- **JEPA underperforms on a single tenant's volume.** Mitigated by reporting NLL prominently and gating counterfactuals on agreement; the system degrades to factual + forecast cleanly.
- **PII or content sensitivity blocks training.** Mitigated by structural-only inputs to the JEPA — content snippets are stored for retrieval but excluded from training inputs by default.
- **Supabase scale ceiling.** Mitigated by the v2 migration path described above; the schema is portable and the prototype's volume sits comfortably below the ceiling.
- **Question router hallucination.** Mitigated by typed query payloads (router emits structured payloads, never executes anything) and by reviewer-visible "what query did this become?" surface for every NL question.
- **Schema drift between this repo and the existing repo.** Mitigated by Stage 1's byte-compatibility test and by explicit ownership: this repo owns canonical events going forward; the existing repo can later depend on this one's schema package if both stay active.

## Repository

Three options for where this prototype lives. They are listed without a preference — the choice depends on team size, expected lifetime of both products, and tolerance for upfront infrastructure work.

**Option A — New standalone repository.** Greenfield. Lift the canonical event schema and ingestion adapters (~3–5 files) from the existing repo as a starting point and write everything else fresh. The two products evolve in two repos with no shared code. Pros: clean substrate, no taxonomy gravity, two products with their own release cadences and doc surfaces, easier onboarding for a contributor who only cares about analytics, smaller working context for an LLM coding assistant which improves per-turn quality. Cons: schema drift if both repos evolve `CanonicalEvent` independently; ingestion-adapter improvements have to be ported by hand; some duplicated CI / tooling.

**Option B — Shared schema package, two product repos.** Extract `CanonicalEvent`, surfaces, deltas, and ingestion adapters into a small library (e.g., `vei-event-spine`), publish it, depend on it from both the existing repo and a new analytics repo. Pros: clean product separation and one source of truth for the schema; this is the right shape if both products are real and will live for years; ingestion adapter improvements benefit both consumers automatically. Cons: setup cost — a third codebase with versioning concerns; coordination overhead on schema changes; investment is wasted if the analytics product does not pan out.

**Option C — Subdirectory in the existing repo.** Add `vei/analytics/` as a new top-level subpackage that imports only from a (extracted) `vei/event_spine/` and explicitly does not import from `vei/whatif/` or `vei/workflow/`. Add a CI rule (lint or import check) enforcing the boundary. Pros: fastest to ship; shared schema for free; one CI / test surface; no infrastructure decisions to make. Cons: gravitational pull is real — reviewers familiar with the existing code will keep suggesting reuse that subtly couples the products; the legacy taxonomy stack stays in everyone's peripheral vision; the boundary rule has to be enforced mechanically and maintained; the existing repo's working context remains larger for an LLM coding assistant.

A few orthogonal considerations worth noting alongside the choice. The analytics product targets human decision-makers; the workflow / RL product targets agent training — these have different vocabularies, release cadences, demos, and success definitions, which favors clearer separation but does not by itself force a separate repo. Schema sharing is the only nontrivial overlap, which makes B compelling if both products are confidently going to ship. Repo-loading cost for an LLM coding assistant is real but secondary; it shifts A vs. C marginally and does not change the B story. Whichever option is chosen for v1 does not foreclose the others — A can become B by extracting the schema later; C can become A by lifting the subpackage out; B is a one-way door that is best taken once both products are confirmed.

## What this is and is not

**This is** a migration of the whatif stack's core idea — JEPA-shaped predictive modeling over a canonical event spine — from concept stage to a real-data prototype, with the keyword-formula labels and the taxonomy of named axes left behind.

**This is not** a replacement for, or extension of, the workflow / RL track. It shares the canonical event schema and nothing else. It does not consume or produce `BusinessTaskSpec`, `ContractSpec`, or RL artifacts.

**This is not** a use of the JEPA as a judge, reward source, rubric author, or contract validator. The JEPA predicts next events.

**This is not** a single-scalar operator score system. The three metrics stand on their own.

**This is not** the discovery agenda from `DISCOVER_PREDICTIVE_FEATURES.md`. That work, if pursued, lands on this prototype's JEPA encoder once v1 is stable.
