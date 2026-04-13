# Opus Review Brief

## What VEI is trying to become

VEI is meant to model how work inside a real company unfolds across many systems, then let a user branch from a real decision point and ask what might have happened under a different action.

The goal is larger than email replay. A company may have history in email, Slack, Teams, Jira, ClickUp, docs, CRM, calendars, public filings, financial checkpoints, news, and other exports. VEI should accept those raw exports, normalize them into a shared company history bundle, build a coherent case timeline, and then use that timeline for search, branch selection, replay, ranking, benchmarking, and learned forecasting.

Enron is the working example because it has a rich historical record. Enron itself is still email-heavy as a primary internal record, but VEI is no longer meant to be mail-first as a product architecture.

## What changed in this round

This round pushed VEI away from a mail-shaped historical path and toward a broader company-history path.

The historical what-if flow now reads a normalized company history bundle instead of assuming an email archive. Supported inputs in the new path include:

- mail archive and Gmail threads
- Slack and Teams-style chat history
- Jira-style ticket history
- CRM and Salesforce revenue context
- public company context such as dated financial checkpoints and public news

The shared case model now links related activity across surfaces. A branch point can open on one surface, while the saved workspace keeps the earlier linked history from other relevant surfaces plus linked record-style context such as docs or CRM state.

The Enron example was also upgraded. It now includes public company context, a clearer business-state readout, refreshed screenshots, a ranked comparison of candidate actions, and repo-owned saved example artifacts so a fresh clone can inspect the example directly.

## The core product direction

The important architectural direction is this:

- raw exports come in from arbitrary systems
- adapters normalize them into one company history bundle
- VEI builds a shared event stream and case structure from that bundle
- a branch point opens from that shared history
- VEI rebuilds the relevant surfaces for that case
- VEI runs counterfactuals and evaluates them in business terms rather than only in message mechanics

The action layer should be general. It should not be permanently shaped around email. Enron being email-heavy is a property of that dataset, not a reason to make the world model email-first.

## Design principles we care about

Please review the repo with these principles in mind.

### Modularity inside one monolith

The codebase is one monolith, but it is meant to be divided into modules with explicit typed interfaces. Modules should talk to each other through their `api.py` interfaces or other clearly defined module interfaces, not by reaching through internal files casually.

### Reusable normalization

The outer edge of the system should absorb source-specific mess. The inner historical engine should work on a stable normalized shape. The more source-specific branching logic that leaks into the core what-if flow, the worse the architecture is.

### Reusable business reasoning

Business-state reasoning should sit above raw channel mechanics. A useful review will check whether the code is truly moving toward shared business interpretation across surfaces, or whether it is still mostly a mail-specific system wearing a thin generic wrapper.

### Honest saved examples

A fresh clone should be able to inspect the Enron example directly from repo-owned saved artifacts. When a real local archive is available, the same example should be able to rerun live against that archive.

### Strong handling of incomplete or messy inputs

Real exports are messy. Failed captures, partial data, missing surfaces, mixed providers, and formatted values should be handled clearly and safely. The system should reject unusable bundles early and accept common real-world input formats where it can.

### No one-off fixes

The main concern in this area is accidental accumulation of Enron-specific or surface-specific special cases. Please look for places where the code solves the immediate problem in a narrow way instead of moving behavior into a reusable adapter, typed model, or shared helper.

## Specific things that were fixed recently

These were real problems and are now expected to be fixed on `main`.

- Saved workspaces now materialize linked cross-surface case history instead of only keeping the main branch surface.
- Company-history detection now ignores sources whose capture status is `error`.
- Revenue hydration now merges CRM and Salesforce inputs instead of choosing one and dropping the other.
- Revenue amounts now accept common formatted export values such as `$10,000` instead of crashing on direct numeric parsing.

If you still see these issues, call them out clearly.

## Files that matter most for this review

These files are the most important places to inspect for the new direction:

- `vei/whatif/corpus.py`
- `vei/whatif/api.py`
- `vei/whatif/cases.py`
- `vei/whatif/business_state.py`
- `vei/whatif/models.py`
- `vei/whatif/public_context.py`
- `vei/whatif/research.py`
- `vei/whatif/benchmark.py`
- `vei/context/hydrate.py`
- `vei/ui/_api_models.py`
- `vei/ui/static/studio-whatif.js`

These docs explain the intended behavior and example framing:

- `README.md`
- `docs/WHATIF.md`
- `docs/ARCHITECTURE.md`
- `docs/examples/enron-master-agreement-public-context/README.md`

## What a useful Opus review should focus on

Please review the current `main` branch for:

- correctness of the new company-history path
- whether the architecture is actually becoming source-agnostic
- whether case linking across surfaces is mechanically sound
- whether the saved workspace truly preserves the context that the user sees in the branch scene
- whether the Enron example is a good, honest, inspectable example for a fresh repo user
- whether business-state interpretation is placed in the right layer
- whether adapters and normalization are reusable enough for future datasets
- whether the docs describe the real behavior plainly and accurately
- any stale code, stale assets, or dead paths left behind by this transition

Please bias toward actionable findings that the maintainers would genuinely want to fix.

## Useful context about the Enron example

The flagship saved example is the Debra Perlingiere to Cargill Master Agreement case. The repo now includes:

- the saved workspace
- the saved branch scene
- the counterfactual result artifact
- the business-state comparison artifact
- screenshots showing the predicted business change and the ranked comparison

The intended message for a fresh repo reader is that VEI can start from a real historical decision point, attach only the public facts known by that date, compare several interventions, and express the likely effect in business terms.

## What would count as a strong review

A strong review will separate:

- true correctness bugs
- architectural regressions against the source-agnostic direction
- places where a generic abstraction should replace a narrow fix
- places where docs and saved examples overstate what the system really does

A strong review should also say when the current shape is solid and reusable, not only where it is weak.
