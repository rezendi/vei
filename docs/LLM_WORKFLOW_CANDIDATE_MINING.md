# LLM Workflow Candidate Mining

This document describes a plan to replace the keyword-driven candidate scoring inside `vei/workflow/api.py::mine_workflows` with a parallel LLM-based mining pass that emits its own candidates and its own salience scores. The deterministic grouping pass is preserved as a structural baseline. The hand-tuned `_BUSINESS_TERMS` and `_ESCALATION_TERMS` keyword sets are retired. The rank formula collapses from six heuristic terms to three structural ones.

The goal is a v1 mining surface that is honest about what it is doing — semantic grouping is a semantic task and is delegated to an LLM, structural grouping stays deterministic, and predictive modeling stays out of the mining stage entirely.

## Why this versus the status quo

Mining today is the weakest link in the workflow intelligence pipeline. It does four things, all deterministic:

1. Groups events by `case_id` / `thread_ref` / object reference. One group becomes one candidate.
2. Buckets candidates into "patterns" by string equality on a five-component key over surfaces, event-kind families, and the top-five stopword-filtered tokens of title and snippet text.
3. Scores each candidate with a hand-tuned linear combination that includes `escalation_score` (keyword count over `_ESCALATION_TERMS`) and `business_hits` (keyword count over `_BUSINESS_TERMS`) terms.
4. Sorts and truncates to a fixed limit.

This is the simplest thing that produces *something*, and it produces a usable v1 stream of candidates. But it is shallow in three concrete ways:

- **No semantic grouping.** "Morning Dispatch Board" and "AM Dispatch Coordination" do not bucket together unless their tokens overlap. Re-titling a thread breaks pattern continuity. Synonymy, role abstraction, and process-boundary detection are absent.
- **No cross-case workflows.** A "weekly billing reconciliation" that touches dozens of tickets is not discoverable as one candidate; each ticket becomes its own candidate.
- **Tenant-implicit keyword tuning.** `_BUSINESS_TERMS` and `_ESCALATION_TERMS` are baked into source. They are implicitly Enron- and service-ops-shaped. Applying them to a law firm or a hedge fund makes the ranking noisier without anyone noticing it has happened.

We considered using the JEPA at mining time — driving candidate discovery from windows of high model surprise. We rejected it. Surprise tells you "something weird happened here," not "these events belong to the same process." Conflating the two muddles the JEPA's calibration story (next-event NLL stops being a clean metric once mining quality also depends on encoder quality), and it asks the JEPA to do a job it is not shaped for.

An LLM at mining time is the right move because the operation is semantic. Reading structured event windows, deciding which constitute one process, naming them, and distinguishing variants is exactly what an LLM is good at, and it is cheap enough at offline batch volumes to ignore on the budget. It also lets the JEPA stay narrow: predict the next event, report held-out NLL, do not touch mining.

## Objective

Replace keyword-driven scoring inside `mine_workflows` with a two-track architecture that produces a merged candidate set the downstream pipeline (`label → promote → rubric → contract → package`) consumes unchanged. Retire `_BUSINESS_TERMS` and `_ESCALATION_TERMS`. Reduce the rank formula to three structural terms. Add an LLM mining pass that handles boundary detection, cross-case grouping, and variant classification, and emits its own salience scores with one-line justifications.

The objective explicitly excludes:

- Replacing the deterministic grouping pass. It is fast, reproducible, and gives a structural sanity floor; we keep it.
- Wiring the JEPA into mining. The JEPA stays at readout time only.
- Authoring rubrics or contracts. The LLM produces candidates; humans still review and promote.
- Learning the rank weights from feedback. The three structural weights stay hand-set for v1.

## Scope

First implementation targets the existing Clearwater service-ops fixture (`docs/examples/clearwater-technician-no-show/`) plus the Enron mail bundle. Both have canonical event streams already produced by the existing event spine. Both have small enough event counts that a full LLM mining pass costs under \$10 and runs in minutes. Multi-tenant rollout follows once the architecture is settled on these two.

## Architecture

```
canonical events
   │
   ├─► deterministic mining (preserved, simplified)        ─┐
   │     • group by case_id / thread_ref / object_ref       │
   │     • per-group candidates                             │
   │     • structural rank only (three terms, no keywords)  │
   │                                                        ├─► merged candidate set
   ├─► LLM mining pass (new)                               ─┤      → label → promote → rubric → contract → package
   │     • boundary detection                               │
   │     • cross-case grouping                              │
   │     • variant classification                           │
   │     • LLM-emitted salience + justification             │
   │     • cited event_ids verified deterministically       │
   │                                                       ─┘
   └─► JEPA training (unchanged, mining-independent)
         • next-event prediction, held-out NLL
         • used at readout, never at mining
```

The two mining tracks are independent. They both consume the same `CanonicalEvent` stream and both emit `WorkflowCandidate` records into the same downstream pipeline. Each candidate uses the existing schema: cited LLM members are stored in `source_event_ids`, and source/ranking provenance is stored under `metadata` (`generated_by: "deterministic"` or `generated_by: "llm"`). The reviewer sees a merged, sorted list; downstream tools (label, promote) do not care about the source.

## Methodology

### Stage 1 — Simplify deterministic mining

In `vei/workflow/api.py`:

- Remove `_BUSINESS_TERMS` and `_ESCALATION_TERMS` constants.
- Remove `escalation_score`, `labelability_score`, and `business_hits` terms from `_candidate_scores`.
- Replace `_pattern_tokens` with a structural-only pattern key (surfaces + event-kind families + repetition signature on actor roles). String tokens drop out of the bucketing.
- Collapse `rank_score` to:
  ```
  rank_score = 0.5 * normalize(event_count)
             + 0.3 * cross_surface_score
             + 0.2 * normalize(repetition_count)
  ```
  where `normalize` is `min(1, x / ceiling)` with documented ceilings. Three weights, all on structural quantities, all defensible from a one-line argument.

The deterministic pass now produces a stable, reproducible, keyword-free baseline. It is not expected to find cross-case workflows or to cluster synonymous threads — those are explicitly the LLM's job.

### Stage 2 — LLM mining pass

A new module `vei/workflow/llm_mining.py` exposes:

```python
def mine_workflows_with_llm(
    source_dir: str | Path,
    *,
    output: str | Path | None = None,
    limit: int = 25,
    model: str = "claude-haiku-4-5",
    cache_root: str | Path | None = None,
) -> WorkflowMiningResult: ...
```

Behavior:

1. **Window the event stream.** Partition canonical events into overlapping windows by time and case. Default: 50 events per window with 10-event overlap. Windows are deterministic, hash-keyed, and cached.
2. **Render each window to a structured prompt.** Each event becomes a single line with structural fields (`event_id`, `actor_role`, `surface`, `kind`, `object_refs`, `timestamp_delta_from_prior`, `snippet`). Snippets are truncated to a fixed character limit. No raw HTML, no headers, no quoted-reply chains.
3. **Call the LLM with a fixed schema.** The prompt asks the model to identify recurring processes within the window and emit a JSON array of candidates. Each candidate must include:
   - `title`: short human-readable name
   - `objective_summary`: one sentence on what the process is for
   - `member_event_ids`: list of event_ids drawn from this window only
   - `variant_class`: one of `happy_path`, `with_rework`, `with_escalation`, `other`
   - `salience`: 0..1 score with reasoning
   - `salience_reason`: one-line justification
4. **Verify citations deterministically.** A post-check rejects any candidate whose `member_event_ids` are not all present in the input window. Rejected candidates are logged with the reason; they do not silently propagate.
5. **Cluster across windows.** A second LLM pass takes the per-window candidates and clusters synonymous and continuation candidates — "this is the same process as that one in the next window." Output: cross-window candidate groups, each with merged `member_event_ids` and a single canonical title. Verified the same way.
6. **Cluster across cases.** A third pass identifies multi-case processes — "these candidates from 30 different cases are all instances of the same workflow." This is where weekly billing reconciliation surfaces. Optional in v1 if cost or latency is a concern; the architecture supports it without schema change.
7. **Emit `WorkflowCandidate` records** in the existing schema. LLM `member_event_ids` map directly to `source_event_ids`; no unknown top-level Pydantic extras are passed. Full provenance lives in `metadata`: `generated_by: "llm"`, model id, prompt cache key, window id, variant class, LLM salience, and salience_reason.

Determinism: `temperature=0`, fixed model id, prompt-and-window hash as cache key. Re-runs hit cache and produce bit-identical output. Cache invalidates when prompt template version, model id, or window content changes.

### Stage 3 — Merge and rank

A new helper `merge_candidate_streams` takes the deterministic and LLM candidate sets and produces the merged stream the downstream pipeline reads. Merge logic:

- Deterministic candidates whose `source_event_ids` overlap heavily with an LLM candidate's `source_event_ids` are absorbed into the LLM candidate, with `metadata.merged_from: [det_candidate_id, ...]` provenance. The LLM title and salience win.
- LLM candidates that do not overlap any deterministic candidate stand alone. The deterministic structural rank is computed for them post-hoc to give the merged sort key a stable baseline.
- Deterministic candidates that have no LLM overlap stand alone with their structural rank.

Final sort key:

```
final_rank = 0.5 * structural_rank + 0.5 * llm_salience
```

For deterministic-only candidates, `llm_salience = 0`. For LLM-only candidates, `structural_rank` is computed against the candidate's events. Ties broken by `event_count`, then `cross_surface_score`, then candidate id.

For downstream compatibility, `rank_score` is set to `final_rank`. The component scores are retained in `metadata.structural_rank`, `metadata.llm_salience`, and `metadata.final_rank`; this avoids a `WorkflowCandidate` schema migration for v1 while keeping the audit data available.

The reviewer sees the merged ranked list. Each candidate's row reads `generated_by`, `final_rank`, `structural_rank`, `llm_salience`, and the LLM's `salience_reason` from metadata when present. Sort can be flipped to either source-only.

### Stage 4 — Provenance manifest

Every mining run writes a `mining_manifest.json` alongside `workflow_candidates.json`:

```json
{
  "mining_version": "workflow_mining_v2_llm",
  "deterministic": {"version": "structural_only_v1", "weights": {...}},
  "llm": {
    "model": "claude-haiku-4-5",
    "prompt_template_version": "v1",
    "temperature": 0,
    "windows": 142,
    "windows_cached": 138,
    "windows_called": 4,
    "llm_token_input": 312841,
    "llm_token_output": 18402,
    "estimated_cost_usd": 0.41,
    "rejected_for_invalid_citations": 3
  },
  "merged": {"deterministic_only": 17, "llm_only": 22, "merged_pairs": 19}
}
```

This is the artifact a reviewer or auditor reads to know what produced the candidate set. No mining run lacks one.

### Stage 5 — Validation on the two fixtures

Run end-to-end on Clearwater service-ops and Enron mail bundles. Validate by:

- **Candidate-set sanity.** Manual review of top-25 candidates from each source. We expect to see (a) the same descriptive workflows the deterministic pass already finds, plus (b) at least three cross-case or synonymous-merger candidates the deterministic pass misses.
- **Citation soundness.** 100% of accepted LLM candidates cite event_ids that exist in the source. Rejection rate logged but not zero — some rejections are healthy.
- **Cost.** Full Clearwater pass under \$1, full Enron pass under \$15. If higher, revisit windowing.
- **Reproducibility.** Two consecutive runs of `vei workflow mine --backend llm` on the same source produce bit-identical `workflow_candidates.json`.
- **Downstream compatibility.** `vei workflow label`, `vei workflow promote`, and `vei workflow package-env` all run unchanged on the merged candidate set.

If any validation fails, fix before merging. None of these are stretch goals; they are the contract for v1.

### Stage 6 — Retire the keyword stack

Once Stage 5 passes on both fixtures:

- Delete `_BUSINESS_TERMS`, `_ESCALATION_TERMS`, `_PATTERN_STOPWORDS` from `vei/workflow/api.py` (keep stopwords as the smallest portable list if needed for the structural pattern key).
- Update `tests/test_workflow_intelligence.py` to remove fixtures that depended on keyword scoring.
- Document the change in `docs/AGENT_ONBOARDING.md` and the workflow walkthrough README.

## Calibration

Each subsystem has one job and one calibration metric. None validates another:

- **Deterministic mining**: reproducibility (bit-identical re-runs) and citation soundness (every event_id exists in source).
- **LLM mining**: citation verification rate (post-check rejects invalid candidates; rejection rate logged) and reviewer-judged candidate-set quality on a held-out sample.
- **JEPA**: held-out next-event NLL, separately and untouched.
- **Rubric tier**: judge inter-rater agreement on a sample of episodes.
- **Contract tier**: predicate evaluation is mechanical; no calibration needed.
- **RL tier**: standard policy evaluation against the contract.

The property that matters: no system's validity depends on another's quality. If mining gets better, JEPA NLL is unchanged. If JEPA gets better, mining output is unchanged. The calibration stories compose.

## Deliverables

- `vei/workflow/api.py` simplified: keyword stack removed, rank formula collapsed to three structural terms.
- `vei/workflow/llm_mining.py` new module: windowing, prompting, citation verification, cross-window and cross-case clustering, cache.
- `vei/workflow/merge.py` new module: merge_candidate_streams, final ranking.
- CLI: `vei workflow mine` gets `--backend [deterministic|llm|merged]` flag, default `merged`. Existing scripts that pin `--backend deterministic` continue to work.
- `mining_manifest.json` artifact emitted alongside every mining run.
- Tests: `tests/test_workflow_llm_mining.py` covering windowing determinism, prompt rendering, citation verification, cross-window clustering, merge logic.
- Doc updates: `docs/examples/workflow-intelligence-walkthrough/README.md`, `docs/AGENT_ONBOARDING.md`, `docs/GLOSSARY.md`.

## Decision points and stop conditions

- **After Stage 1.** If the structural-only deterministic ranker produces obviously worse ordering than the keyword-tinted one on Clearwater and Enron, do not proceed; investigate whether keyword scoring was load-bearing in ways we did not see.
- **After Stage 2 first pass.** If LLM citation rejection rate exceeds 15% on either fixture, the prompt is too loose. Revise prompt template before continuing. If cost on Enron exceeds \$30 for a full pass, revisit windowing before continuing.
- **After Stage 3.** If the merge logic produces fewer than three LLM-only or merged-pair candidates that the deterministic pass missed, the LLM pass is not earning its keep — investigate before retiring keywords.
- **After Stage 5.** If reviewer-judged candidate-set quality on the held-out sample is not clearly better than the deterministic-only baseline, do not retire the keyword stack in Stage 6. Hold the LLM pass as opt-in until quality is settled.

## Risks and mitigations

- **LLM hallucinated processes.** Mitigated by deterministic citation post-check. Every candidate must cite real event_ids; non-citing candidates are dropped, citing-but-invented candidates fail verification. Residual risk: candidates that cite real events but assert relationships that are not real. Mitigated by reviewer in the label stage; not a v1 blocker.
- **Non-determinism.** Mitigated by `temperature=0`, model-id pinning, and prompt-and-window hash caching. Re-runs hit cache; cold re-runs after model deprecation will not be bit-identical, which is a known and acceptable tradeoff.
- **Cost drift on large tenants.** Mitigated by per-tenant cost cap in the manifest; mining run aborts before exceeding cap and emits a partial manifest. Operator decides whether to raise the cap or accept partial coverage.
- **Prompt template drift over time.** Mitigated by `prompt_template_version` in the manifest. Changing the template invalidates cache and is logged as a versioned change. Old runs remain reproducible against their original template.
- **Vendor lock to one model family.** Mitigated by abstracting the LLM call behind `vei/llm/client.py` (already exists for other LLM uses in the codebase). Model id is a config field, not a hardcode.
- **Reviewer overload.** A merged set may surface more candidates than the reviewer wants to triage. Mitigated by `final_rank` truncation (default top-25) and by `--backend deterministic` for fast triage of structural baseline only.

## What this is and is not

**This is** a one-stage upgrade to candidate mining that retires keyword scoring, adds semantic grouping via an LLM, and keeps the JEPA out of the mining loop. The downstream pipeline is unchanged.

**This is not** a replacement for human review. LLM candidates land in the descriptive tier, same as deterministic candidates. They become labeled, rubric-evaluable, contract-evaluable, and rl-packaged only through the existing human-in-the-loop ladder.

**This is not** a use of the LLM as a judge, a rubric author, a contract writer, or a reward source. The LLM does one thing: group events into candidate processes and score their salience. Promotion to higher evaluation tiers stays a human decision.

**This is not** a calibration of the LLM mining pass against the JEPA. The two systems are independent by design. Mining quality is judged by reviewer assessment and citation soundness; JEPA quality is judged by held-out NLL. Neither validates the other.

**This is not** the discovery agenda from `DISCOVER_PREDICTIVE_FEATURES.md`. That work, if pursued, lands on the JEPA encoder and is unrelated to mining. The two plans compose without conflict.
