# Rigor

## What this document is

A statement of the rigor problem in VEI as it stands today, what rigor would actually mean for us, what the per-tenant architectural constraint does and doesn't let us calibrate, and the minimum commitment we have to make now so that calibration becomes possible later.

It is not an implementation plan. It is not a roadmap. It is the framing this codebase should be held to whenever someone — human or LLM — adds a new estimator, score, threshold, or weighted sum.

If you are an LLM picking this up cold after a context compaction: do not interpret the existence of citation patterns, schema-constrained outputs, validation gates, or grounding requirements as evidence that the system is rigorous. Those are the *style* of rigor. The *substance* — calibration of model output against observed reality — is mostly absent and has to be earned, not adopted by pattern.

## The vision

The business is not a static dataset. It is a moving target that generates new evidence every day, across every surface VEI ingests — email, tickets, documents, calendars, code, conversations. The daily-refresh pipeline turns that flow into a continuous calibration substrate: each tenant's snapshot today is a slightly more complete corpus than yesterday's, with slightly more ground truth about which of the model's past claims held up.

The framing this enables, and which everything downstream of this document depends on, is:

> **Every model VEI ships is an estimator whose error is continuously observable. Not an artifact frozen at training time.**

The JEPA, the workflow ranker, the world-model opportunity gate, the measurement manifests, the Py Insights daily validation, an LLM forecaster sitting on top of the JEPA — all of them emit numbers whose realizations the corpus will eventually contain. The right architectural shape for each is the same loop:

1. The model emits a prediction with full provenance — what was predicted, under what conditioning, with what resolution criteria.
2. The corpus accumulates the realization over subsequent refreshes.
3. The residual between prediction and realization is computable.
4. Subsequent refreshes update the model so the residual shrinks.

This loop is the product. A per-tenant model whose error empirically decreases over use, with explicit residuals telling the operator how well the model fits *their* business, is qualitatively different from a model that emits plausible-sounding numbers without a way to check them. The latter is decorating; the former is estimating. We want estimators.

This is architecturally achievable under our current constraints. Per-tenant world models foreclose cross-sample triangulation today — counterfactual correctness via comparing tenants who took action A to tenants who took action B is a deliberate future investment, not a current capability. But within one tenant, most of the surfaces we care about have observable residuals (factual prediction, ranker quality, opportunity persistence, vocabulary persistence, validation-gate calibration). Where they don't (counterfactual outputs of the JEPA), **calibrated LLM forecasting** offers a partial answer: an LLM forecaster on top of the JEPA emits propositional forecasts with probabilities; factual resolutions calibrate the forecaster's confidence; that calibration applies to its counterfactual outputs too, under a stated and testable transfer assumption.

The implementation details of the current codebase will change, possibly drastically. The framing should not. Whatever modules end up shipping, they should be loops that close — predictions with provenance, residuals computed, models updated, error reported honestly to the operator. The work to earn this is the work this document is about.

## The problem

VEI has converged on a recurring design pattern across several tracks (workflow mining, skill-map opportunities, target-layer measurement, world-model strategic state points, Py Insights daily refresh). The pattern looks like:

- Outputs are cited to evidence.
- LLM calls use schema-constrained JSON.
- Anything ungrounded is filtered out.
- A composite score is computed from several inputs with weighted coefficients.
- A top-N or threshold gate decides what gets surfaced.

The first three are real discipline. The last two are theatre. The coefficients and thresholds are hand-typed numbers, chosen by whoever wrote that file, with no documented justification and no mechanism to know whether the resulting ranking or gate corresponds to anything correct. They have not been calibrated against operator behaviour, against business outcomes, against held-out time windows, or against anything else. They are guesses that look like physics.

This is a structural property of LLM-assisted codebases: assembling a plausible-looking weighted sum is cheap, while validating that the weights are right is expensive. The cheap part keeps getting shipped and the expensive part keeps getting deferred. The cumulative effect is a system whose surfaces *look* principled and whose claims of principledness are unsupported.

## Concrete instances in tree

Examples, not exhaustive, current as of writing:

- `vei/skillmap/skill_pipeline.py` — the world-model opportunity gate uses `best_skill_score ≥ 0.28` and `len(overlap_terms) ≥ 3` to decide whether a row becomes a "skill upgrade" or a "missing-skill gap." `max_opportunities = 8`. None of 0.28, 3, or 8 have a documented derivation.
- `vei/workflow/api.py` — `_semantic_workflow_quality` returns `usefulness*0.28 + confidence*0.18 + evidence_coverage*0.18 + operational_shape*0.16 + readiness*0.10 + alignment*0.10`. Six coefficients summing to ~1.0. None calibrated against anything.
- `vei/whatif/benchmark_business.py` — `_count_norm(regulatory_hits, 4)`, `_count_norm(liquidity_hits, 5)`, `_count_norm(governance_hits, 5)`. The denominators differ per category; the differences are not documented. Composite heads then multiply these by `0.25`, `0.20`, `0.15` etc.
- `docs/KILL_HARDCODED_KEYWORDS.md` and its `vei mcm` implementation — citations are required in the LLM output; the runtime does not verify that the cited event's text actually contains the term verbatim. We adopted the form (cite to event_id) without the predicate (verify substring presence). The plan would call this a Stage 5 validator concern; today it is unenforced.
- `vei/pyinsights/daily_refresh.py` — the validation gate has real predicates (counts reconcile, files exist) and at least one soft threshold class (source freshness, strategic saturation). The soft thresholds inherit the same problem at a different layer: we know what passes today, we don't know that what passes is correct.

The pattern is consistent across all of these: a number whose realization the corpus *could* eventually contain, emitted with no record of the prediction and no mechanism for the next refresh to score it.

## What rigor actually means

An estimator is rigorous when the loop closes:

1. The model emits a prediction or score, with provenance.
2. The corpus eventually accumulates a realization corresponding to that prediction.
3. The residual between prediction and realization is computable.
4. Subsequent refreshes update the model so the residual shrinks.

The interesting word is "eventually." Static-data calibration is a luxury we mostly don't have; what we do have is a daily-refresh substrate that turns our tenants into a continuous source of ground truth, one day at a time. The right framing of every model we ship is not "an artifact" but "an estimator whose error is continuously observable." If a piece of code emits a number and no future refresh can score that number, the code is not estimating anything; it is decorating.

This reframing is the central commitment of this document. Everything else follows from it.

## The substrate

Each daily refresh per tenant writes:

- A new skill map.
- A new workflow candidate list with rank scores.
- A new world-model strategic-state-point report.
- A validation manifest (per-check pass/fail).
- Potentially a CEO Counterfactual Report (gated by validation).

The workflow surface accumulates operator artifacts over time:

- `WorkflowLabel` records (good_example / bad_example / etc.).
- Promoted `BusinessTaskSpec` objects.
- Implicit rejection-by-non-action on unlabeled candidates.

Across N tenants × M days that is a real dataset, not a thought experiment. Held-out splits are natural: by tenant (fit on tenants A, B; evaluate on tenant C), by time (fit on weeks 1..N-1; evaluate on week N), by candidate (block-cross-validate within a tenant). The substrate makes online calibration *possible* in a way no purely static evaluation would.

## What is calibrable per-tenant

Single-tenant signals that close the loop without needing population data:

- **Factual prediction.** Given the corpus through time *t*, the world model predicts events at *t+1*. Tomorrow's refresh contains the realizations. Residuals are computable trivially. This is the JEPA's natural surface.
- **Ranker quality.** Top-ranked workflow candidates should get labeled `good_example` and promoted more often than bottom-ranked. Operator behaviour is the ground truth. Caveats below.
- **Opportunity persistence.** A skill opportunity that recurs across refreshes was a real gap; one that disappears was noise the model corrected. The corpus adjudicates without operator input.
- **Vocabulary persistence.** A manifest term that keeps getting cited as the corpus grows is real vocabulary; one that stops being cited has aged out. Same shape as opportunity persistence.
- **Validation-gate calibration.** Did checks that passed correspond to reports that held up? Did checks that failed correspond to real downstream problems? Audit signal, single-tenant.

For all five, the cost of starting to log is small. The cost of not logging is that the historical record is unrecoverable for calibration later.

## What is not calibrable per-tenant

Things that need population data we have deliberately foreclosed for now (per-tenant world models, no cross-sample triangulation):

- **Counterfactual correctness.** "If you had taken action A instead of B, future would have been F" never realizes for the tenant that took B. Without a population of tenants who took A, the counterfactual claim has no empirical ground. The JEPA can produce counterfactual rollouts that are internally consistent with the factual patterns it learned, but internal consistency is not correctness.
- **Cross-tenant similarity transfer.** "Tenants like yours did X" claims need population data. Seeding a new tenant's manifest from a similar tenant's manifest does too.
- **Causal claims.** Within one tenant we observe correlations along the single trajectory the tenant traced. Cause requires variation we don't have.

The honest framing: per-tenant, we have a *forecaster* (factual prediction) plus a *constrained extrapolator* (counterfactual rollouts that obey learned structural rules). We do not have a "world model" in the sense that lets us ask counterfactual questions and get empirically-correct answers. Using "world model" colloquially for the JEPA is fine, but product surfaces should not promise more than the architecture can deliver.

## The product tension

This is uncomfortable and worth stating directly. The most user-valuable surfaces in VEI are the counterfactual ones: the CEO Counterfactual Report, world-model skill opportunities, strategic state points. These are also the surfaces whose correctness we cannot empirically calibrate per-tenant. The least user-exciting surfaces (workflow rank, opportunity persistence, vocabulary persistence) are the ones with clean per-tenant ground truth.

Calibration rigor is highest where excitement is lowest, and vice versa.

This means two things:

1. Where we *can* calibrate, we should be loud about it. "The model's factual forecast residual on this tenant shrank from X to Y over Z refreshes" is a real claim and should be the user-visible signal that the system is improving.

2. Where we cannot calibrate, we should be quiet. The CEO Counterfactual Report should not be framed as "predictions of what would have happened" but as "scenarios the model finds internally consistent given your history." That is an honest claim that survives the architectural constraint. The fancier framing is overclaiming.

## Calibrated LLM forecasting: the partial answer

There is a real way to make some progress on counterfactual correctness without cross-tenant data. The pattern is:

1. The LLM emits *probabilistic propositional forecasts*, conditioned on a state and a branch: "P(customer churn within 30 days | branch X) = 0.42."
2. Some branches are factual (the tenant actually took the path); the rest are counterfactual.
3. Factual branches resolve over time. The corpus contains the answer.
4. Resolved factuals give a per-tenant, per-prompt-class reliability diagram: among the LLM's "0.4" forecasts, what fraction actually held?
5. Apply the resulting calibration mapping to subsequent forecasts — *both* factual and counterfactual.

The point is that we are not calibrating the counterfactual claim directly. We are calibrating the *forecaster's claim about its own confidence*. If the LLM is well-calibrated in the 0.4–0.6 range on factual outcomes for this tenant, we have a defensible reason to treat its 0.4–0.6 counterfactual outputs as comparably calibrated — *given* one load-bearing assumption.

## The transfer assumption

The assumption that does the heavy lifting in the previous section: **the LLM's confidence calibration transfers from factual to counterfactual prompts within the same prompt class.** Plausible but not automatic. The LLM might be systematically more confident on counterfactuals than on factuals (you can fabricate alternate futures more freely than predict the actual one). It might be systematically less. The architecture has to test this assumption rather than assume it:

- Log factual and counterfactual forecasts with prompt class.
- Compare confidence distributions for factual vs counterfactual prompts within each class.
- If they differ materially, soften the transfer claim and/or apply class-specific calibration corrections to the counterfactual side.

If a future LLM reading this skips that check and just applies factual calibration to counterfactual output, the rigor is gone again. The check is the rigor.

## Architecture this implies

A defensible shape for the JEPA + LLM stack under per-tenant constraints:

- **JEPA**: factual encoder and predictor of latent state. Never directly user-facing. Trained against future event embeddings. Its outputs are continuous, hard to interpret, and naturally scored only via downstream tasks.
- **LLM forecaster**: reads JEPA state plus corpus context and emits *propositional* forecasts with explicit probabilities and resolution criteria. Outputs have truth values. The LLM is the readout that turns continuous latent state into statements that can be scored.
- **Daily refresh**: logs every forecast (factual and counterfactual) with full provenance — prompt, class, conditioning state, probability, resolution criteria. Resolves matured forecasts as the corpus arrives. Updates per-tenant, per-class reliability diagrams. Applies calibration to subsequent output.
- **User-facing surfaces**: consume *calibrated* probabilities and can honestly cite "the model has emitted N forecasts in this class; calibration error at confidence 0.4 is ε."

This is the shape that closes the loop. The JEPA does prediction; the LLM does grading-by-proposition; the refresh does scoring; the user sees calibrated output with an honest accounting of how much evidence the calibration rests on.

## Caveats that survive the architecture

Even under this design:

- **Cold start.** The first N forecasts are uncalibrated by definition. Reliability diagrams need samples. Anything emitted before the calibration matures must be marked as uncalibrated and treated as such.
- **Selection bias on forecasts.** The LLM is only calibrated on forecasts it emits. Forecasts it never makes (because the prompt doesn't elicit them) cannot be calibrated. This affects coverage, not correctness, but it matters: "the model is well-calibrated at 0.4 confidence" does not imply "the model knows everything it ought to forecast."
- **Slow-resolving forecasts.** "P(churn within 12 months)" can't update the calibration map fast. Short-horizon and long-horizon forecasts should be tracked separately.
- **Prompt-class partitioning.** Too coarse: calibration is meaningless ("the LLM is 0.5-calibrated on everything"). Too fine: one data point per class, no statistical mass. This is the same bias–variance trade-off all calibration platforms face. Whatever partitioning we pick has to be inspectable and re-derivable, not hardcoded.
- **Operator label bias.** Labels and promotions are produced by operators who decide what to label and what to promote. Calibrating against labels calibrates against "what an operator thought was good," not against "what was actually good." Still useful, still meaningful, but the framing has to acknowledge the layer.
- **Cadence mismatch.** Some signals (workflow rank vs label) resolve in days. Others (business-outcome KPIs) resolve in quarters. Online calibration only helps within the resolution cadence of the signal being calibrated.

None of these break the framing. All of them have to be visible in the calibration manifest so consumers know how much weight to put on the calibrated number.

## The minimum architectural commitment

Survives all of the above and is non-negotiable:

> **Every model-emitted probability and every counterfactual proposition is logged with enough provenance — prompt, class, conditioning state, probability, resolution criteria — that a future refresh can resolve it and score it.**

Cost of doing this now: small. A handful of additional fields per output, a sidecar manifest, a resolution-tracking schema.

Cost of not doing this now: unrecoverable. Every refresh that emits ungrounded numbers and discards the prediction-side context destroys exactly the data the next refresh would need. The calibration story for past output is then permanently lost, and the only data we ever have is what we start logging today.

Anything we ship that emits a score, probability, or ranking that the corpus *could* eventually resolve, but whose prediction-side state is not logged, is a violation of this commitment. Not logging is the only move that cannot be reversed.

## What this document is not

- Not a claim that we have rigor today. We do not.
- Not a plan to add rigor next sprint. The work to earn rigor is real and unfinished.
- Not an endorsement of any specific calibration technique over another. Brier scores, log loss, isotonic regression, Platt scaling, Bayesian posteriors — these are tools. The framing here is upstream of the tool choice.
- Not a license to keep emitting hand-typed weights and thresholds because "calibration is coming." The cheapest move when an existing weighted sum can't be justified is usually to delete the weighted sum, not to add calibration infrastructure for it. Filter where you cannot rank; rank only where you can defend the ranking.

## For future LLMs picking this up cold

A short list of things to look out for:

- A new file containing weighted-sum scoring (`a*0.X + b*0.Y + ...`) with no comment block citing what the weights are calibrated against. Treat as a code smell. Either add the calibration target or replace with a simpler scheme.
- A new threshold in source (`if score > 0.42: ...`) with no comment block citing where 0.42 came from. Same.
- A new top-N cap (`max_results = 8`, `limit = 10`) with no defensible derivation. Same.
- A "scenario," "prediction," or "counterfactual" surface that promises correctness against an alternate reality. The architectural constraint (per-tenant world models, no cross-sample triangulation) makes that promise unsupportable today; rephrase as internally-consistent extrapolation.
- A new "fallback" path kept alive as a "diagnostic." Verify it has a real consumer; if not, the right move is deletion, not demotion.
- An LLM-driven output that adopts the *form* of citation (event_id references) without the *predicate* (verbatim substring check against the cited event's text). The form is not the rigor; the check is the rigor.
- A "manifest," "validation," or "calibration" surface whose presence is treated as evidence of correctness rather than as a record of provenance. Manifests document; they do not validate. Validation happens at runtime via observable residuals.

The work this document points at is not finished. The point of writing it down is so that the framing survives context compaction and the next contributor — human or model — can hold their own work to the standard rather than re-derive it under pressure.
