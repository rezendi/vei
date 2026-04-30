# Discover Predictive Features: Diagnosing the Hand-Made Taxonomy

## Why this, vs. the status quo

The current what-if pipeline depends on hand-made formulae at every layer except the JEPA's transformer encoder. Event flags (`consult_legal_specialist`, `is_escalation`, `has_attachment_reference`) are substring matches against hand-picked keyword bags. The 22 evidence counters that the JEPA is supervised against are tallies of further keyword bags walked over the realized future event log. The six future-state heads (`regulatory_exposure`, `accounting_control_pressure`, `liquidity_stress`, `governance_response`, `evidence_control`, `external_confidence_pressure`) and the five business heads (`enterprise_risk`, `commercial_position_proxy`, `org_strain_proxy`, `stakeholder_trust`, `execution_drag`) are clamped weighted sums of those counters with hand-picked weights and normalization targets. The reported `balanced_operator_score` is an equal-weighted mean over five business heads. None of these layers were calibrated against external truth; they were specified by hand.

The names attached to those layers carry semantic weight that the mechanics underneath do not earn. "Stakeholder trust" sounds like a measurement; it is a substring count of `please/thanks/appreciate/confirm` plus a normalized contribution from `executive_escalation_count`. "Liquidity stress" sounds like a financial-state estimate; it is a substring count of `liquidity/credit/moody/fitch/runway/fundraising` plus a normalized contribution from `urgency_spike_count`. When WHATIF.md reports AUROC of 0.79 and Brier of 0.33 on the held-out Enron split, the strict reading is that the JEPA has learned to reproduce *the values these formulae produce* on the realized future to that calibration. The model has not been validated against any external notion of stakeholder trust or liquidity stress.

This raises an unanswered question: which of the named axes correspond to real structure the data supports, and which are formula artifacts that survive only because no one has tested them? It also raises a second question: are there predictive axes the data finds important that the current taxonomy doesn't name at all? The current pipeline cannot answer either. A self-supervised representation-learning study can. The output is not a replacement for the current heads — it is a diagnostic of the current heads, plus evidence about what the model would discover if not constrained to the formulae.

The reasons to do this rather than continue with the status quo are concrete:

1. The named axes ship in user-facing artifacts (CSVs, decision summaries, markdown reports), and people read them as measurements. If a substantive fraction don't correspond to features the model has actually learned, the framing on those outputs is overclaim. The diagnostic surfaces that.
2. Calibration claims about predicted heads currently mean "calibrated against the formula." A feature-discovery study generates an alternative: calibration against axes the data anchors on, which is a stronger and more transferable claim.
3. Hand-picked weights and keyword bags are the most likely place latent biases hide (e.g., `consult_legal` weighted positively in the old risk formula, which is causally backwards). A discovery study identifies axes the model finds important without the bias path through human formula design.
4. If the project ever expands into RL or model-based decision support, knowing which heads are real and which are artifacts is load-bearing — every formula artifact that ends up in a reward function is a reward-hacking surface.
5. The work is bounded. It is a 4–8 week research effort with a clear stop condition at every stage. It produces a paper-shaped artifact, not a product replacement.

The deliverable is explicitly *not* "replace the formulae with discovered features." It is a partitioned report: which named axes the model has internally validated, which it has not, which predictively useful axes are not currently named, and what fraction of the model's predictive variance lives in human-describable structure versus the residual.

## Objective

Train a JEPA on Enron event sequences with a self-supervised next-event prediction loss only — no keyword formulae in the loss. Train sparse autoencoders over its internal representations. Characterize the resulting features through maximum activating examples, ablation, counterfactual editing, cross-seed stability, and probing classifiers against the current named axes. Partition the discovered features into tiers reflecting how cleanly each corresponds to a nameable concept, and report what fraction of the model's predictive content the current taxonomy covers.

The objective is diagnosis. The objective is not to ship a new head set. Whether to act on the diagnosis — by retiring named axes, adding new ones, or rebuilding the supervision target — is a separate decision the diagnosis informs.

## Scope

In scope:

- One corpus only for the first run: Enron. It is the largest single source of real human enterprise communication, has known downstream events that can serve as outcome anchors for some validation steps, and avoids the cross-corpus interpretation confounds that pooling introduces. Cross-corpus generalization is a follow-up question, not part of this study.
- One model architecture: a JEPA with the existing transformer encoder + predictor backbone, with the formula-derived regression / business / future-state heads removed from the training loss. Self-supervised next-event prediction (structured features + hashed text fingerprint reconstruction) is the only training signal.
- Sparse autoencoders applied to the encoder output and the predicted latent, separately. Standard top-K or L1-regularized SAE with dictionary size 8–16× the model dim.
- Feature characterization through the methodology described below, with explicit tier assignment.

Out of scope for the first run:

- Cross-corpus features and transfer claims.
- Replacing the production heads in user-facing artifacts. The current pipeline keeps running unchanged during this study.
- Building a new RL substrate on top of discovered features. Reward design against discovered axes is a separate question that requires the diagnosis as input but is not part of producing it.
- Validating that discovered features correspond to externally meaningful business outcomes beyond what is already in the Enron event log. That is a richer outcome-anchoring question that needs a different data acquisition path.

## Methodology

### Stage 1 — Train a self-supervised JEPA

Build a variant of the existing `JEPAOutcomeModel` whose only training targets are next-event structural features (event type, recipient scope, external recipient count bucket, attachment flag, time-since-prior bucket) and a reconstruction loss on the hashed text fingerprint of the next event's content. The 22-dim regression head, the 5-dim business head, the 6-dim future-state head, and the 5-dim objective head are all removed from the loss. The model still has a JEPA-style latent prediction objective: encode the prior `K` events plus a candidate next-event-structured-features vector, predict the latent of the next event's encoded representation, train against a `.detach()`-stopped target encoding of the actual next event.

Train on Enron with thread-level held-out splits and a separate actor-level hold-out (to detect memorization of specific people's patterns). Stop condition for this stage is factual next-event AUROC on held-out reaching a calibration that beats the current pooled JEPA's structural-counter calibration on Enron. If the self-supervised model cannot match that bar, the rest of the study is not worth running, because the representations it produces will not be informative.

The artifact at the end of this stage is a checkpoint, a held-out evaluation report on factual structural prediction, and a frozen activation cache for use in subsequent stages.

### Stage 2 — Train sparse autoencoders over hidden states

Train SAEs on two activation sites: the output of the context encoder (the 96-dim representation pre-prediction) and the predicted latent (the 96-dim post-prediction representation). These represent different stages of the computation; running both lets us distinguish features the model uses to *understand the current state* from features it constructs to *predict the future state*.

Use top-K or L1-regularized SAE with dictionary size 768–1536 (8–16× model dim). Train at least three seeds per activation site for stability analysis. Use a large activation cache covering all training and validation branch points; do not train SAEs only on training-set activations because that introduces a memorization confound.

Stop condition for this stage is dual: (a) reconstruction loss on held-out activations is acceptable (the SAE represents the activation space well enough to be informative), and (b) features show non-trivial sparsity (each input activates a small fraction of the dictionary, with a long-tailed distribution). If the SAE can only reconstruct by activating most features densely, the dictionary is not finding sparse structure and the rest of the analysis is unreliable.

### Stage 3 — First-pass feature characterization

For each feature in each SAE, compute and store: (a) the top 50 activating branch points, (b) the activation distribution histogram across the validation split, (c) Pearson and rank correlation against every hand-coded head value, every structural counter, every binary outcome label that exists in the Enron event log (legal involvement appeared by day 7, executive joined the thread, external disclosure occurred, etc.), and (d) a candidate description from an LLM given the top activating examples. The LLM-naming step uses a separate inference call with an explicit instruction to refuse to summarize when the activating examples don't share a clear pattern.

The output of this stage is a feature catalog: ~768–1536 features per SAE, each with quantitative correlation profiles and a candidate description (or a "no clear pattern" tag).

### Stage 4 — Validation

For each feature with a candidate description above a confidence threshold, run three validation tests.

**Ablation.** Zero the feature's activation on its top activating examples, run the model forward, measure the change in factual next-event prediction quality and the change in any predictively-relevant downstream labels in the corpus. A feature whose ablation shifts a specific prediction has an established functional role.

**Counterfactual editing.** Take a high-activation example and produce targeted edits — remove external recipients, change escalation language, redact attachments, etc. Re-encode and observe activation change. A feature whose activation drops under a specific edit responds to the edited property; a feature that activates regardless of edits is responding to a confound.

**Cross-seed stability.** Compare features across the multiple SAE seeds. Identify features that appear consistently (same activation patterns, same top examples) across at least two of three seeds. Features that appear in only one seed are noise and are dropped from the catalog.

The output of this stage is a validated catalog: features that survive the three tests retain their candidate descriptions; features that fail one or more lose their descriptions but keep their functional-role data.

### Stage 5 — Compare against the current taxonomy

For each named axis in the current pipeline (the 22 evidence counters, 6 future-state heads, 5 business heads), train a probing classifier from SAE feature activations to predict the formula value of that head. Use linear probes first; escalate to small MLPs only when linear fails. Probing is run on held-out activations.

A named axis whose value is well-predicted by a small linear probe over a small number of features is *model-supported*: the model has internally represented something equivalent to that formula. A named axis that is well-predicted only by complex non-linear combinations of many features is *fragmented*: the model represents components of the concept but not the concept as a unit. A named axis that is not well-predicted at all is *not model-supported*: the formula's value is not represented in the model's internal state, which means the model's predictions of it are essentially noise driven by whichever features happen to weakly correlate with it.

The output of this stage is a per-axis classification (supported / fragmented / not supported) with quantitative evidence.

### Stage 6 — Diagnostic report

Partition the validated feature catalog into five tiers:

- **Tier A — clean named correspondence.** Features that have stable cross-seed presence, a candidate description that survives ablation and counterfactual editing, and high correlation with at least one named axis. These validate existing taxonomy entries.
- **Tier B — partial / polysemantic.** Features that have stable presence and predictive importance, but whose top activating examples mix two or more recognizable patterns. These suggest the model has factored a property humans currently treat as unitary.
- **Tier C — structural / functional only.** Features that lack a clean human description but whose ablation measurably degrades prediction quality on at least one outcome. Predictively important, descriptively opaque. Report the functional role; do not assign a name.
- **Tier D — named axes that don't show up.** From Stage 5: named axes whose probing classifiers fail. These are the diagnostic finding most consequential for the current product surface — formulae that produce numbers but don't correspond to model-internal structure.
- **Tier E — noise.** Features that fail cross-seed stability or fail to validate. Discarded.

The headline metrics in the report are: (1) what fraction of the validation-set predictive variance is captured by Tier A + Tier B features (the model's predictive content that overlaps with the current taxonomy in some form), (2) what fraction is captured by Tier C features (predictive content the taxonomy misses entirely), (3) which Tier D axes are flagged for taxonomy revision, and (4) what fraction of features fall in each tier.

The report does not recommend specific product changes. It produces the evidence on which such recommendations could be based.

## Deliverables

A single Markdown report under `_vei_out/discover_features/<run_id>/` containing the tier breakdown, headline metrics, per-tier representative examples, and per-axis Stage 5 outcomes. Alongside it, a JSON artifact with the full validated feature catalog (top activating examples, correlation profiles, ablation deltas, candidate descriptions where applicable). A reproducibility manifest with the JEPA checkpoint hash, SAE checkpoint hashes (per seed), training-data hash, validation-data hash, and the LLM model and prompt hashes used for naming.

A second markdown summary under `docs/examples/discover_features_<run_id>/` for stakeholder communication: short, prose-first, with the headline metrics, three to five representative findings per tier, and an explicit "what this does and does not mean" framing section.

## Decision points and stop conditions

After Stage 1: if the self-supervised JEPA cannot match the current pooled JEPA's structural-counter calibration on Enron, stop. The representations will not be informative.

After Stage 2: if the SAEs cannot reconstruct activations under non-trivial sparsity, stop. The dictionary is not finding the structure that downstream analysis depends on.

After Stage 3: if the LLM-naming step refuses for more than ~70% of features and the correlation profiles are diffuse, the SAE may be operating on a representation where features are too polysemantic to characterize. Consider retraining the SAE with different sparsity hyperparameters before continuing.

After Stage 4: if cross-seed stability is below ~30% (i.e., fewer than a third of features appear consistently across seeds), the analysis is not generating reliable findings. Investigate before continuing to the comparison stage.

After Stage 5: report regardless of outcome. Even a finding that most named axes are not model-supported is decision-useful.

## Risks and mitigations

The most likely failure mode is not technical failure but findings that are weaker or more ambiguous than hoped — features that are partially interpretable, axes that are partially supported, no clean story for an executive summary. Mitigation: structure the report around tier sizes and quantified claims rather than a narrative. The honest version of an inconclusive finding is itself useful.

A secondary risk is overclaim from LLM-assisted naming. Mitigation: every name is validated by Stage 4 ablation and counterfactual editing before it appears in the report. Names that fail validation are stripped, leaving the feature in Tier C with a functional-role description only.

A third risk is corpus specificity — features that are real on Enron but don't generalize. Mitigation: this is acknowledged upfront and the report scope is "Enron-specific findings." A separate cross-corpus run is the natural follow-up if the first run produces useful results, but it is not a prerequisite for the first round being valuable.

A fourth risk is that the self-supervised next-event prediction objective produces a representation that is *less* useful than the formula-supervised one, because the formulae provide a stronger learning signal than next-event reconstruction at the small data scales available. Mitigation: the Stage 1 stop condition explicitly tests this. If the self-supervised model loses to the formula-supervised one on factual-counter calibration, the project halts and the conclusion is that the current data is too small for label-free representation learning to be informative — which is itself a useful finding about where the project's bottleneck actually lives.

## What this is and is not

This is a diagnostic of the current taxonomy. It tells you which named axes the model supports, which it doesn't, and what predictive structure exists outside the names you're shipping. It produces evidence to inform decisions about taxonomy revision, head retirement, and where additional outcome labeling would be highest leverage.

This is not a replacement for the current pipeline, a new product surface, an RL substrate, or a calibrated counterfactual analysis tool. The discovered features are descriptions of the model's internal state on Enron data; promoting them to any of those roles requires further work that this study makes possible but does not perform.

The discipline that distinguishes this from the failure mode of "we ran interpretability and found 50 cool things" is the explicit tier structure, the validation gates at every stage, and the commitment to ship the negative findings (Tier D axes that don't show up, Tier C residual that isn't human-narratable) alongside the positive ones. Both kinds of finding are decision-relevant; only reporting the positive ones is how interpretability work loses credibility with the audiences that should be using it.
