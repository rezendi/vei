# Examples

VEI ships two worked example surfaces: real Enron company history and public
news timelines. Both use the same what-if engine, world model, and Studio UI.
For the general command reference, use [WHATIF.md](WHATIF.md).

---

## VEI Control to Skill Map

The [Control skill-map refresh example](examples/control-skillmap-live-map/README.md)
shows a small workspace with company context plus captured agent activity. It
imports the activity into the workspace evidence spine and runs
`vei skillmap refresh` so the living company map updates from real captured
behavior rather than only the static context bundle.

---

## Enron (real company history)

Enron is the repo-owned public company example. Use it when you want a
fresh-clone demonstration of historical replay, public-context slicing,
business-outcome forecasting, and saved Studio bundles.

### What you'll see

- A saved what-if branch point from real Enron email history, viewable in Studio with no API key
- Counterfactual comparisons showing how alternate actions would have changed the company's risk, trust, and execution state
- Eight saved bundles spanning contract control, crisis communications, governance, and disclosure decisions

### What Ships

The normal checkout includes enough data to open and rerun the saved Enron
examples without downloading the full archive:

- a small Rosetta email sample under `data/enron/rosetta/`
- the full archive release manifest at `data/enron/full_dataset_release.json`
- public-company fixtures under `vei/whatif/fixtures/enron_public_context`
- curated public-record fixtures under `vei/whatif/fixtures/enron_record_history`
- saved Enron example bundles under `docs/examples/`
- the shipped reference backend under `data/enron/reference_backend/`

The public context currently contains 11 dated financial checkpoints, 21 dated
public news events, 24 archived public source files, 986 daily stock rows, 7
credit events, and 1 FERC timeline event. VEI slices these facts to the branch
date before showing them in Studio or adding them to benchmark dossiers.

### Full Archive

Fetch the full Enron archive when you want whole-history search, full benchmark
rebuilds, reference-backend training, macro-study rebuilds, or candidate-event
mining:

```bash
make fetch-enron-full
python scripts/check_rosetta_archive.py
```

The fetch command reads `data/enron/full_dataset_release.json`, downloads the
release asset, verifies the checksum, and extracts it into the local cache root
described by the manifest. The current release lives at
`https://github.com/Strange-Lab-AI/vei/releases/tag/enron-dataset-v1`.

`VEI_WHATIF_ROSETTA_DIR` overrides the discovered full-data path when you need
to point VEI at a different archive location.

VEI resolves Enron Rosetta data in this order:

- the workspace manifest source directory
- `VEI_WHATIF_ROSETTA_DIR`
- the fetched full-dataset cache path
- the checked-in sample at `data/enron/rosetta/`
- a workspace-local `rosetta/` folder

<!-- BEGIN GENERATED ENRON CASES -->
### Saved Examples

Start with the Master Agreement example. It is the clearest fresh-clone
walkthrough:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 \
  --port 3055
```

#### Proof examples

- [Enron Master Agreement Example](examples/enron-master-agreement-public-context/README.md)
  - Branch point: Debra Perlingiere is about to send the Master Agreement draft to Cargill on September 27, 2000.
  - What actually happened: The draft went outside quickly, then the thread widened into a long reassignment and redline tail with no visible formal signoff.
- [Enron PG&E Power Deal Example](examples/enron-pge-power-deal/README.md)
  - Branch point: Sara Shackleton is moving a PG&E financial power deal while the counterparty credit picture is deteriorating.
  - What actually happened: The deal thread kept moving through the legal and commercial loop while the wider PG&E situation worsened.
- [Enron California Crisis Strategy Example](examples/enron-california-crisis-strategy/README.md)
  - Branch point: Tim Belden's desk receives a preservation order tied to the California crisis while the trading strategy is still active.
  - What actually happened: The preservation-order thread stayed inside the active crisis loop while the desk was still deciding how far to halt or continue.
- [Enron Baxter Press Release Example](examples/enron-baxter-press-release/README.md)
  - Branch point: The Cliff Baxter press-release loop is active and the company has to decide how transparent, delayed, or reassuring the public message should be.
  - What actually happened: The communications loop moved through a tight internal chain while the company shaped how much to say and how fast to say it.
- [Enron Braveheart Forward Example](examples/enron-braveheart-forward/README.md)
  - Branch point: The Braveheart structure is being forwarded through the valuation and review chain as the company decides whether to reopen the accounting question.
  - What actually happened: The thread kept moving through a narrow finance and legal chain tied to the larger broadband and structure story.

#### Narrative examples

- [Enron Watkins Follow-up Example](examples/enron-watkins-follow-up/README.md)
- [Enron Q3 Disclosure Review Example](examples/enron-q3-disclosure-review/README.md)
- [Enron Skilling Resignation Materials Example](examples/enron-skilling-resignation-materials/README.md)
<!-- END GENERATED ENRON CASES -->

### Business-Outcome Benchmark

The Enron benchmark asks:

From one real Enron decision point, does a candidate action make the later
business state look better or worse on outcomes a company cares about?

Each model sees only the canonical event history before the branch point and a
structured action description for the candidate move. It does not get generated
rollout messages or post-branch summary fields.

The model predicts later evidence that can actually be read from the archive,
including outside-recipient spread, outside forwarding, legal follow-up burden,
review-loop burden, executive escalation, fanout, coordination load, time to
follow-up, reassurance language, disagreement markers, and attachment
recirculation.

VEI converts those evidence heads into five business-facing proxy scores:

- `enterprise_risk`
- `commercial_position_proxy`
- `org_strain_proxy`
- `stakeholder_trust`
- `execution_drag`

These are proxy outcomes. Enron email can support evidence and business proxies;
it does not support true profit ground truth or true HR outcome ground truth.

### Benchmark Commands

```bash
# Build the factual dataset and held-out Enron pack
vei whatif benchmark build \
  --rosetta-dir /path/to/full/rosetta \
  --artifacts-root _vei_out/whatif_benchmarks/branch_point_ranking_v2 \
  --label enron_business_outcome_public_context

# Train one model family
vei whatif benchmark train \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context \
  --model-id jepa_latent

# Evaluate the trained model
vei whatif benchmark eval \
  --root _vei_out/whatif_benchmarks/branch_point_ranking_v2/enron_business_outcome_public_context \
  --model-id jepa_latent
```

### Shipped reference backend

The current fresh-clone headline path is the shipped `full_context_transformer`
reference backend under `data/enron/reference_backend/`. A fresh clone can open
the repo-owned Enron bundles and use that checkpoint without setting an external
path.

The current shipped metrics card reports:

- factual next-event AUROC `0.787817`
- factual next-event Brier `0.332025`
- calibration ECE `0.373951`
- 7,613 train rows and 1,631 validation rows

Use those numbers as the main factual forecasting headline for the repo-owned
Enron path. They are weaker than the earlier mail-heavier checkpoint, and that
gap is the current cost of moving the shipped Enron path onto the thicker
canonical timeline.

### Refresh Paths

```bash
# Refresh saved example bundles and screenshots
make enron-example
make enron-screens

# Refresh the static strangelab.ai/enron browser bundle
python scripts/export_enron_static_assets.py --output ../strangelab.ai/public/enron

# Audit whether the exported browser checkpoint actually separates actions
python scripts/audit_enron_action_sensitivity.py \
  --bundle ../strangelab.ai/public/enron/bundle.json \
  --output _vei_out/enron_model_audit/current.json

# Train an isolated action-sensitive public replay candidate from the full archive
python scripts/train_reference_backend_on_enron.py \
  --output-root _vei_out/enron_action_model_candidate_jepa/reference_backend \
  --benchmark-root _vei_out/enron_action_model_candidate_jepa/benchmark \
  --label enron_action_sensitive_candidate_jepa \
  --model-id jepa_latent \
  --epochs 3 \
  --batch-size 64 \
  --learning-rate 0.001 \
  --device cpu

# When publishing a newly trained action-sensitive checkpoint, make the export fail
# unless same-state candidate actions produce a measurable score spread.
python scripts/export_enron_static_assets.py \
  --checkpoint _vei_out/enron_action_model_candidate_jepa/reference_backend/model.pt \
  --output ../strangelab.ai/public/enron \
  --min-action-score-spread 0.01 \
  --require-action-sensitive

# Refresh public fixtures
python scripts/prepare_enron_public_context.py

# Build or refresh the checked-in sample from a full archive
python scripts/build_enron_rosetta_sample.py

# Refresh the full Rosetta archive itself
python scripts/build_enron_rosetta.py --prefer-local-source --include-content

# Package a new full-dataset release asset
make package-enron-full
```

---

## Public News Timelines

> **Exploratory surface.** News timelines use generic business heads (risk, trust, drag) that are workable but not yet news-native. Treat results as decision support, not historical causal proof. See [Limits](#limits) for details.

### What you'll see

- A dated world model built from public newspaper articles (AmericanStories or PleIAs archives)
- Strategic state-point decisions proposed from pre-cutoff public evidence, scored by JEPA
- A Studio demo with a compact shipped checkpoint spanning 1859–1865 American history

News timelines are the public outside-in example. Use them when you want to test
whether VEI can build a dated world model from public articles, propose
state-level decision points, generate counterfactual actions, and score the
predicted future with the same JEPA path used for company data.

### What This Is

The news setup is not a full internal operating model. It does not see private
emails, tickets, customer records, or meeting notes. It sees dated public
articles or newspaper pages and turns them into canonical timeline events.

The repo also ships a no-key Studio demo with a compact checked-in JEPA
checkpoint and an expanded 1,150-record AmericanStories public-news fixture:

```bash
vei ui serve \
  --root docs/examples/news-public-history-demo/workspace \
  --host 127.0.0.1 \
  --port 3055
```

Open the **Public History** tab to choose a cutoff date, inspect cited
pre-cutoff evidence, and score candidate public actions through the live JEPA
state-point path. The score route returns unavailable instead of falling back to
a local estimate if the checkpoint is missing.

That makes it useful for questions like:

- as of a historical date, what public risks were visible?
- what next public event or policy choice should we test?
- what happens if secession accelerates, Fort Sumter escalates, emancipation
  policy shifts, a draft crisis spreads, or blockade and battlefield reports
  change public confidence?
- which candidate action creates better predicted future risk, trust, drag,
  commercial, or public-confidence tradeoffs?

### Data Sources

The builder supports two historical public-news sources:

- AmericanStories: article-level extractions from Chronicling America.
- PleIAs US-PD-Newspapers: OCR newspaper pages from Chronicling America.

AmericanStories is usually the cleaner starting point because rows are
article-level. PleIAs is lower-friction and broad, but rows are OCR pages and can
carry headers, tables, page-number noise, and multi-column artifacts.

### Build A Bounded News Snapshot

Example:

```bash
python scripts/build_news_world_model_snapshot.py \
  --dataset americanstories \
  --output-root _vei_out/datasets/news_americanstories_1859_1865 \
  --start-date 1859-01-01 \
  --end-date 1865-12-31 \
  --max-pages-per-day 18 \
  --max-pages-per-source-per-day 3
```

For a PleIAs page-level sample:

```bash
python scripts/build_news_world_model_snapshot.py \
  --dataset pleias \
  --output-root _vei_out/news_world_model/pleias_1935_1939_sample \
  --start-date 1935-01-01 \
  --end-date 1939-12-31 \
  --max-pages-per-day 20 \
  --max-pages-per-source-per-day 2
```

The output is a normal VEI context snapshot, so it can be pooled with company
tenants in the multi-tenant world-model benchmark.

To refresh the checked-in Public History demo from that broader local bundle:

```bash
python scripts/build_public_history_demo_fixture.py \
  --input _vei_out/datasets/news_americanstories_1859_1865 \
  --workspace docs/examples/news-public-history-demo/workspace
```

By default the fixture builder selects 1,150 records stratified by month and
source topic from the broader local bundle. The checked-in demo currently spans
1859 through 1865 and includes markets, policy, secession and war, local civic
life, slavery and emancipation, labor, agriculture and weather, public health
and disasters, crime and courts, and transport infrastructure.

### Decision Point Shape

For news, a decision point does not need to be an existing article. It should be
an as-of state question:

```text
Date: 1861-04-12
State known so far:
- secession and the Lincoln administration visible in the public record
- slavery and emancipation politics visible as national fault lines
- bank, cotton, gold, and war-finance signals visible
- army, navy, Fort Sumter, blockade, and battlefield reports visible
- public-order, draft, labor, railroad, telegraph, and casualty signals visible

Candidate next event/action:
- publish a national risk bulletin
- brief Congress before public release
- hold for cross-source verification
- open a secession and blockade watch
- issue a narrow public-order and relief advisory
```

This is the same strategic state-point interface used for companies. The LLM or
a human proposes decision points and candidate actions from pre-as-of evidence
only; JEPA scores the predicted future vector.

### Run Strategic State Points

```bash
vei whatif benchmark strategic-state-points \
  --input news=_vei_out/datasets/news_americanstories_1859_1865/context_snapshot.json \
  --checkpoint _vei_out/world_model_multitenant_jepa/enron_dispatch_powr_news_fuller_cap512_h12_20260427/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_strategic_state_points \
  --label news_public_world_statepoints \
  --as-of news=1861-04-12 \
  --decisions-per-tenant 3 \
  --candidates-per-decision 8 \
  --proposal-mode llm \
  --proposal-model gpt-5.4
```

Strategic proposal models route through Codex by default. The current default is
`gpt-5.4`; override `--proposal-model` when a newer Codex-supported model is
available. Set `VEI_STRATEGIC_PROPOSAL_BACKEND=api` only when an explicit
direct-provider API run is intended.

### Current Local Result

The latest local four-group run includes Enron, Dispatch, Powr of You, and a
small AmericanStories news sample. The human-facing current output is:

```text
_vei_out/world_model_current/world_model_decision_summary.csv
```

The news rows in that file are a worked application example. They are not
committed source data.

Read the score column as an optional `balanced_operator_score` readout. The
world-model output to inspect first is the predicted future vector, the delta
versus the baseline action, the Pareto frontier membership, and the concrete
success/failure observables. In current exports, `display_rank` is the shareable
order, `operator_score_rank` is only the fixed score order, and `frontier_rank`
identifies Pareto-frontier options.

### Limits

The current model still uses generic business and future-state heads:

- risk
- commercial position
- organizational strain
- stakeholder trust
- execution drag
- regulatory exposure
- liquidity stress
- governance response
- evidence control
- external-confidence pressure

Those heads are workable but imperfect for news. Future news-native heads should
include follow-up coverage, topic persistence, source diversity, public-risk
escalation, market or policy attention, and correction pressure.

Until those heads exist, treat news results as exploratory decision support, not
historical causal proof.
