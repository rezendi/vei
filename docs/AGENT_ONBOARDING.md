# Onboarding (Humans and Agents)

This is the one starting document for anyone — human or AI agent — working on VEI. It explains what VEI is, how to run it, and how to navigate the codebase.

**If you have 10 minutes, do these five things:**

1. `make setup-full` — creates `.venv` with all extras
2. `vei quickstart run` — launches Studio + Twin Gateway with a live synthetic workspace
3. `vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --port 3055` — opens the flagship Enron what-if bundle
4. `vei eval benchmark --runner workflow --family security_containment` — runs the deterministic eval baseline
5. Read the [Glossary](GLOSSARY.md) for every term of art

## What VEI Is

VEI builds a runnable replica of any organisation or situation from real records. The same event spine supports the CLI, Studio UI, twin gateway, branch-point what-if experiments, company skill-map compilation, and learned world-model benchmarks.

The practical job is usually one of five things:

- run a deterministic enterprise scenario and score an agent
- turn real historical records into a canonical timeline
- ingest agent-activity evidence and review VEI Control reports
- compile company-specific draft skills from the normalized company bundle
- train or apply the JEPA-style world model to forecast future state and rank counterfactual actions

Do not treat VEI as a magic CEO oracle. Treat it as an offline benchmark and decision-support workflow. Factual prediction is the strongest evidence. Counterfactual rankings are hypotheses for human or expert review.

## Start Here

Read these next when needed:

- `README.md` for setup and operator flows
- `docs/ARCHITECTURE.md` for the module map, five surfaces, and what is and isn't learned
- `docs/GLOSSARY.md` for every term of art defined in one place
- `docs/EXAMPLES.md` for worked examples (Enron, public history, Clearwater)
- `docs/WHATIF.md` for the world-model and what-if command reference
- `docs/EVALS.md` for the three-layer evaluation framework
- `docs/RL_GYM.md` for the scoped contract-only RL-training plan
- `AGENTS.md` for repo policy, eval runners, and validation expectations
- `Makefile` and `.agents.yml` before changing gates or CI behavior

Use the repo command surface:

```bash
make check        # format, lint, types, import boundaries, security
make test         # fast tests (skips slow)
make check-full   # + bandit, semgrep, detect-secrets
make test-full    # full suite with coverage
```

## Repo Map

- `vei/world/` holds the deterministic world-session kernel and replayable state.
- `vei/router/` exposes the MCP tool surface over the kernel.
- `vei/structure/` builds the event-derived read model and truth-comparison helpers.
- `vei/twin/` exposes the HTTP governed twin surface.
- `vei/workspace/` and `vei/run/` hold the file-backed workspace and run model.
- `vei/skillmap/` compiles evidence-backed, replay-checked company skills from normalized bundles.
- `vei/ingest/agent_activity/` captures external agent behavior from JSONL landing zones, MCP transcripts, and OpenAI org usage/audit evidence.
- `vei/provenance/` builds Control reports: timeline, activity graph, access review, blast radius, policy replay, and OTel export.
- `vei/whatif/` handles branch-point replay, counterfactuals, benchmarks, and saved example bundles.
- `vei/verticals/` holds seeded business packs and overlays.
- `tests/` covers the repo. `tests/dynamics/` also emits `_vei_out/dynamics_eval/metrics.json`.

Keep cross-module calls behind typed module APIs, usually `some_module/api.py`. Keep local secrets in `.env`. Do not commit `_vei_out/`, `.artifacts/`, private exports, generated prompts, raw archives, or ad hoc traces.

## Basic Use

Install the full development extras:

```bash
make setup-full
```

`make setup` is a lighter alternative that skips worldmodel, jepa, test, rl, and browser extras.
For a focused install of the optional JEPA backend:

```bash
pip install -e ".[jepa]"
```

Useful entrypoints:

```bash
vei quickstart run
vei eval benchmark --runner workflow --family security_containment
vei skillmap build --source-dir _vei_out/<tenant>/context_snapshot.json --output _vei_out/<tenant>/skill_map
vei ingest agent-activity --source agent_activity_jsonl --path ./logs --workspace _vei_out/<tenant>
vei skillmap refresh --workspace _vei_out/<tenant> --output _vei_out/<tenant>/skill_map
vei provenance access-review --agent-id <agent-id> --workspace _vei_out/<tenant>
vei provenance verify --workspace _vei_out/<tenant>
vei provenance export --format evidence-pack --workspace _vei_out/<tenant> --output _vei_out/<tenant>/evidence_pack.json
vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace --host 127.0.0.1 --port 3055
```

Eval runners:

- `workflow` is the reference runner.
- `scripted` is the deterministic floor baseline.
- `bc` is the tool-frequency baseline powered by `FrequencyPolicy`.
- `llm` runs a real model through the MCP world.

Same seed means same world. Determinism is part of the product.

## Canonical Inputs

For each tenant, prefer a VEI bundle with:

- `context_snapshot.json`
- `canonical_events.jsonl`
- `canonical_event_index.json`

Source records may be email, ClickUp, Notion, docs, tickets, CRM, meeting notes, news articles, or other time-bearing records. The key requirement is that they can be canonicalized into dated events with enough thread or case structure to build pre-branch state and future tails.

Check readiness before training:

```bash
python scripts/check_tenant_world_model.py --root _vei_out/<tenant>/context_snapshot.json
```

## Advanced: World Model

> This section covers the JEPA-style learned world model, strategic state points, leakage rules, and counterfactual workflows. Skip it if you're just getting started with the repo.
>
> For the canonical breakdown of what is learned vs. heuristic vs. external, see [ARCHITECTURE.md](ARCHITECTURE.md) § What Is and Isn't Learned.

### Mental model

The world-model loop is:

```text
raw work or news data
-> canonical timestamped events
-> archive-derived doctrine packet
-> temporal train / validation / test split
-> JEPA learns: doctrine text + current state + action -> future heads
-> decision points selected from pre-branch signals
-> candidate actions generated from pre-branch context only
-> JEPA scores the candidate futures
-> CSV / Markdown results reviewed by humans
```

The model predicts a bundle of future heads, not one universal truth number. The single score in decision tables is a convenience score built from those predicted heads for a balanced decision objective.

### Pooled training

Build one pooled benchmark from all timestamp-ready tenants. Hold out the final tail of each tenant timeline for testing.

```bash
vei whatif benchmark build-multitenant \
  --input enron=_vei_out/enron/context_snapshot.json \
  --input dispatch=_vei_out/dispatch/context_snapshot.json \
  --artifacts-root _vei_out/world_model_multitenant_jepa \
  --label enron_dispatch \
  --candidate-mode template

vei whatif benchmark train \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch \
  --model-id jepa_latent \
  --train-split train --train-split validation \
  --validation-split test

vei whatif benchmark eval \
  --root _vei_out/world_model_multitenant_jepa/enron_dispatch \
  --model-id jepa_latent
```

Report factual metrics first: AUROC when defined, Brier score, calibration error, business-head MAE, and objective-head MAE.

### Decision points and counterfactuals

Use strategic state-point runs when the user wants concrete choices a manager, editor, or analyst could have made. This is the only user-facing counterfactual interface.

```bash
vei whatif benchmark strategic-state-points \
  --input dispatch=_vei_out/datasets/dispatch_real/context_snapshot.json \
  --checkpoint _vei_out/world_model_multitenant_jepa/current/model_runs/jepa_latent/model.pt \
  --artifacts-root _vei_out/world_model_strategic_state_points \
  --label current_strategic_state_points \
  --decisions-per-tenant 3 \
  --candidates-per-decision 8 \
  --proposal-mode llm \
  --proposal-model gpt-5.4
```

See [WHATIF.md](WHATIF.md) § Strategic state-point counterfactual runs for the full reference.

### Leakage rules

Never let the candidate generator or judge see the recorded future tail.

Required checks:

- no held-out branch event IDs leak into fit rows
- train, validation, and test splits are temporal by tenant
- candidate-generation prompts contain only pre-branch context
- generated candidates contain no future-tail event markers
- judge dossiers contain no future-tail event markers or model scores
- generated candidates record prompt hash, evidence hash, model, source, and `no_future_context=true`

If any leakage check fails, do not make model-performance claims from that run.

### Artifact names

Supported what-if forecast filenames:

- `whatif_ejepa_result.json`
- `whatif_reference_result.json`
- `whatif_heuristic_baseline_result.json`

Use `whatif_reference_result.json` as the main saved forecast artifact for repo-owned Enron bundles. Import constants from `vei.whatif.filenames` or `vei.whatif.api`.

## Cleanup

After large runs, inspect before deleting:

```bash
du -sh _vei_out
make clean-workspace-dry-run
make clean-workspace-hard-dry-run
git status --short --branch
```

Use `make clean-workspace` for low-risk cache/build cleanup. It leaves `_vei_out` runs alone. Use `make clean-workspace-hard` only when the user approves a cutover that discards old generated VEI runs; it preserves `_vei_out/world_model_current/`, `_vei_out/datasets/`, and `_vei_out/llm_live/latest/`.
