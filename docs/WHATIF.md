# Historical What-Ifs

VEI now supports a mail-first historical what-if workflow for archive-backed datasets such as the Enron Rosetta event tables.

The flow has four steps:

1. Explore the whole history to see what a rule or intervention would have touched.
2. Pick one affected thread.
3. Materialize that thread into a strict historical workspace.
4. Compare the baseline future against one or more counterfactual paths.

## Why this shape

VEI does not try to turn an entire historical corpus into one giant always-running simulation. That would be slower, heavier, and harder to understand in a demo.

Instead, the system uses two connected layers:

- **Whole-history analysis** for broad questions such as “what would this policy have caught?”
- **Thread-level replay** for one chosen moment, where VEI can branch, replay, and compare outcomes inside a normal world workspace

This keeps the whole-history pass deterministic and cheap while still giving us a true replay environment for the interesting moment.

## What gets materialized

When VEI opens a historical episode, it builds a mail-first workspace from the selected thread:

- past messages become the initial mail state
- later historical messages become scheduled replay events
- observed thread participants become identity records
- policy-relevant annotations stay attached for analysis and scoring

The important constraint is honesty:

- VEI does **not** invent Slack history for archive-backed email episodes
- VEI keeps historical body excerpts labeled as excerpts when the source data is truncated
- unsupported surfaces stay disabled instead of being faked

## Compare paths

There are two compare paths today:

- **LLM actor continuation**
  - bounded email-only continuation on the affected thread
  - limited to the known thread participants and allowed recipients
  - useful for “what would someone have said or done next?”
- **Forecast adapter**
  - E-JEPA-style proxy forecast for risk and volume deltas
  - useful for “how much would this likely reduce exposure, escalation, or follow-up volume?”
  - this is a forecast adapter today, not a trained checkpoint-backed E-JEPA model

## CLI

```bash
# Whole-history analysis
vei whatif explore \
  --rosetta-dir /path/to/rosetta \
  --scenario compliance_gateway \
  --format markdown

# Build a replayable episode from one thread
vei whatif open-episode \
  --rosetta-dir /path/to/rosetta \
  --root _vei_out/whatif/enron_case \
  --thread-id thr_1234

# Replay the historical future
vei whatif replay \
  --root _vei_out/whatif/enron_case \
  --tick-ms 600000

# Run the full counterfactual experiment
vei whatif experiment \
  --rosetta-dir /path/to/rosetta \
  --artifacts-root _vei_out/whatif_experiments \
  --label early_legal_quarantine \
  --selection-scenario compliance_gateway \
  --counterfactual-prompt "Loop in compliance, pause forwarding, and keep this internal."
```

## Artifacts

`vei whatif experiment` writes a result bundle that includes:

- experiment result JSON
- experiment overview Markdown
- LLM path JSON
- forecast path JSON
- the strict replay workspace used for the run

This makes it easy to inspect the result in Studio later, compare runs, or hand the output to another tool.
