# Current Macro Public History Demo

This workspace is a current public-history variant built as of 2026-05-14. It uses U.S. macro public sources: FRED time-series observations, U.S. Treasury daily yield-curve rows, BEA current releases, and the latest Federal Reserve FOMC statement visible at build time. The UI can choose a cutoff, show only pre-cutoff evidence, and score candidate public-policy or market-response actions with the shared public-history JEPA checkpoint.

The checked-in fixture is intentionally official-source first. The builder can optionally query GDELT headline metadata with `--include-gdelt-headlines`, but that endpoint is not required for the shipped example because it can rate-limit.

## Open in Studio

```bash
vei ui serve \
  --root docs/examples/current-public-history-macro/workspace \
  --host 127.0.0.1 \
  --port 3058
```

Open `http://127.0.0.1:3058`.

## Run a State-Point Counterfactual

```bash
vei whatif benchmark news-state-point \
  --input current_macro=docs/examples/current-public-history-macro/workspace/context_snapshot.json \
  --checkpoint docs/examples/news-public-history-demo/workspace/jepa_model.pt \
  --artifacts-root _vei_out/current_public_history_macro/state_points \
  --label current_macro_20260514 \
  --topic all_public_record \
  --as-of 2026-05-14 \
  --candidate "customer_status_note::Publish inflation-risk bulletin::Publish a dated public bulletin that separates inflation, energy, labor-market, and Treasury-yield signals visible by 2026-05-14." \
  --candidate "decision_log_evidence::Prepare Fed/Treasury evidence memo::Prepare a dated evidence memo for Federal Reserve and Treasury watchers that separates price, jobs, GDP, imports, exports, and yield-curve evidence." \
  --candidate "narrow_pilot::Open household-stress watch::Open a narrow household-stress watch over unemployment, payrolls, prices, mortgage rates, and retail-sales signals before recommending a broad policy move." \
  --candidate "hold_compliance_review::Hold for next release::Hold any public recommendation until the next official release confirms whether the inflation and labor-market signals persist."
```

## Refresh

```bash
python scripts/build_current_macro_public_history_fixture.py \
  --workspace docs/examples/current-public-history-macro/workspace

python scripts/export_public_history_static_assets.py \
  --workspace docs/examples/current-public-history-macro/workspace \
  --output /path/to/strangelab.ai/public/public-history-current
```

The fixture records exact source fetches in `workspace/source_manifest.json`.
