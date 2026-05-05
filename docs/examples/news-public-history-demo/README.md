# News Public History Demo

This workspace is the public-history Studio demo. It uses an expanded AmericanStories-derived historical news snapshot across markets, government policy, war and foreign affairs, local civic life, slavery and abolition, labor, agriculture and weather, public health and disasters, crime and courts, and transport infrastructure. The UI can choose a historical cutoff, show only pre-cutoff evidence, and score candidate public actions with a bundled JEPA checkpoint. It does not use live LLM keys and it does not fabricate rankings when the checkpoint is unavailable.

## Open in Studio

```bash
vei ui serve \
  --root docs/examples/news-public-history-demo/workspace \
  --host 127.0.0.1 \
  --port 3057
```

Open `http://127.0.0.1:3057`.

## What You'll See

The UI lets you pick a historical cutoff date within the shipped public-history window, restricts visible evidence to pre-cutoff records, and scores candidate public actions using the bundled JEPA checkpoint. The live JEPA ranking is exploratory decision support — it is not causal proof, and unsupported scenarios should be treated as weakly grounded until broader retrieval and confidence checks are added.

## Saved Files

- `workspace/`: the full public-history workspace seed
- `workspace/public_demo_manifest.json`: exact shipped record count, date range, launch command, and refresh path

## Refresh

```bash
python scripts/build_news_world_model_snapshot.py \
  --dataset americanstories \
  --output-root _vei_out/datasets/news_americanstories_1859_1865 \
  --start-date 1859-01-01 \
  --end-date 1865-12-31 \
  --max-pages-per-day 18 \
  --max-pages-per-source-per-day 3

python scripts/build_public_history_demo_fixture.py \
  --input _vei_out/datasets/news_americanstories_1859_1865 \
  --workspace docs/examples/news-public-history-demo/workspace

python scripts/export_public_history_static_assets.py \
  --workspace docs/examples/news-public-history-demo/workspace \
  --output /path/to/strangelab.ai/public/public-history
```
