# Connector Runbooks

VEI ingests company history from enterprise systems through direct connectors,
managed pipelines, and tenant-level backfill paths. This document covers the
two managed-connector paths: the PipesHub pilot and Microsoft Teams via Graph.

For the basic normalize-and-verify path (raw exports, Gmail Takeout, Notion
export zips, etc.), see the "Bring your own company history" section in
[README.md](../README.md).

---

## PipesHub

VEI can launch a local PipesHub stack and snapshot its synced enterprise
records into the same canonical company-history bundle. PipesHub remains a
separate connector/search service; VEI pulls a point-in-time snapshot and writes
reviewable local artifacts.

```bash
pip install -e ".[pipeshub]"

# Generate a local Compose profile and start PipesHub.
vei connectors pipeshub up

# Open http://127.0.0.1:3000, create the local PipesHub admin account, and
# configure connectors in the PipesHub UI. Personal Google accounts are fine
# for the Drive/Gmail pilot if you provide a Google OAuth desktop/web client
# ID + secret. Google Workspace Drive/Gmail connectors use the workspace
# service-account/domain-delegation path instead.
#
# Then export a PipesHub API token. The inspect/capture commands also accept
# --token-env if you store it elsewhere.
export PIPESHUB_BEARER_AUTH="<pipeshub bearer token>"

# Inspect what VEI can ingest.
vei context pipeshub inspect

# Pull a snapshot from PipesHub into VEI. Use gmail/drive for personal Google
# connectors, or gmailworkspace/driveworkspace for Workspace connectors.
vei context pipeshub capture \
  --workspace _vei_out/yourco \
  --run-id first_march_backfill \
  --org "YourCo" --domain "yourco.example" \
  --connector gmail \
  --connector drive \
  --connector jira \
  --connector confluence \
  --connector salesforce \
  --connector onedrive \
  --connector outlook \
  --since 2026-03-01T00:00:00Z

# If a long capture is interrupted, rerun with the same run id.
vei context pipeshub capture \
  --workspace _vei_out/yourco \
  --run-id first_march_backfill \
  --resume \
  --org "YourCo" --domain "yourco.example" \
  --connector gmail \
  --connector drive \
  --connector jira \
  --connector confluence \
  --connector salesforce \
  --connector onedrive \
  --connector outlook \
  --since 2026-03-01T00:00:00Z

# Smoke the captured bundle through VEI's downstream read models.
vei context verify --snapshot _vei_out/yourco/context_snapshot.json
vei wiki build --source-dir _vei_out/yourco --output _vei_out/yourco/wiki
vei workflow mine --source-dir _vei_out/yourco --output _vei_out/yourco/workflows
vei context readiness --root _vei_out/yourco --format json

# Stop the local PipesHub stack when the pilot sync is finished.
vei connectors pipeshub down
```

### Capture details

The capture writes raw evidence under
`imports/source_syncs/pipeshub/<run_id>/`, including `records.jsonl` and a
page-level `capture_manifest.json` that can resume an interrupted backfill.
It then writes `context_snapshot.json`, `canonical_events.jsonl`, and
`canonical_event_index.json`.

By default VEI captures metadata, snippets, source links, permissions, and
other list/detail fields without streaming full document text; pass
`--include-content` only for a bounded materialization window where you
intentionally want the extra local copy.

### Provider mapping

Records keep their upstream system identity — PipesHub is the transport, not
the origin — so a Gmail message lands under `provider="gmail"`, a Jira ticket
under `provider="jira"`, a Drive file under `provider="google"`, and a managed
Teams bridge or future PipesHub Teams connector would land under
`provider="teams"`.

VEI does not maintain its own provider allowlist at the command boundary;
connector filters are resolved against the configured PipesHub connectors when
possible, then whatever PipesHub serves is ingested under the source system it
came from.

**Known exceptions:**

- **ClickUp** — PipesHub exposes ClickUp agent/tool code in the inspected
  build, but not a mature normalized ingestion connector. Use VEI's direct
  ClickUp provider for now.
- **Teams** — The inspected local PipesHub image does not ship a mature Teams
  sync connector like Outlook or OneDrive. For Teams backfills today, use VEI's
  direct Microsoft Graph capture lane (see below). Teams is VEI-ready when
  PipesHub, a managed bridge, or a future PipesHub release exposes chat/message
  records.

Records whose record type does not have a VEI-normalized shape land in an
`other` bucket on their provider so they remain discoverable downstream.

### Local launcher notes

The local launcher keeps the service boundary explicit. Generated Compose/env
files live under the VEI-managed runtime directory, PipesHub stores synced data
inside its own databases and indexes, and VEI only reads a reviewed snapshot.

For the local pilot, the launcher pins the PipesHub image and builds a small
local image layer to normalize deployment config parsing, keep Google Drive and
Gmail OAuth scopes read-only, and expose connector-owned records through the
PipesHub list/detail APIs. If a managed PipesHub deployment already exists, pass
`--base-url` to `inspect`/`capture` and skip the local launcher.

### Canonical timeline coverage

Canonical timeline events (`canonical_events.jsonl`) cover the original
provider set plus the main enterprise surfaces VEI can normalize today: Outlook
mail, Teams chat when captured through the direct Graph lane or a compatible
managed bridge, OneDrive/SharePoint/Confluence/Box/Dropbox documents, and
ServiceNow tickets. Unknown provider/record-type combinations still remain in
the snapshot under their provider's `other` bucket until VEI learns a typed
event shape for them.

---

## Microsoft Teams

Teams is captured directly from Microsoft Graph rather than through PipesHub for
now. This is the tenant-backfill path for Microsoft 365 customers: VEI gets an
application token, reads Teams channel and chat messages for a bounded time
window, writes raw local evidence, and emits the same `context_snapshot.json`
plus canonical sidecars as the PipesHub lane.

### Entra app setup

Customer-side setup is the main gate. In Microsoft Entra, create an app
registration, add a client secret, and grant admin consent for the read-only
Graph application permissions needed by the scope you want:

- `Team.ReadBasic.All` and `Channel.ReadBasic.All` for team/channel discovery.
- `ChannelMessage.Read.All` for Teams channel messages.
- `User.Read.All` and `Chat.Read.All` for 1:1 and group chat backfill.
- Later, add `OnlineMeetingTranscript.Read.All` only if you want meeting
  transcripts. Do not capture recordings by default; they are large binary
  assets and should be explicitly materialized.

Store the local pilot credentials in `.env`:

```bash
VEI_MSFT_TENANT_ID="<tenant-id>"
VEI_MSFT_CLIENT_ID="<app-client-id>"
VEI_MSFT_CLIENT_SECRET="<client-secret>"
```

### Capture

```bash
vei context teams inspect --format json

vei context teams capture \
  --workspace _vei_out/yourco-teams \
  --run-id teams_20260504_20260511 \
  --org "YourCo" --domain "yourco.example" \
  --since 2026-05-04T00:00:00Z \
  --until 2026-05-11T23:59:59Z \
  --limit 5000 \
  --format json

vei context verify --snapshot _vei_out/yourco-teams/context_snapshot.json
vei wiki build --source-dir _vei_out/yourco-teams --output _vei_out/yourco-teams/wiki
vei workflow mine --source-dir _vei_out/yourco-teams --output _vei_out/yourco-teams/workflows
```

Use `--team` to restrict channel export to specific team ids or display names,
and `--user` to restrict chat scanning to specific users. Re-run with
`--resume --run-id <same-id>` if a long capture is interrupted. The raw audit
bundle lands under
`imports/source_syncs/microsoft_teams/<run_id>/records.jsonl`, with a
`capture_manifest.json` beside it. Microsoft Teams export endpoints reject
Graph `$top`, so `--limit` is enforced locally while VEI follows Graph
pagination.

---

## What to do with the captured bundle

Once you have a `context_snapshot.json` from any path above, the full VEI
surface is available:

```bash
# 1. Rank strong branch points (no LLM, no training)
vei whatif candidates \
  --source company_history \
  --source-dir _vei_out/yourco-teams/context_snapshot.json \
  --limit 10

# 2. Run a counterfactual. --mode e_jepa materializes a bounded training window
#    from the spine and predicts; --mode heuristic_baseline is deterministic
#    and fast.
vei whatif experiment \
  --source company_history \
  --source-dir _vei_out/yourco-teams/context_snapshot.json \
  --label first_experiment \
  --counterfactual-prompt "What if escalation had gone through legal first?" \
  --mode e_jepa --forecast-backend e_jepa

# 3. Build the company wiki (Overview, Recent Changes, Cases, People,
#    Knowledge, Skills, Evidence Index). Citations link back to canonical
#    events; nothing from synthetic vertical packs is mixed in.
vei wiki build --source-dir _vei_out/yourco-teams/context_snapshot.json \
  --output _vei_out/yourco-teams/wiki

# 4. Compile evidence-backed skills (LLM-derived, every step cited)
vei knowledge skillmap build \
  --source-dir _vei_out/yourco-teams/context_snapshot.json \
  --output _vei_out/yourco-teams/skill_map

# 5. When real agent activity has been imported into the workspace, refresh
#    skills + wiki from the context plus the Control evidence spine.
vei knowledge skillmap refresh --workspace _vei_out/yourco-teams \
  --output _vei_out/yourco-teams/skill_map
vei wiki refresh --workspace _vei_out/yourco-teams

# 6. Mine recurring work and promote an evidence-backed task spec.
vei workflow mine \
  --source-dir _vei_out/yourco-teams/context_snapshot.json \
  --output _vei_out/yourco-teams/workflows \
  --skill-map _vei_out/yourco-teams/skill_map/company_skill_map.json
vei workflow promote \
  --root _vei_out/yourco-teams/workflows \
  --candidate-id <candidate-id> \
  --output _vei_out/yourco-teams/workflows/task_spec.json
```

`vei workflow mine` publishes a semantic, skill-backed workflow list. A
company skill map is required — build one with `vei knowledge skillmap build`
first. Add `--world-model-report <strategic_state_point_results.csv>` to
annotate candidates against the latest counterfactual action landscape.

For a checked-in end-to-end example of the workflow-intelligence ladder, see
[docs/examples/workflow-intelligence-walkthrough](examples/workflow-intelligence-walkthrough/).

Full command reference: [docs/WHATIF.md](WHATIF.md).
