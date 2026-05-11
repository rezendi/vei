## VEI

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Strange-Lab-AI/vei)

**VEI is infrastructure for enterprise AI agents.** It turns a company's real operational history — email, Slack, tickets, docs, CRM, GitHub, identity, observability, and agent traces — into one canonical event spine, then exposes that spine as replayable enterprise environments where agents can be tested, governed, audited, and improved before they touch production systems.

Try it now: [strangelab.ai/enron](https://strangelab.ai/enron) · [strangelab.ai/public-history](https://strangelab.ai/public-history)

VEI sits above the systems of record and any existing data platform. It preserves
reviewable evidence or source pointers, builds a canonical event spine of how
the company works, and materializes bounded packages for governance, evals,
what-if analysis, and model training.

That spine powers one deterministic kernel and five infrastructure surfaces:

1. **Test / Eval** — run agents against fixed company worlds before production.
2. **Governor / Control** — gate writes, record agent activity, and export evidence packs.
3. **Sandbox / What-if** — replay historical branch points and compare alternate actions as decision support.
4. **Train / Data** — turn traces and reviewed workflow specs into bounded process-training packages.
5. **Knowledge / Wiki / Skill Map** — compile company memory, recurring workflows, and draft agent skills from evidence.

```mermaid
flowchart TB
    subgraph SOURCES["1. Enterprise sources of record"]
        SYS["Email, Slack/Teams, docs, tickets,<br/>CRM, GitHub, identity, observability,<br/>product systems"]
        ACTORS["People, agents, and activity traces"]
    end

    subgraph DATA["2. Existing data platform, if present"]
        WAREHOUSE["Warehouse / lakehouse / archive<br/>Snowflake, BigQuery, Databricks,<br/>Redshift, S3, ADLS, GCS, eDiscovery"]
    end

    SYNC["3. Governed sync<br/>PipesHub, direct connectors,<br/>warehouse adapters, exports"]
    EVIDENCE["4. Reviewable evidence layer<br/>raw snapshots or source pointers<br/>hashes, manifests, permissions, retention"]
    SPINE["5. VEI canonical event spine<br/>who did what, when, to what<br/>with provenance, links, and case threads"]
    SURFACES["6. VEI infrastructure surfaces<br/>wiki, skill map, workflow mining<br/>control, audit, policy replay<br/>what-if, evals, bounded training packages"]
    MODELS["7. Models and agents<br/>JEPA/world model, frontier agents,<br/>small models, deterministic tools"]

    SOURCES --> SYNC
    DATA --> SYNC
    SYNC --> EVIDENCE
    EVIDENCE --> SPINE
    SPINE --> SURFACES
    SURFACES --> MODELS
    MODELS --> ACTIVITY["New actions and decisions"]
    ACTIVITY --> SYNC
    SPINE -.->|optional export| WAREHOUSE
```

## Product Surfaces

**Test / Eval** — Run fixed company worlds and score agents against contracts. Compare scripted, workflow, behavioral-cloning, and live LLM runners on the same scenario. See [docs/EVALS.md](docs/EVALS.md).

**Governor / Control** — Ingest agent activity from JSONL, MCP transcripts, or OpenAI org exports. Review access, blast radius, and policy compliance; gate writes and export evidence packs. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) § VEI Control.

**Sandbox / What-if** — Fork a world, change a policy or action, and compare alternate paths as decision support. Branch from real historical decisions in the Enron archive, from your own company data, or from public news timelines. See [docs/WHATIF.md](docs/WHATIF.md).

**Train / Data** — Turn traces into rollouts, demonstrations, and scoped training data. Mine repeated work from canonical company history, promote evidence-backed Business Task Specs, and package only reviewed contract-ready specs into process environments. See [docs/RL_GYM.md](docs/RL_GYM.md).

**Knowledge / Wiki / Skill Map** — Hydrate notes, transcripts, metrics, SOPs, and pricing into a knowledge graph; materialize a company wiki with citations; compile company-specific draft skills from the normalized bundle, with replay checks and evidence backing.

## Quick Start

```bash
git clone https://github.com/Strange-Lab-AI/vei.git
cd vei                # or your checkout folder name
make setup-full       # creates .venv, installs all extras
vei doctor            # checks environment
vei quickstart run    # launches Studio + Twin Gateway
```

`vei quickstart run` gives you:

- Studio at `http://127.0.0.1:3011`
- Twin Gateway at `http://127.0.0.1:3012`
- a seeded workspace with visible activity already in motion
- connection details in `.vei/quickstart.json`

**Requirements:** Python 3.11, ports 3011 and 3012 free. Local interactive LLM generation defaults to the Codex CLI (`gpt-5.3-codex-spark`). `OPENAI_API_KEY` in `.env` is only needed for explicit direct-provider runs or CI-style `llm-live`.

For the optional JEPA backend by itself:

```bash
pip install -e ".[jepa]"
```

`make setup-full` already includes this extra for full local development.

### Try a saved example

Open the flagship Enron what-if bundle from a fresh clone — no API key needed:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 --port 3055
```

Studio exposes the what-if surface as two explicit modes when both are available: **Live archive** for full-history exploration and **Saved reference** for the committed branch replay. Live archive remains the default and warms its loaded world in the UI server after status loads, so whole-history exploration stays visible without hiding behind the saved reference path.

![Readable decision scene for the Enron Master Agreement branch point](docs/assets/enron-whatif/enron-decision-scene-readme.jpg)

See [docs/EXAMPLES.md](docs/EXAMPLES.md) for all saved bundles (Enron, public history, Clearwater).

## CLI Map

All commands live under `vei <group> <command>`. The top-level surface is
grouped by product workflow:

| Surface | Key commands |
|---|---|
| **Quickstart** | `vei admin quickstart run`, `vei admin doctor`, `vei eval smoke run` |
| **Test / Eval** | `vei eval benchmark`, `vei eval demo`, `vei eval showcase`, `vei eval llm-test run`, `vei run start`, `vei admin report` |
| **Governor / Control** | `vei workspace twin serve`, `vei workspace twin onboard`, `vei workspace ingest agent-activity`, `vei provenance access-review`, `vei provenance verify`, `vei provenance export` |
| **Sandbox / What-if** | `vei whatif candidates`, `vei whatif events`, `vei whatif open`, `vei whatif experiment` (`--mode e_jepa` for the trained backend), `vei whatif rank`, `vei whatif pack run` |
| **Train / Data** | `vei rollout procurement`, `vei train bc`, `vei workflow package-env` |
| **Knowledge / Skills / Wiki** | `vei knowledge compose`, `vei knowledge ingest`, `vei knowledge skillmap build`, `vei knowledge skillmap refresh`, `vei wiki build`, `vei wiki refresh`, `vei wiki query` |
| **Workflow intelligence** | `vei workflow mine`, `vei workflow label`, `vei workflow promote`, `vei workflow refresh` |
| **Inspect / Debug** | `vei admin world list`, `vei inspect fidelity`, `vei workspace context timeline`, `vei workspace context readiness`, `vei admin visualize`, `vei ui serve` |
| **Project / Workspace** | `vei workspace project init`, `vei workspace project show`, `vei admin blueprint`, `vei admin contract`, `vei admin release` |
| **Static-site exports** | `python scripts/export_enron_static_assets.py`, `python scripts/export_public_history_static_assets.py` (powers `strangelab.ai/enron` and `strangelab.ai/public-history`) |

## Bring Your Own Company History

```bash
# Normalize raw exports into a verified bundle
vei context normalize \
  --source-dir <raw_input_path> \
  --org "YourCo" --domain "yourco.example" \
  --output _vei_out/yourco/context_snapshot.json

vei context verify --snapshot _vei_out/yourco/context_snapshot.json

# Or: onboard from live sources (GitHub, ClickUp, Gmail, Notion, Teams, etc.)
vei workspace twin onboard \
  --root _vei_out/yourco/twin \
  --org "YourCo" --domain "yourco.example" \
  --provider gmail --provider notion \
  --base-url gmail=/path/to/gmail-takeout.zip \
  --base-url notion=/path/to/notion-export.zip
```

### Experimental PipesHub Connector Pilot

VEI can also launch a local PipesHub stack and snapshot its synced enterprise
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
  --connector teams \
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
  --connector teams \
  --since 2026-03-01T00:00:00Z

# Smoke the captured bundle through VEI's downstream read models.
vei context verify --snapshot _vei_out/yourco/context_snapshot.json
vei wiki build --source-dir _vei_out/yourco --output _vei_out/yourco/wiki
vei workflow mine --source-dir _vei_out/yourco --output _vei_out/yourco/workflows
vei context readiness --root _vei_out/yourco --format json

# Stop the local PipesHub stack when the pilot sync is finished.
vei connectors pipeshub down
```

The capture writes raw evidence under
`imports/source_syncs/pipeshub/<run_id>/`, including `records.jsonl` and a
page-level `capture_manifest.json` that can resume an interrupted backfill.
It then writes `context_snapshot.json`, `canonical_events.jsonl`, and
`canonical_event_index.json`. By default VEI captures metadata, snippets, source
links, permissions, and other list/detail fields without streaming full document
text; pass `--include-content` only for a bounded materialization window where
you intentionally want the extra local copy. Records keep their upstream system
identity — PipesHub is the transport, not the origin — so a Gmail message lands
under `provider="gmail"`, a Jira ticket under `provider="jira"`, a Drive file
under `provider="google"`, a Teams chat message under `provider="teams"`, and so
on. VEI does not maintain its own provider allowlist at the command boundary;
connector filters are resolved against the configured PipesHub connectors when
possible, then whatever PipesHub serves is ingested under the source system it
came from. ClickUp is the current known exception: PipesHub exposes ClickUp
agent/tool code in the inspected build, but not a mature normalized ingestion
connector, so use VEI's direct ClickUp provider for now. Teams is VEI-ready when
PipesHub, a managed bridge, or a future PipesHub release exposes chat/message
records, but the inspected local PipesHub image does not ship a mature Teams
sync connector like Outlook or OneDrive. For Teams backfills today, use VEI's
direct Microsoft Graph capture lane below. Records whose record type does not
have a VEI-normalized shape land in an `other` bucket on their provider so they
remain discoverable downstream.

The local launcher keeps the service boundary explicit. Generated Compose/env
files live under the VEI-managed runtime directory, PipesHub stores synced data
inside its own databases and indexes, and VEI only reads a reviewed snapshot.
For the local pilot, the launcher pins the PipesHub image and builds a small
local image layer to normalize deployment config parsing, keep Google Drive and
Gmail OAuth scopes read-only, and expose connector-owned records through the
PipesHub list/detail APIs. If a managed PipesHub deployment already exists, pass
`--base-url` to `inspect`/`capture` and skip the local launcher.

Canonical timeline events (`canonical_events.jsonl`) cover the original
provider set plus the main PipesHub-backed enterprise surfaces: Outlook mail,
Teams chat, OneDrive/SharePoint/Confluence/Box/Dropbox documents, and
ServiceNow tickets. Unknown provider/record-type combinations still remain in
the snapshot under
their provider's `other` bucket until VEI learns a typed event shape for them.

### Microsoft Teams Graph Capture

Teams is captured directly from Microsoft Graph rather than through PipesHub for
now. This is the tenant-backfill path for Microsoft 365 customers: VEI gets an
application token, reads Teams channel and chat messages for a bounded time
window, writes raw local evidence, and emits the same `context_snapshot.json`
plus canonical sidecars as the PipesHub lane.

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

Then run a bounded capture:

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
`capture_manifest.json` beside it.

Then explore branch points, run what-if experiments, build a wiki, or compile
a skill map — all from the same canonical event spine:

```bash
# 1. Rank strong branch points (no LLM, no training)
vei whatif candidates --source-dir _vei_out/yourco/context_snapshot.json --limit 10

# 2. Run a counterfactual. --mode e_jepa trains a structured-state JEPA on the
#    spine and predicts; --mode heuristic_baseline is deterministic and fast.
vei whatif experiment \
  --source-dir _vei_out/yourco/context_snapshot.json \
  --label first_experiment \
  --counterfactual-prompt "What if escalation had gone through legal first?" \
  --mode e_jepa --forecast-backend e_jepa

# 3. Build the company wiki (Overview, Recent Changes, Cases, People,
#    Knowledge, Skills, Evidence Index). Citations link back to canonical
#    events; nothing from synthetic vertical packs is mixed in.
vei wiki build --source-dir _vei_out/yourco/context_snapshot.json \
  --output _vei_out/yourco/wiki

# 4. Compile evidence-backed skills (LLM-derived, every step cited)
vei knowledge skillmap build \
  --source-dir _vei_out/yourco/context_snapshot.json \
  --output _vei_out/yourco/skill_map

# 5. When real agent activity has been imported into the workspace, refresh
#    skills + wiki from the context plus the Control evidence spine.
vei knowledge skillmap refresh --workspace _vei_out/yourco \
  --output _vei_out/yourco/skill_map
vei wiki refresh --workspace _vei_out/yourco

# 6. Mine recurring work and promote an evidence-backed task spec.
vei workflow mine \
  --source-dir _vei_out/yourco/context_snapshot.json \
  --output _vei_out/yourco/workflows
vei workflow promote \
  --root _vei_out/yourco/workflows \
  --candidate-id <candidate-id> \
  --output _vei_out/yourco/workflows/task_spec.json
```

For a checked-in end-to-end example of the workflow-intelligence ladder, see
[docs/examples/workflow-intelligence-walkthrough](docs/examples/workflow-intelligence-walkthrough/).

Hardening smokes for public-facing paths:

```bash
make codex-live-smoke      # Codex-backed planning, skillmap, and what-if smoke
make worldmodel-smoke      # JEPA/reference world-model contracts
make public-demo-smoke     # strangelab.ai/enron and /public-history assets
make workflow-intel-smoke  # workflow mining/spec/package gates
```

Run a real LLM agent against the resulting twin via MCP stdio:

```bash
vei eval llm-test run \
  --provider openai --model gpt-5 \
  --task "Triage the open exception and reply to the customer." \
  --artifacts _vei_out/yourco/llm_run
```

Full command reference: [docs/WHATIF.md](docs/WHATIF.md).

## Repo Checks

```bash
make check          # format, lint, types, import boundaries, security
make test           # fast tests (skips slow)
make check-full     # + bandit, semgrep, detect-secrets
make test-full      # full suite with coverage
make llm-live       # needs live API keys
make clean-workspace  # clears caches; leaves _vei_out/ runs alone
```

Exit codes: `0` pass · `1` test/gate failure · `2` cost ceiling exceeded · `3` infrastructure failure · `4` threshold/config missing.

## Where to Go Next

- [docs/AGENT_ONBOARDING.md](docs/AGENT_ONBOARDING.md) — fast repo briefing and 10-minute checklist for humans and agents
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module map, five infrastructure surfaces, runtime shape, what is and isn't learned
- [docs/GLOSSARY.md](docs/GLOSSARY.md) — every term of art used in this repo, defined in one place
- [docs/WHATIF.md](docs/WHATIF.md) — world-model and what-if command reference
- [docs/EXAMPLES.md](docs/EXAMPLES.md) — Enron, public history, and Clearwater worked examples
- [docs/EVALS.md](docs/EVALS.md) — evaluation layers: factual metrics, LLM judge, human audit
- [docs/RL_GYM.md](docs/RL_GYM.md) — scoped RL-training plan over deterministic process contracts
- [CONTRIBUTING.md](CONTRIBUTING.md) — setup, daily loop, module boundaries, PR workflow

## License

Business Source License 1.1. See [LICENSE](LICENSE). Change date: `2030-03-10`. Change license: `GPL-2.0-or-later`.
