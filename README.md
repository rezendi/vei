## VEI

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Strange-Lab-AI/vei)

VEI turns a company's email, Slack, tickets, docs, CRM, identity, and agent traces into a replayable company environment: one canonical event spine that can be branched, audited, mined for workflows, and packaged into process-training worlds for AI agents.

From [Strange Lab](https://strangelab.ai). The first public release of our enterprise-AI infrastructure stack.

## The bet

Every company that runs agents will need a machine-readable model of how work actually happens. VEI's bet is that the right primitive is a canonical event spine: who did what, when, to what, with provenance.

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

## Try it now

Live demos (no install, no API key):

- [strangelab.ai/enron](https://strangelab.ai/enron) — Enron what-if archive
- [strangelab.ai/public-history](https://strangelab.ai/public-history) — public news timeline

![Readable decision scene for the Enron Master Agreement branch point](docs/assets/enron-whatif/enron-decision-scene-readme.jpg)

## Quick Start

```bash
git clone https://github.com/Strange-Lab-AI/vei.git
cd vei
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

## What VEI gives you

Five surfaces, one spine:

- **Knowledge / Wiki / Skill Map** — materialize a company wiki with citations; compile draft agent skills from evidence.
- **Workflow Intelligence / Train** — mine repeated work, promote reviewed task specs, and package scoped process-training data. See [docs/RL_GYM.md](docs/RL_GYM.md).
- **Sandbox / What-if** — fork a world, change a policy or action, compare alternate paths. See [docs/WHATIF.md](docs/WHATIF.md).
- **Governor / Control** — gate writes, record agent activity, export evidence packs. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
- **Test / Eval** — run agents against fixed company worlds and score them against contracts. See [docs/EVALS.md](docs/EVALS.md).

## Open a saved example

Open the flagship Enron what-if bundle from a fresh clone — no API key needed:

```bash
vei ui serve \
  --root docs/examples/enron-master-agreement-public-context/workspace \
  --host 127.0.0.1 --port 3055
```

Studio exposes the what-if surface as two modes: **Live archive** for full-history exploration and **Saved reference** for the committed branch replay.

See [docs/EXAMPLES.md](docs/EXAMPLES.md) for all saved bundles (Enron, public history, Clearwater).

## Bring your own company history

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

For managed connector pipelines and tenant-level backfills:

- **PipesHub** (Gmail, Drive, Jira, Confluence, Salesforce, OneDrive, Outlook) — `pip install -e ".[pipeshub]"` then `vei connectors pipeshub up`. See [docs/CONNECTORS.md](docs/CONNECTORS.md#pipeshub).
- **Microsoft Teams via Graph** — direct tenant capture using `vei context teams capture`. See [docs/CONNECTORS.md](docs/CONNECTORS.md#microsoft-teams).

## Run a real LLM agent against the twin

```bash
vei eval llm-test run \
  --provider openai --model gpt-5 \
  --task "Triage the open exception and reply to the customer." \
  --artifacts _vei_out/yourco/llm_run

# Or score a model against a named benchmark family and its workflow contract.
VEI_OPENAI_REASONING_EFFORT=low vei eval benchmark \
  --runner llm \
  --family security_containment \
  --provider openai --model gpt-5-mini \
  --max-steps 32 --tool-top-k 48 \
  --artifacts-root _vei_out/benchmark
```

Named-family benchmark runs derive the agent task from the workflow objective, constraints, relevant tools, and known argument anchors. The result includes both the enterprise score and workflow-contract validation.

## CLI Map

All commands live under `vei <group> <command>`:

| Surface | Key commands |
|---|---|
| **Quickstart** | **`vei quickstart run`**, **`vei doctor`**, `vei eval smoke run` |
| **Test / Eval** | **`vei eval benchmark`**, `vei eval demo`, `vei eval showcase`, **`vei eval llm-test run`**, `vei eval agent-demo run`, `vei run start`, `vei admin report` |
| **Governor / Control** | `vei workspace twin serve`, `vei workspace twin onboard`, `vei workspace ingest agent-activity`, `vei provenance access-review`, `vei provenance verify`, `vei provenance export` |
| **Sandbox / What-if** | `vei whatif candidates`, `vei whatif events`, `vei whatif open`, `vei whatif experiment` (`--mode e_jepa` for the trained backend), `vei whatif rank`, `vei whatif pack run` |
| **Train / Data** | `vei rollout procurement`, `vei train bc`, `vei workflow package-env` |
| **Knowledge / Skills / Wiki** | `vei knowledge compose`, `vei knowledge ingest`, `vei knowledge skillmap build`, `vei knowledge skillmap refresh`, `vei wiki build`, `vei wiki refresh`, `vei wiki query` |
| **Workflow intelligence** | `vei workflow mine`, `vei workflow label`, `vei workflow promote`, `vei workflow refresh` |
| **Inspect / Debug** | `vei admin world list`, `vei inspect fidelity`, `vei workspace context timeline`, `vei workspace context readiness`, `vei admin visualize`, **`vei ui serve`** |
| **Project / Workspace** | `vei workspace project init`, `vei workspace project show`, `vei admin blueprint`, `vei admin contract`, `vei admin release` |

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
- [docs/CONNECTORS.md](docs/CONNECTORS.md) — PipesHub pilot and Microsoft Teams Graph capture runbooks
- [CONTRIBUTING.md](CONTRIBUTING.md) — setup, daily loop, module boundaries, PR workflow

## About Strange Lab

Strange Lab builds infrastructure for enterprise AI agents — testing, governance, replay, and training over real company history. VEI is the first public piece of that stack.

- GitHub: [github.com/Strange-Lab-AI](https://github.com/Strange-Lab-AI)
- Web: [strangelab.ai](https://strangelab.ai)
- Questions or bugs: open an issue on this repo.

## License

Business Source License 1.1. See [LICENSE](LICENSE). BSL today; converts to GPL-2.0-or-later on 2030-03-10.
