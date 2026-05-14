# Semantic Workflow Candidate Mining

Workflow mining is meant to surface work worth reviewing, automating, testing,
or turning into an agent environment. It should not rank a Teams greeting above
a release gate just because the greeting happened in a busy thread.

The pipeline is therefore:

```text
canonical events
  -> LLM-derived company skill map with cited evidence
  -> world-model skill opportunities from counterfactual action gaps
  -> semantic workflow candidates
  -> optional world-model alignment
  -> workflow_candidates.json for review/promotion
```

A prior structural miner (keyword scoring over case/thread clusters) used to
ship alongside this path as a diagnostic fallback. It was deleted because it
silently produced keyword-clustered output that called itself "workflows" but
was not what operators or CEOs should review. Mining now requires a company
skill map and fails fast when one is missing.

## Why This Shape

The skill map already performs the expensive semantic step: it reads a company
history, identifies operating capabilities, cites event evidence, drafts
triggers, steps, blocked actions, and replay checks, and assigns usefulness and
confidence. Workflow mining should reuse that semantic substrate instead of
rediscovering workflows from raw keyword clusters.

The workflow miner now promotes only skills that look like real workflows:

- not a gap or retired skill
- usable title, goal, and trigger
- cited evidence
- steps or expected output artifacts
- no obvious one-word/greeting titles

Each promoted workflow carries the original skill id, the cited events, the
draft task spec, replay criteria, approval boundaries, evidence snippets, and a
quality breakdown.

## Ranking

Semantic workflow rank is a weighted quality score over:

- skill usefulness
- skill confidence
- cited evidence coverage
- operational shape: trigger, steps, outputs, allowed and blocked actions
- deployment readiness
- optional alignment with the latest strategic/world-model report

If a `strategic_state_point_results.csv` or JSON report is supplied, the skill
map can first add a cited opportunity layer:

- high-value trusted counterfactual rows become skill-upgrade notes when an
  existing skill already covers the area
- weakly covered rows become `World-model opportunity` gaps only when canonical
  event evidence supports the action area
- uncited or untrusted rows are skipped

The workflow miner then publishes skill-backed workflows plus cited
world-model opportunity candidates. It also still records
`metadata.world_model_alignment` for existing skills. The world model therefore
acts as a search prior over "what capability would change the predicted future,"
while canonical events remain the citation boundary.

## Outputs

Every run writes:

- `workflow_candidates.json` — published semantic candidates
- `workflow_mining_manifest.json` — counts, source paths, and policy

Skill-backed semantic workflows and cited world-model opportunities are the
only candidate sources. Mining fails fast (`FileNotFoundError`) if no company
skill map is available — produce one with `vei knowledge skillmap build` first.

## Daily Command

After refreshing the context bundle and skill map, run:

```bash
vei workflow mine \
  --source-dir _vei_out/<tenant>/context_snapshot.json \
  --output _vei_out/<tenant>/workflows \
  --skill-map _vei_out/<tenant>/skill_map/company_skill_map.json \
  --world-model-report _vei_out/world_model_strategic_state_points/<run>/strategic_state_point_results.csv
```

Use `vei workflow refresh --refresh-skillmap` when the workspace form is
available and the goal is a daily update that preserves existing labels.

## Acceptance Checks

A daily workflow list is good enough to review when:

- top candidates are named operating patterns, not thread subjects
- every candidate has cited event ids
- noisy snippets such as greetings are filtered from the visible evidence list
- credential-like snippets are redacted in workflow evidence surfaces
- mining fails fast when the required company skill map is missing
- world-model alignment is present when a strategic report is supplied
- world-model skill opportunities are cited or skipped
- old human labels survive `vei workflow refresh`

The miner still does not activate anything by itself. Candidates become labeled,
rubric-evaluable, contract-evaluable, and packaged only through the existing
review and promotion ladder.
