# Workflow Intelligence Walkthrough

This walkthrough shows the supported order of operations for workflow
intelligence:

```text
company-history bundle
-> mined workflow candidate
-> human label
-> promoted Business Task Spec
-> reviewed contract/RL-ready spec
-> contract or RL environment package
```

The fixture uses the checked-in Clearwater service-ops bundle:

```text
docs/examples/clearwater-technician-no-show/workspace/context_snapshot.json
```

That bundle is small enough for a repo-owned smoke path and still contains a
real canonical event history across Slack, docs, tickets, billing, exceptions,
and dispatch records.

## 1. Mine Candidates

```bash
export WORKDIR="$(mktemp -d)"

vei workflow mine \
  --source-dir docs/examples/clearwater-technician-no-show/workspace/context_snapshot.json \
  --output "$WORKDIR/workflows" \
  --limit 3
```

Expected boundary:

- the output is `workflow_candidates.json`
- candidates are descriptive evidence summaries
- draft specs have `evaluation_level=descriptive`
- no deterministic workflow, contract, or RL reward has been created

In the current fixture, the top candidate is:

```text
wfc_140309889b0c4241  Morning Dispatch Board
```

## 2. Label the Candidate

```bash
CANDIDATE_ID="wfc_140309889b0c4241"
EVENT_ID="$(python - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["WORKDIR"]) / "workflows" / "workflow_candidates.json"
candidate = json.loads(root.read_text())["candidates"][0]
print(candidate["source_event_ids"][0])
PY
)"

vei workflow label \
  --root "$WORKDIR/workflows" \
  --candidate-id "$CANDIDATE_ID" \
  --label good_example \
  --note "walkthrough evidence path" \
  --event-id "$EVENT_ID"
```

Expected boundary:

- the label is file-backed human review context
- it does not prove the task is deterministic
- it does not create success/failure predicates

## 3. Promote to a Business Task Spec

```bash
vei workflow promote \
  --root "$WORKDIR/workflows" \
  --candidate-id "$CANDIDATE_ID" \
  --output "$WORKDIR/task_spec.json"
```

Expected boundary:

- the promoted spec is a Business Task Spec
- it is still declarative, not an ordered script
- after the label above, it has `evaluation_level=labeled`
- it is not contract-evaluable or RL-ready yet

That boundary is intentional. A Business Task Spec can be useful for review,
handoff, and rubric design before it is safe to turn into a deterministic
contract.

## 4. Package Gate Must Reject the Labeled Spec

```bash
vei workflow package-env \
  --spec "$WORKDIR/task_spec.json" \
  --source-dir docs/examples/clearwater-technician-no-show/workspace/context_snapshot.json \
  --output "$WORKDIR/rejected_env"
```

Expected result: this fails with `requires evaluation_level=rl_packaged`.

That failure is part of the contract. VEI should not quietly turn a mined or
labeled pattern into a reward surface.

## 5. Package Only a Reviewed RL-Ready Spec

This directory includes a small reviewed fixture:

```text
docs/examples/workflow-intelligence-walkthrough/reviewed_service_ops_task_spec.json
```

It uses the same mined Clearwater source event IDs, but adds a reviewed
deterministic `service_ops` contract and sets:

```text
status=reviewed
evaluation_level=rl_packaged
```

Package it:

```bash
vei workflow package-env \
  --spec docs/examples/workflow-intelligence-walkthrough/reviewed_service_ops_task_spec.json \
  --source-dir docs/examples/clearwater-technician-no-show/workspace/context_snapshot.json \
  --output "$WORKDIR/env"
```

Expected package files:

```text
action_schema.json
contract.json
environment_manifest.json
example_traces.jsonl
observation_schema.json
reset_cases.jsonl
reward_spec.json
splits.json
task_spec.json
README.md
```

Final claim boundary:

- rewards are deterministic process/compliance predicates
- the package does not claim to optimize real-world business outcomes
- train/validation/test splits are stable by case/thread hash

## Private Data Boundary

Dispatch and Powr of You bundles can be used as private local inputs under
`_vei_out/datasets/`. They are useful for local validation, and the test suite
has optional skipped tests for them. They are not used as this repo-owned
walkthrough fixture because raw exports and full normalized bundles are private,
large, and intentionally ignored by git.

No external Dispatch, Powr of You, or process-mining library is required for
this path. The walkthrough exercises VEI's current `vei workflow` commands.
