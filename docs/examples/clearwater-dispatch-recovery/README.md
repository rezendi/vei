# Clearwater Dispatch Recovery

Synthetic Clearwater what-if bundle built from the repo-owned service-ops story workspace. This example captures the moment when a VIP outage command thread has already triggered dispatch recovery work, and the team must decide whether to stabilize the full service loop or keep responding tactically.

## Open in Studio

```bash
vei ui serve \
  --root docs/examples/clearwater-dispatch-recovery/workspace \
  --host 127.0.0.1 \
  --port 3056
```

Open `http://127.0.0.1:3056`.

## Branch Point

The VIP outage command thread (`tickets:JRA-CFS-10`) branches after dispatch recovery work has started, under the `service_day_collision` scenario variant. The branch fires at event `history_1b8c4c0ac118f774` with 44 prior canonical events across google, jira, mail_archive, and slack source families.

- Scenario variant: `service_day_collision`
- Contract variant: `protect_sla`
- Recorded future events after the branch: 1

## What Actually Happened

The saved reference forecast completed. The top-ranked candidate action is "Stabilize the full service loop" — a move that stays close to the historical business path.

## Saved Files

- `workspace/context_snapshot.json`: saved workspace seed
- `workspace/canonical_events.jsonl`: saved canonical timeline
- `workspace/canonical_event_index.json`: saved searchable timeline index
- `whatif_experiment_overview.md`: saved what-if summary
- `whatif_reference_result.json`: saved learned forecast
- `whatif_business_state_comparison.md`: saved candidate comparison
- `clearwater_story_overview.md`: bundle-local story walkthrough

## Refresh

```bash
python scripts/build_service_ops_example_bundles.py --bundle clearwater-dispatch-recovery
python scripts/validate_whatif_artifacts.py docs/examples/clearwater-dispatch-recovery
```
