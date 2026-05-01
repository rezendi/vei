# Clearwater Technician No-Show

Synthetic Clearwater what-if bundle built from the repo-owned service-ops story workspace. This example shows the moment when a technician no-show becomes the main customer-trust risk and the team has to decide whether to own the failure fast or let the backup dispatch routing play out.

## Open in Studio

```bash
vei ui serve \
  --root docs/examples/clearwater-technician-no-show/workspace \
  --host 127.0.0.1 \
  --port 3056
```

Open `http://127.0.0.1:3056`.

## Branch Point

The backup dispatch routing thread (`tickets:JRA-CFS-11`) branches after the technician no-show becomes the main customer-trust risk. The branch fires at event `history_dd90fc6e8d1fa5b8` with 31 prior canonical events across google, jira, mail_archive, and slack source families.

- Scenario variant: `technician_no_show`
- Contract variant: `protect_customer_trust`
- Recorded future events after the branch: 1

## What Actually Happened

The saved reference forecast completed. The top-ranked candidate action is "Own the no-show fast" — a move that stays close to the historical business path.

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
python scripts/build_service_ops_example_bundles.py --bundle clearwater-technician-no-show
python scripts/validate_whatif_artifacts.py docs/examples/clearwater-technician-no-show
```
