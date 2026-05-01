# Clearwater Billing Dispute Reopened

Synthetic Clearwater what-if bundle built from the repo-owned service-ops story workspace. This example explores what happens when a billing dispute thread pulls finance back into an active service response, forcing the team to decide whether to contain the revenue risk early or let the threads run in parallel.

## Open in Studio

```bash
vei ui serve \
  --root docs/examples/clearwater-billing-dispute-reopened/workspace \
  --host 127.0.0.1 \
  --port 3056
```

Open `http://127.0.0.1:3056`.

## Branch Point

The billing dispute follow-through thread (`tickets:JRA-CFS-12`) reopens on the same morning that finance is already being pulled back into the service response. The branch fires at event `history_d72d8164b1ca4198` with 38 prior canonical events across google, jira, mail_archive, and slack source families.

- Scenario variant: `billing_dispute_reopened`
- Contract variant: `protect_revenue`
- Recorded future events after the branch: 1

## What Actually Happened

The saved reference forecast completed. The top-ranked candidate action is "Contain finance risk early" — a move that stays close to the historical business path.

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
python scripts/build_service_ops_example_bundles.py --bundle clearwater-billing-dispute-reopened
python scripts/validate_whatif_artifacts.py docs/examples/clearwater-billing-dispute-reopened
```
