# VEI Studio UX Rollout

This document now tracks the shipped Studio UX pass.

## Current shell behavior

- Studio ships one shell with automatic tone selection:
  - historical archive workspaces use the lighter casebook tone
  - live and governor workspaces use the darker control-room tone
- The top shell now carries:
  - Company sub-navigation (`Live Company`, `Next Move`, `Recent Changes`, `Historical Decision`)
  - helper copy under the top-level Studio tabs
  - persistent section memory when you leave and return to the `Company` tab
- Historical workspaces still auto-focus `Historical Decision` on the first load of a saved branch.

## Current interaction behavior

- `Historical Decision` is a guided 3-step flow:
  1. Find Decision
  2. Compare Moves
  3. Review Forecast
- Historical action feedback is explicit:
  - busy and disabled states for search, materialize, run, and rank
  - clear no-results messaging separate from the initial empty state
  - Enter-to-search from the query field
  - one in-flight what-if request at a time across the shared page state
- Ranked comparison cards surface exposure, delay, and relationship signals above the expanded details.
- `service_ops` keeps the control-room hierarchy, KPI strip, and explicit empty-state copy for idle panels.

## Smoke checks

Use these checks after UI changes:

1. Load the repo-owned Enron workspace in Studio:
   - `vei ui serve --root docs/examples/enron-master-agreement-public-context/workspace`
   - Confirm Company sub-navigation appears and the first load lands on `Historical Decision`
2. Check Company tab memory:
   - Jump to `Live Company`
   - Open `Outcome`
   - Return to `Company`
   - Confirm Studio returns to `Live Company`
3. Check historical action states:
   - Search a decision
   - Double-trigger search while it is still running
   - Confirm only one request runs and the historical action buttons stay disabled until it finishes
4. Check Dispatch or `service_ops`:
   - Open a `service_ops` workspace
   - Verify the KPI strip includes dispatch pressure and approval context
   - Verify idle panels show explicit empty-state text when no live panel data is available
