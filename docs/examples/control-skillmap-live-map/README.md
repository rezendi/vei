# VEI Control Skill-Map Refresh Example

This example shows the smallest path from captured agent behavior to a refreshed
company skill map.

The workspace fixture contains company context in `workspace/context_snapshot.json`.
The adjacent `agent_activity.jsonl` file is a captured Control event: an agent
created an internal renewal-risk draft for `case:CASE-789`.

## Command Path

```bash
mkdir -p _vei_out/control-skillmap-live-map
cp docs/examples/control-skillmap-live-map/workspace/context_snapshot.json \
  _vei_out/control-skillmap-live-map/context_snapshot.json

vei ingest agent-activity \
  --source agent_activity_jsonl \
  --path docs/examples/control-skillmap-live-map/agent_activity.jsonl \
  --workspace _vei_out/control-skillmap-live-map \
  --tenant-id apex.example \
  --format json

vei skillmap refresh \
  --workspace _vei_out/control-skillmap-live-map \
  --output _vei_out/control-skillmap-live-map/skill_map \
  --limit 4 \
  --no-replay
```

`vei skillmap refresh` reads the workspace context and imported Control events
as one canonical event spine, preserves the previous map from the output
directory when one exists, writes refreshed draft skills, and leaves a
`control_evidence_pack.json` beside the skill-map reports. The example uses
`--no-replay` because it is a minimal Control-to-skill-map fixture rather than
a packaged what-if replay bundle.
