from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.enron_action_sensitivity import (
    evaluate_enron_action_sensitivity,
    iter_report_failures,
    load_bundle,
    model_path_for_bundle,
    write_action_sensitivity_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Measure whether the exported Enron browser JEPA model separates "
            "different candidate actions for the same pre-cutoff state."
        )
    )
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--model", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--max-states", type=int, default=12)
    parser.add_argument("--min-score-spread", type=float, default=0.01)
    parser.add_argument("--min-passing-state-fraction", type=float, default=0.5)
    parser.add_argument(
        "--allow-flat",
        action="store_true",
        help="Write the report but do not fail when the checkpoint is flat.",
    )
    args = parser.parse_args()

    bundle_path = args.bundle.expanduser().resolve()
    bundle = load_bundle(bundle_path)
    model_path = (
        args.model.expanduser().resolve()
        if args.model is not None
        else model_path_for_bundle(bundle_path, bundle)
    )
    report = evaluate_enron_action_sensitivity(
        bundle=bundle,
        model_path=model_path,
        max_states=max(1, args.max_states),
        min_score_spread=max(0.0, args.min_score_spread),
        min_passing_state_fraction=min(
            1.0,
            max(0.0, args.min_passing_state_fraction),
        ),
    )
    if args.output is not None:
        write_action_sensitivity_report(report, args.output.expanduser().resolve())
    else:
        print(json.dumps(report, indent=2, sort_keys=True))

    failures = list(iter_report_failures(report))
    if failures and not args.allow_flat:
        for failure in failures:
            print(f"Enron action-sensitivity audit failed: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
