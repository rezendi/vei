from __future__ import annotations

import argparse
import bisect
import json
import sys
import warnings
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.enron_example_specs import bundle_specs
from scripts.enron_action_sensitivity import evaluate_enron_action_sensitivity
from vei.whatif._benchmark_case_packs import (
    BENCHMARK_CASE_PACKS,
    DEFAULT_BENCHMARK_PACK_ID,
)
from vei.whatif._enron_dataset import (
    repo_enron_sample_rosetta_dir,
    resolve_cached_full_enron_rosetta_dir,
)
from vei.whatif._enron_history import build_enron_branch_history
from vei.whatif.api import load_world
from vei.whatif.benchmark import _build_pre_branch_contract
from vei.whatif.benchmark_bridge import (
    BenchmarkPreprocessor,
    TorchTrainer,
    _action_vector_width,
    _load_compatible_state_dict,
    _SEQUENCE_NUMERIC_WIDTH,
    _SEQUENCE_TOKEN_LIMIT,
)
from vei.whatif.models import WhatIfActionSchema, WhatIfArtifactFlags, WhatIfEvent
from vei.whatif.corpus._enron import hydrate_event_snippets

MAX_BUNDLE_BYTES = 25_000_000
DEFAULT_CHECKPOINT_PATH = Path("data/enron/reference_backend/model.pt")


class Lens:
    def __init__(
        self,
        lens_id: str,
        label: str,
        summary: str,
        predicate: Callable[[WhatIfEvent], bool],
    ) -> None:
        self.id = lens_id
        self.label = label
        self.summary = summary
        self.predicate = predicate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a static Enron replay bundle for strangelab.ai/enron."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rosetta-dir", type=Path, default=None)
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--max-dates-per-lens", type=int, default=72)
    parser.add_argument("--max-evidence-events", type=int, default=14)
    parser.add_argument("--max-bundle-bytes", type=int, default=MAX_BUNDLE_BYTES)
    parser.add_argument(
        "--min-action-score-spread",
        type=float,
        default=0.0,
        help=(
            "Minimum action-conditioned balanced-score spread required for the "
            "exported ONNX model. Use a positive value when publishing a new "
            "action-sensitive Enron checkpoint."
        ),
    )
    parser.add_argument(
        "--min-action-passing-state-fraction",
        type=float,
        default=0.5,
        help=(
            "Fraction of sampled Enron states that must meet "
            "--min-action-score-spread when --require-action-sensitive is used."
        ),
    )
    parser.add_argument(
        "--require-action-sensitive",
        action="store_true",
        help="Fail the export when the action-sensitivity audit is below threshold.",
    )
    parser.add_argument("--generated-at", default="")
    args = parser.parse_args()

    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    rosetta_dir = (
        args.rosetta_dir.expanduser().resolve()
        if args.rosetta_dir is not None
        else _default_rosetta_dir()
    )
    checkpoint_path = args.checkpoint.expanduser().resolve()

    world = load_world(
        source="enron",
        rosetta_dir=rosetta_dir,
        include_content=False,
        include_situation_graph=False,
    )
    checkpoint = _load_checkpoint(checkpoint_path)
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])

    export_model_onnx(
        checkpoint=checkpoint,
        preprocessor=preprocessor,
        output_path=output / "jepa_model.onnx",
    )
    bundle = build_static_bundle(
        world=world,
        preprocessor=preprocessor,
        checkpoint_path=checkpoint_path,
        generated_at=args.generated_at or _utc_now(),
        max_dates_per_lens=max(1, args.max_dates_per_lens),
        max_evidence_events=max(4, args.max_evidence_events),
    )
    action_sensitivity = evaluate_enron_action_sensitivity(
        bundle=bundle,
        model_path=output / "jepa_model.onnx",
        min_score_spread=max(0.0, args.min_action_score_spread),
        min_passing_state_fraction=(
            min(1.0, max(0.0, args.min_action_passing_state_fraction))
            if args.require_action_sensitive
            else 0.0
        ),
    )
    bundle["model"]["action_sensitivity"] = {
        key: value for key, value in action_sensitivity.items() if key != "states"
    }
    if args.require_action_sensitive and action_sensitivity["status"] != "pass":
        raise RuntimeError(
            "Enron action-sensitivity audit failed: "
            + "; ".join(action_sensitivity["warnings"])
        )
    bundle_path = output / "bundle.json"
    payload = json.dumps(bundle, sort_keys=True, separators=(",", ":"))
    encoded = payload.encode("utf-8")
    if len(encoded) > int(args.max_bundle_bytes):
        raise RuntimeError(
            f"Enron bundle is {len(encoded):,} bytes, above limit "
            f"{int(args.max_bundle_bytes):,}. Reduce dates/evidence before deploy."
        )
    bundle_path.write_bytes(encoded)
    manifest = {
        "version": "1",
        "generated_at": bundle["generated_at"],
        "rosetta_dir": str(rosetta_dir),
        "checkpoint_path": str(checkpoint_path),
        "bundle_path": str(bundle_path),
        "bundle_bytes": len(encoded),
        "bundle_sha256": sha256(encoded).hexdigest(),
        "model_bytes": (output / "jepa_model.onnx").stat().st_size,
        "model_sha256": _file_sha256(output / "jepa_model.onnx"),
        "lens_count": len(bundle["lenses"]),
        "state_count": sum(len(states) for states in bundle["states"].values()),
        "event_count": len(bundle["events"]),
        "case_anchor_count": len(bundle["case_anchors"]),
        "action_sensitivity": bundle["model"]["action_sensitivity"],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


def _default_rosetta_dir() -> Path:
    return (
        resolve_cached_full_enron_rosetta_dir() or repo_enron_sample_rosetta_dir()
    ).resolve()


def _load_checkpoint(path: Path) -> dict[str, Any]:
    import torch

    return torch.load(path, map_location="cpu")


def export_model_onnx(
    *,
    checkpoint: dict[str, Any],
    preprocessor: BenchmarkPreprocessor,
    output_path: Path,
) -> None:
    import torch

    trainer = TorchTrainer(
        model_id=checkpoint["model_id"],
        preprocessor=preprocessor,
    )
    model = trainer.build_model(device="cpu")
    _load_compatible_state_dict(model, checkpoint["state_dict"])
    model.eval()

    class EnronReplayModel(torch.nn.Module):
        def __init__(self, wrapped: Any) -> None:
            super().__init__()
            self.wrapped = wrapped

        def forward(
            self,
            summary: Any,
            action: Any,
            token_categorical: Any,
            token_numeric: Any,
        ) -> tuple[Any, Any, Any, Any, Any]:
            output = self.wrapped(
                summary,
                action,
                token_categorical,
                token_numeric,
            )
            return (
                output["binary_logits"],
                output["regression"],
                output["business"],
                output["objective"],
                output["future_state"],
            )

    wrapper = EnronReplayModel(model).eval()
    # Batch 2 keeps the first axis symbolic under torch.export; batch 1 is
    # otherwise specialized to a constant in the ONNX graph.
    sample_batch = 2
    summary = torch.zeros(
        sample_batch,
        len(preprocessor.summary_feature_names),
        dtype=torch.float32,
    )
    action = torch.zeros(
        sample_batch,
        _action_vector_width(preprocessor),
        dtype=torch.float32,
    )
    token_categorical = torch.zeros(
        sample_batch,
        _SEQUENCE_TOKEN_LIMIT,
        3,
        dtype=torch.long,
    )
    token_numeric = torch.zeros(
        sample_batch,
        _SEQUENCE_TOKEN_LIMIT,
        _SEQUENCE_NUMERIC_WIDTH,
        dtype=torch.float32,
    )
    batch = torch.export.Dim("batch", min=1, max=64)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"`isinstance\(treespec, LeafSpec\)` is deprecated.*",
            category=FutureWarning,
        )
        torch.onnx.export(
            wrapper,
            (summary, action, token_categorical, token_numeric),
            output_path,
            input_names=["summary", "action", "token_categorical", "token_numeric"],
            output_names=[
                "binary_logits",
                "regression",
                "business",
                "objective",
                "future_state",
            ],
            dynamic_shapes={
                "summary": {0: batch},
                "action": {0: torch.export.Dim.AUTO},
                "token_categorical": {0: torch.export.Dim.AUTO},
                "token_numeric": {0: torch.export.Dim.AUTO},
            },
            opset_version=18,
            dynamo=True,
        )


def build_static_bundle(
    *,
    world: Any,
    preprocessor: BenchmarkPreprocessor,
    checkpoint_path: Path,
    generated_at: str,
    max_dates_per_lens: int,
    max_evidence_events: int,
) -> dict[str, Any]:
    lenses = _lenses()
    events_by_id: dict[str, dict[str, Any]] = {}
    states: dict[str, dict[str, Any]] = {}
    global_dates: set[str] = set()
    case_anchors = _case_anchors(world)
    case_dates = {
        str(anchor.get("timestamp", ""))[:10]
        for anchor in case_anchors
        if str(anchor.get("timestamp", "")).strip()
    }

    for lens in lenses:
        lens_events = sorted(
            [event for event in world.events if lens.predicate(event)],
            key=lambda event: (event.timestamp_ms, event.event_id),
        )
        if not lens_events:
            continue
        lens_timestamps = [event.timestamp_ms for event in lens_events]
        dates = _sample_dates(
            [event.timestamp[:10] for event in lens_events],
            required_dates=case_dates,
            max_dates=max_dates_per_lens,
        )
        lens_states: dict[str, Any] = {}
        for day in dates:
            as_of_ms = _timestamp_ms(f"{day}T23:59:59Z")
            cutoff_index = bisect.bisect_right(lens_timestamps, as_of_ms)
            if cutoff_index <= 0:
                continue
            # The browser bundle must stay deployable. The state records the full
            # pre-cutoff count, but the model/evidence window uses a recent slice.
            history_window = lens_events[max(0, cutoff_index - 260) : cutoff_index]
            state = _build_state(
                world=world,
                lens=lens,
                day=day,
                history_events=history_window,
                raw_history_event_count=cutoff_index,
                preprocessor=preprocessor,
                max_evidence_events=max_evidence_events,
            )
            if state is None:
                continue
            for event in state.pop("_events"):
                events_by_id[event["event_id"]] = event
            lens_states[day] = state
            global_dates.add(day)
        states[lens.id] = lens_states

    for anchor in case_anchors:
        event_id = str(anchor.get("event_id") or "")
        if event_id and event_id not in events_by_id:
            event = _world_event_by_id(world, event_id)
            if event is not None:
                hydrated = hydrate_event_snippets(
                    rosetta_dir=world.rosetta_dir,
                    events=[event],
                )[0]
                events_by_id[event_id] = _event_payload(hydrated)

    state_actor_ids = {
        actor_id
        for lens_states in states.values()
        for state in lens_states.values()
        for actor_id in state.get("top_actor_ids", [])
    }
    state_thread_ids = {
        thread_id
        for lens_states in states.values()
        for state in lens_states.values()
        for thread_id in state.get("thread_ids", [])
    }

    return {
        "version": "1",
        "generated_at": generated_at,
        "source": {
            "source_id": "enron_live_replay_v1",
            "title": "Enron Replay",
            "summary": (
                "Choose a historical cutoff, inspect only pre-cutoff Enron evidence, "
                "write an email as an actor, and score plausible branches with VEI."
            ),
            "default_lens": "all_mail_public",
            "default_as_of": _default_as_of(states),
            "first_timestamp": world.summary.first_timestamp,
            "last_timestamp": world.summary.last_timestamp,
            "event_count": world.summary.event_count,
            "thread_count": world.summary.thread_count,
            "actor_count": world.summary.actor_count,
        },
        "lenses": [
            {"id": lens.id, "label": lens.label, "summary": lens.summary}
            for lens in lenses
            if lens.id in states
        ],
        "dates": sorted(global_dates),
        "events": dict(sorted(events_by_id.items())),
        "states": states,
        "actors": _actor_payloads(world, include_actor_ids=state_actor_ids),
        "threads": _thread_payloads(world, include_thread_ids=state_thread_ids),
        "case_anchors": case_anchors,
        "suggested_emails": _suggested_emails(),
        "model": {
            "model_id": str("enron_" + str(checkpoint_path.name).replace(".pt", "")),
            "onnx_path": "jepa_model.onnx",
            "checkpoint_sha256": _file_sha256(checkpoint_path),
            "summary_feature_names": preprocessor.summary_feature_names,
            "action_tag_names": preprocessor.action_tag_names,
            "event_type_names": preprocessor.event_type_names,
            "target_mean": _round_floats(preprocessor.target_mean.tolist()),
            "target_std": _round_floats(preprocessor.target_std.tolist()),
            "business_mean": _round_floats(preprocessor.business_mean.tolist()),
            "business_std": _round_floats(preprocessor.business_std.tolist()),
            "objective_mean": _round_floats(preprocessor.objective_mean.tolist()),
            "objective_std": _round_floats(preprocessor.objective_std.tolist()),
            "objective_head_trained": preprocessor.objective_head_trained,
            "future_state_mean": _round_floats(preprocessor.future_state_mean.tolist()),
            "future_state_std": _round_floats(preprocessor.future_state_std.tolist()),
            "action_text_vector_width": preprocessor.action_text_vector_width,
            "sequence_token_limit": _SEQUENCE_TOKEN_LIMIT,
            "sequence_numeric_width": _SEQUENCE_NUMERIC_WIDTH,
        },
        "caveat": (
            "This is evidence-grounded historical simulation, not causal proof. "
            "The user email is treated as a new candidate action at the selected "
            "cutoff, generated alternatives must cite only pre-cutoff evidence, "
            "and JEPA scores are decision-support readouts rather than historical "
            "truth."
        ),
    }


def _build_state(
    *,
    world: Any,
    lens: Lens,
    day: str,
    history_events: Sequence[WhatIfEvent],
    raw_history_event_count: int,
    preprocessor: BenchmarkPreprocessor,
    max_evidence_events: int,
) -> dict[str, Any] | None:
    as_of = f"{day}T23:59:59Z"
    as_of_ms = _timestamp_ms(as_of)
    history = [event for event in history_events if event.timestamp_ms <= as_of_ms]
    if not history:
        return None
    actor_id = _top_actor_id(history) or "operator@enron.com"
    branch_event = WhatIfEvent(
        event_id=f"enron_state_{lens.id}_{day}",
        timestamp=as_of,
        timestamp_ms=as_of_ms,
        actor_id=actor_id,
        target_id="enron.com",
        event_type="message",
        thread_id=f"state:{lens.id}",
        case_id=f"enron-state:{lens.id}",
        surface="mail",
        subject=f"{lens.label} state as of {day}",
        snippet=f"Playable Enron state for {lens.label} as of {day}.",
        flags=WhatIfArtifactFlags(
            to_recipients=["enron.com"],
            to_count=1,
            subject=f"{lens.label} state as of {day}",
        ),
    )
    branch_history = build_enron_branch_history(
        public_context=world.public_context,
        branch_event=branch_event,
        past_events=history,
        min_history_events=30,
        max_history_events=36,
    )
    evidence = hydrate_event_snippets(
        rosetta_dir=world.rosetta_dir,
        events=_select_evidence_events(branch_history, max_events=max_evidence_events),
    )
    dummy_action = _dummy_action_schema()
    contract = _build_pre_branch_contract(
        case_id=branch_event.case_id,
        thread_id=branch_event.thread_id,
        branch_event=branch_event,
        history_events=branch_history,
        organization_domain=world.summary.organization_domain,
        action_schema=dummy_action,
        notes=[
            "Enron playable state-point row.",
            "no_future_context=true",
            "state_point_not_historical_branch_event=true",
        ],
    )
    summary_values = preprocessor._encode_summary(
        contract.summary_features,
        doctrine_context=contract.doctrine_context,
    )
    token_categorical, token_numeric = preprocessor._encode_sequence(
        contract.sequence_steps,
        dummy_action,
        summary_values,
    )
    top_threads = _top_threads(history, limit=8)
    state_events = [_event_payload(event) for event in evidence]
    return {
        "as_of": as_of,
        "lens": lens.id,
        "state_summary": _state_summary(
            lens=lens,
            day=day,
            history=history,
            raw_history_event_count=raw_history_event_count,
        ),
        "history_event_count": raw_history_event_count,
        "model_history_event_count": len(branch_history),
        "evidence_event_ids": [event.event_id for event in evidence],
        "headline": _headline(evidence, lens=lens),
        "top_actor_ids": [
            actor_id for actor_id, _count in _top_actor_counts(history, 6)
        ],
        "thread_ids": [thread_id for thread_id, _count in top_threads],
        "summary": _round_floats(summary_values.tolist()),
        "token_categorical_base": [
            int(value) for value in token_categorical.reshape(-1).tolist()
        ],
        "token_numeric_base": _round_floats(token_numeric.reshape(-1).tolist()),
        "action_index": len(contract.sequence_steps[-(_SEQUENCE_TOKEN_LIMIT - 2) :]),
        "_events": state_events,
    }


def _lenses() -> tuple[Lens, ...]:
    return (
        Lens(
            "all_mail_public",
            "All Enron record",
            "Mail, public-company facts, credit/stock signals, and saved case anchors.",
            lambda _event: True,
        ),
        Lens(
            "legal_contracts",
            "Legal and contracts",
            "Agreement drafts, review loops, outside counsel, attachments, and legal routing.",
            lambda event: event.flags.consult_legal_specialist
            or _contains(
                event,
                "agreement",
                "contract",
                "isda",
                "legal",
                "counsel",
                "draft",
                "redline",
                "review",
            ),
        ),
        Lens(
            "trading_regulatory",
            "Trading and regulatory",
            "Power, gas, California, FERC, trader, regulatory, and preservation-order signals.",
            lambda event: event.flags.consult_trading_specialist
            or _contains(
                event,
                "trading",
                "trader",
                "ferc",
                "california",
                "power",
                "gas",
                "refund",
                "preservation",
                "regulatory",
            ),
        ),
        Lens(
            "accounting_disclosure",
            "Accounting and disclosure",
            "Quarter close, review, financial disclosure, restatement, Braveheart, and structure signals.",
            lambda event: _contains(
                event,
                "accounting",
                "disclosure",
                "quarter",
                "q3",
                "earnings",
                "financial",
                "review",
                "restatement",
                "braveheart",
                "raptor",
            ),
        ),
        Lens(
            "executive_crisis",
            "Executive crisis comms",
            "Lay, Skilling, Fastow, Baxter, press-release, investor, and communications loops.",
            lambda event: _contains(
                event,
                "lay",
                "skilling",
                "fastow",
                "baxter",
                "press release",
                "resignation",
                "communications",
                "investor",
                "executive",
            ),
        ),
        Lens(
            "counterparty_credit",
            "Counterparty and credit",
            "PG&E, Cargill, credit, collateral, counterparty, swap, and commercial-deal pressure.",
            lambda event: _contains(
                event,
                "pge",
                "pg&e",
                "cargill",
                "credit",
                "collateral",
                "counterparty",
                "swap",
                "power deal",
                "financial power",
            ),
        ),
        Lens(
            "governance_whistleblower",
            "Governance and whistleblower",
            "Watkins, audit, investigation, compliance, policy, risk, preservation, and board signals.",
            lambda event: _contains(
                event,
                "watkins",
                "audit",
                "investigation",
                "compliance",
                "policy",
                "risk",
                "preserve",
                "board",
                "questions",
                "whistle",
            ),
        ),
    )


def _sample_dates(
    dates: Iterable[str],
    *,
    required_dates: set[str],
    max_dates: int,
) -> list[str]:
    ordered = sorted({date for date in dates if len(date) == 10})
    if not ordered:
        return []
    selected = set(_even_sample(ordered, max_dates))
    for date in required_dates:
        if ordered[0] <= date <= ordered[-1]:
            selected.add(date)
    if len(selected) > max_dates + len(required_dates):
        selected = set(_even_sample(sorted(selected), max_dates + len(required_dates)))
    return sorted(selected)


def _even_sample(values: Sequence[str], limit: int) -> list[str]:
    if len(values) <= limit:
        return list(values)
    if limit <= 1:
        return [values[-1]]
    return [
        values[round(index * (len(values) - 1) / (limit - 1))] for index in range(limit)
    ]


def _select_evidence_events(
    events: Sequence[WhatIfEvent],
    *,
    max_events: int,
) -> list[WhatIfEvent]:
    scored = sorted(
        events,
        key=lambda event: (
            -_evidence_score(event),
            -event.timestamp_ms,
            event.event_id,
        ),
    )
    selected = sorted(
        scored[:max_events],
        key=lambda event: (event.timestamp_ms, event.event_id),
    )
    return selected


def _evidence_score(event: WhatIfEvent) -> int:
    score = 0
    text = _event_text(event)
    for token in (
        "legal",
        "review",
        "agreement",
        "contract",
        "credit",
        "pge",
        "california",
        "ferc",
        "skilling",
        "lay",
        "fastow",
        "watkins",
        "baxter",
        "disclosure",
        "quarter",
        "press release",
        "risk",
    ):
        if token in text:
            score += 2
    if event.flags.has_attachment_reference:
        score += 3
    if event.flags.consult_legal_specialist:
        score += 3
    if event.flags.consult_trading_specialist:
        score += 2
    if event.flags.is_forward:
        score += 1
    if event.flags.cc_count:
        score += min(event.flags.cc_count, 5)
    return score


def _case_anchors(world: Any) -> list[dict[str, Any]]:
    seeds = {
        seed.case_id: seed for seed in BENCHMARK_CASE_PACKS[DEFAULT_BENCHMARK_PACK_ID]
    }
    anchors: list[dict[str, Any]] = []
    for spec in bundle_specs():
        seed = seeds.get(spec.case_id)
        event = _world_event_by_id(world, seed.event_id if seed is not None else "")
        timestamp = event.timestamp if event is not None else ""
        anchors.append(
            {
                "case_id": spec.case_id,
                "bundle_slug": spec.bundle_slug,
                "role": spec.role,
                "title": spec.title,
                "event_id": seed.event_id if seed is not None else "",
                "timestamp": timestamp,
                "family": seed.family if seed is not None else "",
                "branch_point": spec.branch_point,
                "actual_happened": spec.actual_happened,
                "primary_prompt": spec.primary_prompt,
                "candidates": [
                    {
                        "label": candidate.label,
                        "action": candidate.prompt,
                        "explanation": candidate.explanation,
                    }
                    for candidate in spec.candidates
                ],
            }
        )
    return anchors


def _actor_payloads(
    world: Any,
    *,
    include_actor_ids: set[str],
) -> list[dict[str, Any]]:
    top_actors = sorted(
        world.actors,
        key=lambda item: (-item.event_count, item.actor_id),
    )[:80]
    by_id = {actor.actor_id: actor for actor in world.actors}
    actors = {
        actor.actor_id: actor
        for actor in [
            *top_actors,
            *(by_id[actor_id] for actor_id in include_actor_ids if actor_id in by_id),
        ]
    }
    payloads = [
        {
            "actor_id": actor.actor_id,
            "display_name": actor.display_name or actor.email or actor.actor_id,
            "email": actor.email or actor.actor_id,
            "event_count": actor.event_count,
            "sent_count": actor.sent_count,
            "received_count": actor.received_count,
        }
        for actor in sorted(
            actors.values(),
            key=lambda item: (-item.event_count, item.actor_id),
        )
    ]
    known_ids = {payload["actor_id"] for payload in payloads}
    for actor_id in sorted(include_actor_ids - known_ids):
        payloads.append(
            {
                "actor_id": actor_id,
                "display_name": actor_id,
                "email": actor_id,
                "event_count": 0,
                "sent_count": 0,
                "received_count": 0,
            }
        )
    return payloads


def _thread_payloads(
    world: Any,
    *,
    include_thread_ids: set[str],
) -> list[dict[str, Any]]:
    top_threads = sorted(
        world.threads,
        key=lambda item: (-item.event_count, item.thread_id),
    )[:140]
    by_id = {thread.thread_id: thread for thread in world.threads}
    threads = {
        thread.thread_id: thread
        for thread in [
            *top_threads,
            *(
                by_id[thread_id]
                for thread_id in include_thread_ids
                if thread_id in by_id
            ),
        ]
    }
    return [
        {
            "thread_id": thread.thread_id,
            "subject": thread.subject,
            "event_count": thread.event_count,
            "actor_ids": thread.actor_ids[:10],
            "first_timestamp": thread.first_timestamp,
            "last_timestamp": thread.last_timestamp,
        }
        for thread in sorted(
            threads.values(),
            key=lambda item: (-item.event_count, item.thread_id),
        )
    ]


def _suggested_emails() -> list[dict[str, str]]:
    return [
        {
            "label": "Hold and review",
            "subject": "Review path before outside circulation",
            "body": (
                "Before this goes outside, keep the draft internal, assign one owner, "
                "and route legal, credit, and business review through a single reply loop."
            ),
        },
        {
            "label": "Controlled external update",
            "subject": "Limited status update",
            "body": (
                "Send a short status note only, with no attachment, and promise a reviewed "
                "draft after legal and credit signoff."
            ),
        },
        {
            "label": "Executive escalation",
            "subject": "Escalation and evidence preservation",
            "body": (
                "Escalate this to the executive owner, preserve the current record, and "
                "ask compliance whether the team should pause or self-report before acting."
            ),
        },
    ]


def _event_payload(event: WhatIfEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "timestamp": event.timestamp,
        "actor_id": event.actor_id,
        "target_id": event.target_id,
        "event_type": event.event_type,
        "thread_id": event.thread_id,
        "case_id": event.case_id,
        "surface": event.surface or "mail",
        "subject": event.subject,
        "snippet": _snippet(event.snippet or event.subject),
        "to_recipients": event.flags.to_recipients[:12],
        "cc_recipients": event.flags.cc_recipients[:12],
        "has_attachment_reference": event.flags.has_attachment_reference,
        "is_forward": event.flags.is_forward,
        "is_reply": event.flags.is_reply,
        "is_escalation": event.flags.is_escalation,
    }


def _dummy_action_schema() -> WhatIfActionSchema:
    return WhatIfActionSchema(
        event_type="message",
        action_text="Hold for internal review before taking action.",
        recipient_scope="internal",
        external_recipient_count=0,
        attachment_policy="none",
        hold_required=True,
        legal_review_required=True,
        trading_review_required=False,
        escalation_level="manager",
        owner_clarity="single_owner",
        reassurance_style="low",
        review_path="internal_legal",
        coordination_breadth="narrow",
        outside_sharing_posture="internal_only",
        decision_posture="hold",
        action_tags=["hold", "internal_only", "legal", "review", "single_owner"],
    )


def _state_summary(
    *,
    lens: Lens,
    day: str,
    history: Sequence[WhatIfEvent],
    raw_history_event_count: int,
) -> str:
    actors = ", ".join(actor for actor, _count in _top_actor_counts(history, 4))
    threads = len({event.thread_id for event in history if event.thread_id})
    return (
        f"As of {day}, the {lens.label.lower()} state contains "
        f"{raw_history_event_count:,} pre-cutoff Enron events, with a deployable "
        f"recent evidence window across {threads:,} nearby threads. "
        f"Active actors include {actors or 'unknown actors'}. "
        "The simulator may use only the evidence shown for this cutoff."
    )


def _headline(events: Sequence[WhatIfEvent], *, lens: Lens) -> str:
    for event in reversed(events):
        if event.subject:
            return event.subject
    return lens.label


def _top_actor_id(events: Sequence[WhatIfEvent]) -> str:
    counts = _top_actor_counts(events, 1)
    return counts[0][0] if counts else ""


def _top_actor_counts(
    events: Sequence[WhatIfEvent],
    limit: int,
) -> list[tuple[str, int]]:
    counts = Counter(
        actor
        for event in events
        for actor in [event.actor_id, event.target_id, *event.flags.to_recipients]
        if actor and not actor.startswith("group:")
    )
    return counts.most_common(limit)


def _top_threads(events: Sequence[WhatIfEvent], *, limit: int) -> list[tuple[str, int]]:
    counts = Counter(event.thread_id for event in events if event.thread_id)
    return counts.most_common(limit)


def _contains(event: WhatIfEvent, *tokens: str) -> bool:
    text = _event_text(event)
    return any(token in text for token in tokens)


def _event_text(event: WhatIfEvent) -> str:
    return " ".join(
        [
            event.subject,
            event.snippet,
            event.actor_id,
            event.target_id,
            " ".join(event.flags.to_recipients),
            " ".join(event.flags.cc_recipients),
        ]
    ).lower()


def _world_event_by_id(world: Any, event_id: str) -> WhatIfEvent | None:
    if not event_id:
        return None
    for event in world.events:
        if event.event_id == event_id:
            return event
    return None


def _timestamp_ms(timestamp: str) -> int:
    text = timestamp.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return 0


def _default_as_of(states: dict[str, dict[str, Any]]) -> str:
    preferred = "2001-08-14"
    for lens_states in states.values():
        dates = sorted(lens_states)
        if not dates:
            continue
        if preferred in lens_states:
            return preferred
    all_dates = sorted(date for lens_states in states.values() for date in lens_states)
    return all_dates[len(all_dates) // 2] if all_dates else preferred


def _snippet(text: str, limit: int = 520) -> str:
    clean = " ".join(str(text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "..."


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _round_floats(values: Sequence[float]) -> list[float]:
    return [round(float(value), 6) for value in values]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
