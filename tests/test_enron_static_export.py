from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from scripts.export_enron_static_assets import (
    DEFAULT_CHECKPOINT_PATH,
    build_static_bundle,
    export_model_onnx,
    _load_checkpoint,
)
from scripts.enron_action_sensitivity import (
    evaluate_enron_action_sensitivity,
    public_enron_audit_actions,
)
from scripts.enron_example_specs import bundle_specs
from vei.whatif._enron_dataset import repo_enron_sample_rosetta_dir
from vei.whatif.api import load_world
from vei.whatif.benchmark_bridge import BenchmarkPreprocessor, _action_vector_width


def _sample_bundle() -> dict:
    checkpoint = _load_checkpoint(DEFAULT_CHECKPOINT_PATH.resolve())
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    world = load_world(
        source="enron",
        rosetta_dir=repo_enron_sample_rosetta_dir(),
        include_content=False,
        include_situation_graph=False,
    )
    return build_static_bundle(
        world=world,
        preprocessor=preprocessor,
        checkpoint_path=DEFAULT_CHECKPOINT_PATH.resolve(),
        generated_at="2026-04-30T00:00:00Z",
        max_dates_per_lens=8,
        max_evidence_events=6,
    )


def test_enron_static_bundle_is_cutoff_safe_and_covers_saved_cases() -> None:
    bundle = _sample_bundle()

    assert bundle["source"]["source_id"] == "enron_live_replay_v1"
    assert len(bundle["lenses"]) == 7
    assert sum(len(states) for states in bundle["states"].values()) >= 20
    assert len(bundle["events"]) >= 20

    expected_slugs = {spec.bundle_slug for spec in bundle_specs()}
    assert {
        anchor["bundle_slug"] for anchor in bundle["case_anchors"]
    } == expected_slugs

    events = bundle["events"]
    actors = {actor["actor_id"] for actor in bundle["actors"]}
    threads = {thread["thread_id"] for thread in bundle["threads"]}
    for lens_states in bundle["states"].values():
        for day, state in lens_states.items():
            assert state["as_of"].startswith(day)
            assert state["history_event_count"] > 0
            assert state["model_history_event_count"] > 0
            assert set(state["top_actor_ids"]).issubset(actors)
            assert set(state["thread_ids"]).issubset(threads)
            for event_id in state["evidence_event_ids"]:
                event = events[event_id]
                assert event["timestamp"][:10] <= day


def test_enron_static_bundle_is_deterministic_and_deploy_sized() -> None:
    left = _sample_bundle()
    right = _sample_bundle()

    assert json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True)
    assert (
        len(json.dumps(left, sort_keys=True, separators=(",", ":")).encode())
        < 25_000_000
    )


def test_enron_onnx_export_runs_one_forward_pass(tmp_path: Path) -> None:
    ort = pytest.importorskip("onnxruntime")

    checkpoint = _load_checkpoint(DEFAULT_CHECKPOINT_PATH.resolve())
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    output_path = tmp_path / "jepa_model.onnx"

    export_model_onnx(
        checkpoint=checkpoint,
        preprocessor=preprocessor,
        output_path=output_path,
    )
    assert output_path.stat().st_size > 0

    session = ort.InferenceSession(str(output_path), providers=["CPUExecutionProvider"])
    action_width = _action_vector_width(preprocessor)
    outputs = session.run(
        None,
        {
            "summary": np.zeros(
                (1, len(preprocessor.summary_feature_names)),
                dtype=np.float32,
            ),
            "action": np.zeros((1, action_width), dtype=np.float32),
            "token_categorical": np.zeros((1, 12, 3), dtype=np.int64),
            "token_numeric": np.zeros((1, 12, 12), dtype=np.float32),
        },
    )

    assert len(outputs) == 5
    assert outputs[0].shape == (1,)
    assert outputs[1].shape[0] == 1


def test_enron_audit_actions_cover_distinct_public_strategies() -> None:
    actions = public_enron_audit_actions()

    assert len({action.action_id for action in actions}) == len(actions)
    assert any(action.recipient_scope == "external" for action in actions)
    assert any(action.recipient_scope == "internal" for action in actions)
    assert any(action.hold_required for action in actions)
    assert any(action.decision_posture == "resolve" for action in actions)
    assert any(action.legal_review_required for action in actions)


def test_enron_action_sensitivity_audit_runs_on_exported_onnx(
    tmp_path: Path,
) -> None:
    pytest.importorskip("onnxruntime")

    checkpoint = _load_checkpoint(DEFAULT_CHECKPOINT_PATH.resolve())
    preprocessor = BenchmarkPreprocessor.from_metadata(checkpoint["metadata"])
    output_path = tmp_path / "jepa_model.onnx"
    export_model_onnx(
        checkpoint=checkpoint,
        preprocessor=preprocessor,
        output_path=output_path,
    )

    report = evaluate_enron_action_sensitivity(
        bundle=_sample_bundle(),
        model_path=output_path,
        max_states=3,
        min_score_spread=0.0,
    )

    assert report["status"] == "pass"
    assert report["state_count"] == 3
    assert report["candidate_count"] == len(public_enron_audit_actions())
    assert "max_score_spread" in report
    assert len(report["states"]) == 3
