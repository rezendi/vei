from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

import numpy as np

PHASE_VALUES = ["history", "branch", "generated", "historical_future"]
RECIPIENT_SCOPE_VALUES = ["internal", "external", "mixed", "unknown"]
ATTACHMENT_POLICY_VALUES = ["none", "present", "sanitized"]
ESCALATION_LEVEL_VALUES = ["none", "manager", "executive"]
OWNER_CLARITY_VALUES = ["unclear", "single_owner", "multi_owner"]
REASSURANCE_STYLE_VALUES = ["low", "medium", "high"]
REVIEW_PATH_VALUES = [
    "none",
    "internal_legal",
    "outside_counsel",
    "business_owner",
    "cross_functional",
    "hr",
    "executive",
]
COORDINATION_BREADTH_VALUES = ["single_owner", "narrow", "targeted", "broad"]
OUTSIDE_SHARING_POSTURE_VALUES = [
    "internal_only",
    "status_only",
    "limited_external",
    "broad_external",
]
DECISION_POSTURE_VALUES = ["hold", "review", "resolve", "escalate"]
BUSINESS_TARGET_NAMES = [
    "enterprise_risk",
    "commercial_position_proxy",
    "org_strain_proxy",
    "stakeholder_trust",
    "execution_drag",
]
FUTURE_STATE_TARGET_NAMES = [
    "regulatory_exposure",
    "accounting_control_pressure",
    "liquidity_stress",
    "governance_response",
    "evidence_control",
    "external_confidence_pressure",
]
NEGATIVE_HEADS = {"enterprise_risk", "org_strain_proxy", "execution_drag"}


@dataclass(frozen=True)
class EnronAuditAction:
    action_id: str
    label: str
    action_text: str
    recipient_scope: str
    external_recipient_count: int
    attachment_policy: str
    hold_required: bool
    legal_review_required: bool
    trading_review_required: bool
    escalation_level: str
    owner_clarity: str
    reassurance_style: str
    review_path: str
    coordination_breadth: str
    outside_sharing_posture: str
    decision_posture: str
    action_tags: tuple[str, ...]
    event_type: str = "message"


def public_enron_audit_actions() -> list[EnronAuditAction]:
    return [
        EnronAuditAction(
            action_id="public_disclosure",
            label="Immediate public disclosure",
            action_text=(
                "Send a broad public disclosure to investors and press today, "
                "describing the concern and promising more details."
            ),
            recipient_scope="external",
            external_recipient_count=5,
            attachment_policy="none",
            hold_required=False,
            legal_review_required=False,
            trading_review_required=False,
            escalation_level="executive",
            owner_clarity="unclear",
            reassurance_style="medium",
            review_path="none",
            coordination_breadth="broad",
            outside_sharing_posture="broad_external",
            decision_posture="resolve",
            action_tags=("external", "self_report", "urgent", "widen_loop"),
        ),
        EnronAuditAction(
            action_id="legal_hold",
            label="Hold for legal review",
            action_text=(
                "Hold the message internally, preserve the record, and send it "
                "to legal counsel for privilege and disclosure review."
            ),
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
            action_tags=("hold", "internal_only", "legal", "preserve_record", "review"),
        ),
        EnronAuditAction(
            action_id="executive_escalation",
            label="Escalate to executives",
            action_text=(
                "Escalate to Lay, Skilling, and the board with a named owner, "
                "asking for a cross-functional decision before any outside reply."
            ),
            recipient_scope="internal",
            external_recipient_count=0,
            attachment_policy="none",
            hold_required=True,
            legal_review_required=True,
            trading_review_required=False,
            escalation_level="executive",
            owner_clarity="single_owner",
            reassurance_style="low",
            review_path="executive",
            coordination_breadth="broad",
            outside_sharing_posture="internal_only",
            decision_posture="escalate",
            action_tags=(
                "executive_gate",
                "hold",
                "internal_only",
                "legal",
                "widen_loop",
            ),
        ),
        EnronAuditAction(
            action_id="counterparty_update",
            label="Limited counterparty update",
            action_text=(
                "Send a narrow status update to the counterparty without attachments, "
                "committing to a reviewed answer after credit and legal signoff."
            ),
            recipient_scope="external",
            external_recipient_count=1,
            attachment_policy="sanitized",
            hold_required=False,
            legal_review_required=True,
            trading_review_required=True,
            escalation_level="manager",
            owner_clarity="single_owner",
            reassurance_style="medium",
            review_path="cross_functional",
            coordination_breadth="targeted",
            outside_sharing_posture="limited_external",
            decision_posture="review",
            action_tags=("counterparty", "external", "legal", "review", "trading"),
        ),
        EnronAuditAction(
            action_id="trading_regulatory_review",
            label="Trading and regulatory review",
            action_text=(
                "Route the issue to trading, FERC/regulatory counsel, and California "
                "market specialists before taking a market-facing position."
            ),
            recipient_scope="internal",
            external_recipient_count=0,
            attachment_policy="none",
            hold_required=True,
            legal_review_required=True,
            trading_review_required=True,
            escalation_level="manager",
            owner_clarity="multi_owner",
            reassurance_style="low",
            review_path="cross_functional",
            coordination_breadth="targeted",
            outside_sharing_posture="internal_only",
            decision_posture="review",
            action_tags=("hold", "internal_only", "legal", "review", "trading"),
        ),
    ]


def evaluate_enron_action_sensitivity(
    *,
    bundle: dict[str, Any],
    model_path: Path,
    max_states: int = 12,
    min_score_spread: float = 0.01,
    min_passing_state_fraction: float = 0.5,
) -> dict[str, Any]:
    ort = _load_onnxruntime()
    states = _sample_bundle_states(bundle, max_states=max_states)
    actions = public_enron_audit_actions()
    if not states:
        raise ValueError("bundle contains no Enron state rows to audit")
    session = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
    model = bundle["model"]
    prepared = [
        _prepare_row(state=state, action=action, model=model)
        for state in states
        for action in actions
    ]
    output = session.run(None, _build_feeds(prepared, model=model))
    business = np.asarray(output[2], dtype=np.float32)
    future = np.asarray(output[4], dtype=np.float32)

    rows: list[dict[str, Any]] = []
    offset = 0
    all_score_spreads: list[float] = []
    all_business_raw_spreads: list[float] = []
    all_future_raw_spreads: list[float] = []
    for state in states:
        state_scores: list[dict[str, Any]] = []
        state_business = business[offset : offset + len(actions)]
        state_future = future[offset : offset + len(actions)]
        for index, action in enumerate(actions):
            decoded_business = _decode_business(state_business[index], model)
            decoded_future = _decode_future(state_future[index], model)
            score = _balanced_business_readout(decoded_business)
            state_scores.append(
                {
                    "action_id": action.action_id,
                    "label": action.label,
                    "balanced_score": round(score, 6),
                    "business": _round_mapping(decoded_business),
                    "future_state": _round_mapping(decoded_future),
                }
            )
        score_values = [item["balanced_score"] for item in state_scores]
        score_spread = max(score_values) - min(score_values)
        business_raw_spread = float(np.max(np.ptp(state_business, axis=0)))
        future_raw_spread = float(np.max(np.ptp(state_future, axis=0)))
        all_score_spreads.append(score_spread)
        all_business_raw_spreads.append(business_raw_spread)
        all_future_raw_spreads.append(future_raw_spread)
        rows.append(
            {
                "lens": state["lens"],
                "as_of": state["as_of"],
                "headline": state.get("headline", ""),
                "score_spread": round(score_spread, 6),
                "business_raw_max_head_spread": round(business_raw_spread, 6),
                "future_raw_max_head_spread": round(future_raw_spread, 6),
                "scores": state_scores,
            }
        )
        offset += len(actions)

    max_score_spread = max(all_score_spreads)
    median_score_spread = float(median(all_score_spreads))
    passing_state_count = sum(
        1 for spread in all_score_spreads if spread >= min_score_spread
    )
    passing_state_fraction = passing_state_count / max(len(all_score_spreads), 1)
    action_text_vector_width = int(model.get("action_text_vector_width") or 0)
    warnings = _warnings(
        model=model,
        max_score_spread=max_score_spread,
        median_score_spread=median_score_spread,
        passing_state_fraction=passing_state_fraction,
        min_score_spread=min_score_spread,
        min_passing_state_fraction=min_passing_state_fraction,
    )
    passes = not warnings
    return {
        "version": "1",
        "status": "pass" if passes else "flat",
        "min_score_spread": min_score_spread,
        "min_passing_state_fraction": min_passing_state_fraction,
        "max_score_spread": round(max_score_spread, 6),
        "median_score_spread": round(median_score_spread, 6),
        "passing_state_count": passing_state_count,
        "passing_state_fraction": round(passing_state_fraction, 6),
        "max_business_raw_head_spread": round(max(all_business_raw_spreads), 6),
        "max_future_raw_head_spread": round(max(all_future_raw_spreads), 6),
        "state_count": len(states),
        "candidate_count": len(actions),
        "action_text_vector_width": action_text_vector_width,
        "model_sha256": _file_sha256(model_path),
        "warnings": warnings,
        "states": rows,
    }


def write_action_sensitivity_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_onnxruntime() -> Any:
    try:
        import onnxruntime as ort
    except ImportError as exc:  # pragma: no cover - depends on optional install
        raise RuntimeError(
            "onnxruntime is required for Enron action-sensitivity audits"
        ) from exc
    return ort


def _sample_bundle_states(
    bundle: dict[str, Any],
    *,
    max_states: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lens_id, lens_states in sorted((bundle.get("states") or {}).items()):
        for day, state in sorted(lens_states.items()):
            rows.append({"lens": lens_id, **state, "day": day})
    if len(rows) <= max_states:
        return rows
    if max_states <= 1:
        return [rows[-1]]
    return [
        rows[round(index * (len(rows) - 1) / (max_states - 1))]
        for index in range(max_states)
    ]


def _prepare_row(
    *,
    state: dict[str, Any],
    action: EnronAuditAction,
    model: dict[str, Any],
) -> dict[str, Any]:
    token_count = int(model["sequence_token_limit"])
    numeric_width = int(model["sequence_numeric_width"])
    token_categorical = np.asarray(
        state["token_categorical_base"],
        dtype=np.int64,
    ).reshape(token_count, 3)
    token_numeric = np.asarray(
        state["token_numeric_base"],
        dtype=np.float32,
    ).reshape(token_count, numeric_width)
    action_index = int(state.get("action_index", token_count - 1))
    if not 0 <= action_index < token_count:
        raise ValueError(
            f"state {state.get('lens')} {state.get('as_of')} has invalid action_index"
        )
    token_categorical = token_categorical.copy()
    token_numeric = token_numeric.copy()
    token_categorical[action_index, 0] = _safe_index("branch", PHASE_VALUES)
    token_categorical[action_index, 1] = _event_type_index(action.event_type, model)
    token_categorical[action_index, 2] = _safe_index(
        action.recipient_scope,
        RECIPIENT_SCOPE_VALUES,
    )
    token_numeric[action_index, : len(_action_token_numeric(action))] = (
        _action_token_numeric(action)
    )
    return {
        "summary": np.asarray(state["summary"], dtype=np.float32),
        "action": _encode_action(action, model=model),
        "token_categorical": token_categorical,
        "token_numeric": token_numeric,
    }


def _build_feeds(
    rows: Sequence[dict[str, Any]], *, model: dict[str, Any]
) -> dict[str, Any]:
    return {
        "summary": np.stack([row["summary"] for row in rows]).astype(np.float32),
        "action": np.stack([row["action"] for row in rows]).astype(np.float32),
        "token_categorical": np.stack(
            [row["token_categorical"] for row in rows],
        ).astype(np.int64),
        "token_numeric": np.stack([row["token_numeric"] for row in rows]).astype(
            np.float32
        ),
    }


def _encode_action(action: EnronAuditAction, *, model: dict[str, Any]) -> np.ndarray:
    values: list[float] = []
    values.extend(_one_hot(action.recipient_scope, RECIPIENT_SCOPE_VALUES))
    values.extend(_one_hot(action.attachment_policy, ATTACHMENT_POLICY_VALUES))
    values.extend(_one_hot(action.escalation_level, ESCALATION_LEVEL_VALUES))
    values.extend(_one_hot(action.owner_clarity, OWNER_CLARITY_VALUES))
    values.extend(_one_hot(action.reassurance_style, REASSURANCE_STYLE_VALUES))
    values.extend(_one_hot(action.review_path, REVIEW_PATH_VALUES))
    values.extend(_one_hot(action.coordination_breadth, COORDINATION_BREADTH_VALUES))
    values.extend(
        _one_hot(action.outside_sharing_posture, OUTSIDE_SHARING_POSTURE_VALUES)
    )
    values.extend(_one_hot(action.decision_posture, DECISION_POSTURE_VALUES))
    values.extend(
        [
            float(action.external_recipient_count) / 5.0,
            float(action.hold_required),
            float(action.legal_review_required),
            float(action.trading_review_required),
        ]
    )
    tag_names = list(model.get("action_tag_names") or [])
    tag_values = np.zeros(len(tag_names), dtype=np.float32)
    tag_index = {tag: index for index, tag in enumerate(tag_names)}
    for tag in action.action_tags:
        index = tag_index.get(tag)
        if index is not None:
            tag_values[index] = 1.0
    values.extend(tag_values.tolist())
    values.extend(
        _signed_text_vector(
            action.action_text,
            width=int(model.get("action_text_vector_width") or 0),
        ).tolist()
    )
    return np.asarray(values, dtype=np.float32)


def _action_token_numeric(action: EnronAuditAction) -> np.ndarray:
    return np.asarray(
        [
            float(action.external_recipient_count) / 5.0,
            float(action.hold_required),
            float(action.legal_review_required),
            float(action.trading_review_required),
            _safe_index(action.review_path, REVIEW_PATH_VALUES)
            / max(len(REVIEW_PATH_VALUES) - 1, 1),
            _safe_index(action.coordination_breadth, COORDINATION_BREADTH_VALUES)
            / max(len(COORDINATION_BREADTH_VALUES) - 1, 1),
            _safe_index(
                action.outside_sharing_posture,
                OUTSIDE_SHARING_POSTURE_VALUES,
            )
            / max(len(OUTSIDE_SHARING_POSTURE_VALUES) - 1, 1),
            _safe_index(action.decision_posture, DECISION_POSTURE_VALUES)
            / max(len(DECISION_POSTURE_VALUES) - 1, 1),
            _safe_index(action.reassurance_style, REASSURANCE_STYLE_VALUES) / 3.0,
            _safe_index(action.owner_clarity, OWNER_CLARITY_VALUES) / 3.0,
            float(action.external_recipient_count > 0),
            float(len(action.action_tags)) / 6.0,
        ],
        dtype=np.float32,
    )


def _decode_business(values: np.ndarray, model: dict[str, Any]) -> dict[str, float]:
    mean = np.asarray(model["business_mean"], dtype=np.float32)
    std = np.asarray(model["business_std"], dtype=np.float32)
    decoded = np.clip((values * std) + mean, 0.0, 1.0)
    return {
        name: float(decoded[index]) for index, name in enumerate(BUSINESS_TARGET_NAMES)
    }


def _decode_future(values: np.ndarray, model: dict[str, Any]) -> dict[str, float]:
    mean = np.asarray(model["future_state_mean"], dtype=np.float32)
    std = np.asarray(model["future_state_std"], dtype=np.float32)
    decoded = np.clip((values * std) + mean, 0.0, 1.0)
    return {
        name: float(decoded[index])
        for index, name in enumerate(FUTURE_STATE_TARGET_NAMES)
    }


def _balanced_business_readout(business: dict[str, float]) -> float:
    total = 0.0
    for name in BUSINESS_TARGET_NAMES:
        value = float(business.get(name, 0.0))
        total += 1.0 - value if name in NEGATIVE_HEADS else value
    return total / len(BUSINESS_TARGET_NAMES)


def _warnings(
    *,
    model: dict[str, Any],
    max_score_spread: float,
    median_score_spread: float,
    passing_state_fraction: float,
    min_score_spread: float,
    min_passing_state_fraction: float,
) -> list[str]:
    warnings: list[str] = []
    if min_score_spread > 0 and int(model.get("action_text_vector_width") or 0) <= 0:
        warnings.append(
            "model metadata has action_text_vector_width=0; free-text action wording "
            "does not reach the ONNX action vector"
        )
    if max_score_spread < min_score_spread:
        warnings.append(
            f"max action-conditioned score spread {max_score_spread:.6f} is below "
            f"required threshold {min_score_spread:.6f}"
        )
    if median_score_spread < min_score_spread:
        warnings.append(
            f"median action-conditioned score spread {median_score_spread:.6f} is "
            f"below required threshold {min_score_spread:.6f}"
        )
    if passing_state_fraction < min_passing_state_fraction:
        warnings.append(
            f"only {passing_state_fraction:.1%} of audited states meet the spread "
            f"threshold; required {min_passing_state_fraction:.1%}"
        )
    return warnings


def _signed_text_vector(text: str, *, width: int) -> np.ndarray:
    if width <= 0:
        return np.zeros(0, dtype=np.float32)
    tokens = [
        token
        for token in "".join(ch.lower() if ch.isalnum() else " " for ch in text).split()
        if len(token) > 2
    ][:512]
    values = np.zeros(width, dtype=np.float32)
    if not tokens:
        return values
    scale = 1.0 / max(len(tokens) ** 0.5, 1.0)
    for token in tokens:
        digest = sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % width
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        values[index] += sign * scale
    norm = float(np.linalg.norm(values))
    if norm > 1e-6:
        values = values / norm
    return values.astype(np.float32)


def _one_hot(value: str, allowed: Sequence[str]) -> list[float]:
    return [1.0 if value == item else 0.0 for item in allowed]


def _safe_index(value: str, allowed: Sequence[str]) -> int:
    try:
        return list(allowed).index(value)
    except ValueError:
        return 0


def _event_type_index(value: str, model: dict[str, Any]) -> int:
    return _safe_index(value, list(model.get("event_type_names") or []))


def _round_mapping(values: dict[str, float]) -> dict[str, float]:
    return {key: round(float(value), 6) for key, value in values.items()}


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_bundle(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def model_path_for_bundle(bundle_path: Path, bundle: dict[str, Any]) -> Path:
    model_rel = str((bundle.get("model") or {}).get("onnx_path") or "jepa_model.onnx")
    return (bundle_path.parent / model_rel).resolve()


def iter_report_failures(report: dict[str, Any]) -> Iterable[str]:
    if report.get("status") != "pass":
        yield from (str(item) for item in report.get("warnings") or [])
