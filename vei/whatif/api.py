from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from vei.blueprint.api import create_world_session_from_blueprint
from vei.blueprint.models import BlueprintAsset
from vei.context.api import ingest_mail_archive_threads
from vei.data.models import BaseEvent, DatasetMetadata, VEIDataset
from vei.llm import providers
from vei.twin import load_customer_twin
from vei.twin.api import build_customer_twin
from vei.twin.models import ContextMoldConfig

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover

    def load_dotenv(*args: object, **kwargs: object) -> None:
        return None


from .models import (
    WhatIfActorImpact,
    WhatIfActorProfile,
    WhatIfArtifactFlags,
    WhatIfConsequence,
    WhatIfEpisodeManifest,
    WhatIfEpisodeMaterialization,
    WhatIfEvent,
    WhatIfForecast,
    WhatIfForecastDelta,
    WhatIfForecastResult,
    WhatIfExperimentArtifacts,
    WhatIfExperimentMode,
    WhatIfExperimentResult,
    WhatIfReplaySummary,
    WhatIfInterventionSpec,
    WhatIfLLMGeneratedMessage,
    WhatIfLLMReplayResult,
    WhatIfLLMUsage,
    WhatIfResult,
    WhatIfScenario,
    WhatIfScenarioId,
    WhatIfThreadImpact,
    WhatIfThreadSummary,
    WhatIfWorld,
    WhatIfWorldSummary,
)

_ENRON_DOMAIN = "enron.com"
_CONTENT_NOTICE = (
    "Historical email bodies are built from Rosetta excerpts and event metadata. "
    "They are grounded, but they are not full original messages."
)
_SUPPORTED_SCENARIOS: dict[str, WhatIfScenario] = {
    "compliance_gateway": WhatIfScenario(
        scenario_id="compliance_gateway",
        title="Compliance Gateway",
        description=(
            "Threads touching both legal and trading signals require review before "
            "forwarding or escalation."
        ),
        decision_branches=[
            "Block flagged threads until compliance clears them.",
            "Allow them through but log them for post-hoc audit.",
        ],
    ),
    "escalation_firewall": WhatIfScenario(
        scenario_id="escalation_firewall",
        title="Escalation Firewall",
        description=(
            "Direct escalations to senior executives require a department-head gate."
        ),
        decision_branches=[
            "Require sign-off before executive escalation.",
            "Allow direct escalation but flag the thread for governance review.",
        ],
    ),
    "external_dlp": WhatIfScenario(
        scenario_id="external_dlp",
        title="External Sharing DLP",
        description=(
            "Messages with attachment references to outside recipients are held for "
            "review."
        ),
        decision_branches=[
            "Hold the message until DLP review clears it.",
            "Allow send but retain a mandatory audit trail.",
        ],
    ),
    "approval_chain_enforcement": WhatIfScenario(
        scenario_id="approval_chain_enforcement",
        title="Approval Chain Enforcement",
        description=(
            "Assignment-heavy threads require explicit approval before the next "
            "handoff proceeds."
        ),
        decision_branches=[
            "Stop handoffs until an approval is recorded.",
            "Allow handoff but mark the thread out of policy.",
        ],
    ),
}
_EXECUTIVE_MARKERS = ("skilling", "lay", "fastow", "kean")


def list_supported_scenarios() -> list[WhatIfScenario]:
    return list(_SUPPORTED_SCENARIOS.values())


def load_world(
    *,
    source: str,
    rosetta_dir: str | Path,
    time_window: tuple[str, str] | None = None,
    custodian_filter: Sequence[str] | None = None,
    max_events: int | None = None,
) -> WhatIfWorld:
    normalized_source = source.strip().lower()
    if normalized_source != "enron":
        raise ValueError(f"unsupported what-if source: {source}")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise RuntimeError(
            "pyarrow is required for `vei whatif` parquet loading"
        ) from exc

    base = Path(rosetta_dir).expanduser().resolve()
    metadata_path = base / "enron_rosetta_events_metadata.parquet"
    content_path = base / "enron_rosetta_events_content.parquet"
    if not metadata_path.exists():
        raise ValueError(f"metadata parquet not found: {metadata_path}")
    if not content_path.exists():
        raise ValueError(f"content parquet not found: {content_path}")

    metadata_rows = pq.read_table(
        metadata_path,
        columns=[
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "thread_task_id",
            "artifacts",
        ],
    ).to_pylist()
    content_rows = pq.read_table(
        content_path,
        columns=["event_id", "content"],
    ).to_pylist()
    content_by_id = {
        str(row.get("event_id", "")): str(row.get("content", "") or "")
        for row in content_rows
    }

    time_bounds = _resolve_time_window(time_window)
    custodian_tokens = {item.strip().lower() for item in custodian_filter or [] if item}
    events: list[WhatIfEvent] = []
    for row in metadata_rows:
        event = _build_event(row, content_by_id.get(str(row.get("event_id", "")), ""))
        if event is None:
            continue
        if time_bounds is not None and not (
            time_bounds[0] <= event.timestamp_ms <= time_bounds[1]
        ):
            continue
        if custodian_tokens and not _matches_custodian_filter(event, custodian_tokens):
            continue
        events.append(event)

    events.sort(key=lambda item: (item.timestamp_ms, item.event_id))
    if max_events is not None:
        events = events[: max(0, int(max_events))]

    threads = _build_thread_summaries(events)
    actors = _build_actor_profiles(events)
    summary = WhatIfWorldSummary(
        source="enron",
        event_count=len(events),
        thread_count=len(threads),
        actor_count=len(actors),
        custodian_count=len(
            {
                custodian
                for actor in actors
                for custodian in actor.custodian_ids
                if custodian
            }
        ),
        first_timestamp=events[0].timestamp if events else "",
        last_timestamp=events[-1].timestamp if events else "",
        event_type_counts=dict(Counter(event.event_type for event in events)),
        key_actor_ids=[actor.actor_id for actor in actors[:5]],
    )
    return WhatIfWorld(
        source="enron",
        rosetta_dir=base,
        summary=summary,
        scenarios=list_supported_scenarios(),
        actors=actors,
        threads=threads,
        events=events,
        metadata={"content_notice": _CONTENT_NOTICE},
    )


def run_whatif(
    world: WhatIfWorld,
    *,
    scenario: str | None = None,
    prompt: str | None = None,
) -> WhatIfResult:
    resolved = _resolve_scenario(scenario=scenario, prompt=prompt)
    thread_by_id = {thread.thread_id: thread for thread in world.threads}
    matched_events = _matched_events_for_scenario(
        world.events, thread_by_id, resolved.scenario_id
    )
    matched_thread_ids = sorted(
        {event.thread_id for event in matched_events if event.thread_id}
    )
    matched_actor_ids = sorted(
        {
            actor_id
            for event in matched_events
            for actor_id in {event.actor_id, event.target_id}
            if actor_id
        }
    )
    actor_impacts = _build_actor_impacts(matched_events)
    thread_impacts = _build_thread_impacts(
        matched_events, thread_by_id, resolved.scenario_id
    )
    consequences = _build_consequences(thread_impacts, actor_impacts)

    blocked_forward_count = sum(1 for event in matched_events if event.flags.is_forward)
    blocked_escalation_count = sum(
        1
        for event in matched_events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    delayed_assignment_count = sum(
        1 for event in matched_events if event.event_type == "assignment"
    )

    return WhatIfResult(
        scenario=resolved,
        prompt=prompt,
        world_summary=world.summary,
        matched_event_count=len(matched_events),
        affected_thread_count=len(matched_thread_ids),
        affected_actor_count=len(matched_actor_ids),
        blocked_forward_count=blocked_forward_count,
        blocked_escalation_count=blocked_escalation_count,
        delayed_assignment_count=delayed_assignment_count,
        timeline_impact=_timeline_impact(resolved.scenario_id, matched_events),
        top_actors=actor_impacts[:5],
        top_threads=thread_impacts[:5],
        top_consequences=consequences[:5],
        decision_branches=list(resolved.decision_branches),
    )


def materialize_episode(
    world: WhatIfWorld,
    *,
    root: str | Path,
    thread_id: str,
    event_id: str | None = None,
    organization_name: str = "Enron Corporation",
    organization_domain: str = _ENRON_DOMAIN,
) -> WhatIfEpisodeMaterialization:
    workspace_root = Path(root).expanduser().resolve()
    thread_events = _thread_events(world.events, thread_id)
    if not thread_events:
        raise ValueError(f"thread not found in world: {thread_id}")

    branch_event = _choose_branch_event(thread_events, requested_event_id=event_id)
    past_events = [
        event
        for event in thread_events
        if event.timestamp_ms <= branch_event.timestamp_ms
    ]
    future_events = [
        event
        for event in thread_events
        if event.timestamp_ms > branch_event.timestamp_ms
    ]
    if past_events and past_events[-1].event_id != branch_event.event_id:
        past_events.append(branch_event)
    thread_subject = _thread_subject(
        world.threads, thread_id, fallback=branch_event.subject
    )

    archive_threads = [
        {
            "thread_id": thread_id,
            "subject": thread_subject,
            "category": "historical",
            "messages": [
                _archive_message_payload(event, base_time_ms=index * 1000)
                for index, event in enumerate(past_events)
            ],
        }
    ]
    actor_payload = [
        {
            "actor_id": actor.actor_id,
            "email": actor.email,
            "display_name": actor.display_name,
        }
        for actor in world.actors
        if actor.actor_id
        in {
            value
            for event in thread_events
            for value in {event.actor_id, event.target_id}
            if value
        }
    ]
    snapshot = ingest_mail_archive_threads(
        archive_threads,
        organization_name=organization_name,
        organization_domain=organization_domain,
        actors=actor_payload,
        metadata={
            "whatif": {
                "source": world.source,
                "thread_id": thread_id,
                "branch_event_id": branch_event.event_id,
                "content_notice": _CONTENT_NOTICE,
            }
        },
    )
    bundle = build_customer_twin(
        workspace_root,
        snapshot=snapshot,
        organization_name=organization_name,
        organization_domain=organization_domain,
        mold=ContextMoldConfig(
            archetype="b2b_saas",
            density_level="medium",
            named_team_expansion="minimal",
            included_surfaces=["mail", "identity"],
            synthetic_expansion_strength="light",
        ),
        overwrite=True,
    )
    baseline_dataset = _baseline_dataset(
        thread_subject=thread_subject,
        branch_event=branch_event,
        future_events=future_events,
    )
    baseline_dataset_path = workspace_root / "whatif_baseline_dataset.json"
    baseline_dataset_path.write_text(
        baseline_dataset.model_dump_json(indent=2),
        encoding="utf-8",
    )
    forecast = forecast_episode(future_events)
    manifest = WhatIfEpisodeManifest(
        source=world.source,
        source_dir=world.rosetta_dir,
        workspace_root=workspace_root,
        organization_name=organization_name,
        organization_domain=organization_domain,
        thread_id=thread_id,
        thread_subject=thread_subject,
        branch_event_id=branch_event.event_id,
        branch_timestamp=branch_event.timestamp,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        baseline_dataset_path=str(baseline_dataset_path.relative_to(workspace_root)),
        content_notice=_CONTENT_NOTICE,
        actor_ids=sorted(
            {
                actor_id
                for event in thread_events
                for actor_id in {event.actor_id, event.target_id}
                if actor_id
            }
        ),
        forecast=forecast,
    )
    manifest_path = workspace_root / "whatif_episode_manifest.json"
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return WhatIfEpisodeMaterialization(
        manifest_path=manifest_path,
        bundle_path=workspace_root / "twin_manifest.json",
        context_snapshot_path=workspace_root / bundle.context_snapshot_path,
        baseline_dataset_path=baseline_dataset_path,
        workspace_root=workspace_root,
        organization_name=organization_name,
        organization_domain=organization_domain,
        thread_id=thread_id,
        branch_event_id=branch_event.event_id,
        history_message_count=len(past_events),
        future_event_count=len(future_events),
        forecast=forecast,
    )


def load_episode_manifest(root: str | Path) -> WhatIfEpisodeManifest:
    workspace_root = Path(root).expanduser().resolve()
    manifest_path = workspace_root / "whatif_episode_manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"what-if episode manifest not found: {manifest_path}")
    return WhatIfEpisodeManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )


def replay_episode_baseline(
    root: str | Path,
    *,
    tick_ms: int = 0,
    seed: int = 42042,
) -> WhatIfReplaySummary:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    bundle = load_customer_twin(workspace_root)
    asset_path = workspace_root / bundle.blueprint_asset_path
    dataset_path = workspace_root / manifest.baseline_dataset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    session = create_world_session_from_blueprint(asset, seed=seed)
    replay_result = session.replay(mode="overlay", dataset_events=dataset.events)

    delivered_event_count = 0
    current_time_ms = session.router.bus.clock_ms
    pending_events = session.pending()
    if tick_ms > 0:
        tick_result = session.router.tick(dt_ms=tick_ms)
        delivered_event_count = sum(tick_result.get("delivered", {}).values())
        current_time_ms = int(tick_result.get("time_ms", current_time_ms))
        pending_events = dict(tick_result.get("pending", {}))

    inbox = session.call_tool("mail.list", {})
    top_subjects = [
        str(item.get("subj", ""))
        for item in inbox[:5]
        if isinstance(item, dict) and item.get("subj")
    ]
    return WhatIfReplaySummary(
        workspace_root=workspace_root,
        baseline_dataset_path=dataset_path,
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=delivered_event_count,
        current_time_ms=current_time_ms,
        pending_events=pending_events,
        inbox_count=len(inbox),
        top_subjects=top_subjects,
        forecast=manifest.forecast,
    )


def forecast_episode(events: Sequence[WhatIfEvent]) -> WhatIfForecast:
    future_event_count = len(events)
    future_escalation_count = sum(
        1
        for event in events
        if event.flags.is_escalation or event.event_type == "escalation"
    )
    future_assignment_count = sum(
        1 for event in events if event.event_type == "assignment"
    )
    future_approval_count = sum(1 for event in events if event.event_type == "approval")
    future_external_event_count = sum(
        1 for event in events if _has_external_recipients(event.flags.to_recipients)
    )
    risk_score = min(
        1.0,
        (
            (future_escalation_count * 0.25)
            + (future_assignment_count * 0.15)
            + (future_external_event_count * 0.2)
            + max(0, future_event_count - future_approval_count) * 0.02
        ),
    )
    summary = (
        f"{future_event_count} future events remain, including "
        f"{future_escalation_count} escalations and {future_external_event_count} "
        "externally addressed messages."
    )
    return WhatIfForecast(
        future_event_count=future_event_count,
        future_escalation_count=future_escalation_count,
        future_assignment_count=future_assignment_count,
        future_approval_count=future_approval_count,
        future_external_event_count=future_external_event_count,
        risk_score=round(risk_score, 3),
        summary=summary,
    )


def run_llm_counterfactual(
    root: str | Path,
    *,
    prompt: str,
    provider: str = "openai",
    model: str = "gpt-5",
    seed: int = 42042,
) -> WhatIfLLMReplayResult:
    load_dotenv(override=True)
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    context = _load_episode_context(workspace_root)
    session = _session_for_episode(workspace_root, seed=seed)
    allowed_actors, allowed_recipients = _allowed_thread_participants(
        context=context,
        manifest=manifest,
    )
    recipient_scope, recipient_notes = _apply_recipient_scope(
        allowed_recipients,
        tags=_intervention_tags(prompt),
    )
    system = (
        "You are simulating a bounded counterfactual continuation on a historical "
        "enterprise email thread. Return strict JSON with keys tool and args. "
        "Use tool='emit_counterfactual'. In args, include summary, notes, and "
        "messages. messages must be a list of 1 to 3 objects with actor_id, to, "
        "subject, body_text, delay_ms, rationale. Only use the listed actors and "
        "recipient addresses. Keep messages plausible, concise, and clearly tied "
        "to the intervention prompt."
    )
    user = _llm_counterfactual_prompt(
        context=context,
        manifest=manifest,
        prompt=prompt,
        allowed_actors=allowed_actors,
        allowed_recipients=recipient_scope,
    )
    try:
        response = asyncio.run(
            providers.plan_once_with_usage(
                provider=provider,
                model=model,
                system=system,
                user=user,
                timeout_s=90,
            )
        )
        messages, notes = _normalize_llm_messages(
            _counterfactual_args(response.plan),
            manifest=manifest,
            allowed_actors=allowed_actors,
            allowed_recipients=recipient_scope,
        )
        if not messages:
            raise ValueError("LLM returned no usable messages")
    except Exception as exc:  # noqa: BLE001
        return WhatIfLLMReplayResult(
            status="error",
            provider=provider,
            model=model,
            prompt=prompt,
            summary="LLM counterfactual generation failed.",
            error=str(exc),
            notes=["The forecast path can still be used without live LLM output."],
        )

    max_delay = max(message.delay_ms for message in messages)
    replay_result = session.replay(
        mode="overlay",
        dataset_events=[
            BaseEvent(
                time_ms=message.delay_ms,
                actor_id=message.actor_id,
                channel="mail",
                type="counterfactual_email",
                correlation_id=manifest.thread_id,
                payload={
                    "from": message.actor_id,
                    "to": message.to,
                    "subj": message.subject,
                    "body_text": message.body_text,
                    "thread_id": manifest.thread_id,
                    "category": "counterfactual",
                },
            )
            for message in messages
        ],
    )
    tick_result = session.router.tick(dt_ms=max_delay + 1000)
    inbox = session.call_tool("mail.list", {})
    top_subjects = [
        str(item.get("subj", ""))
        for item in inbox[:5]
        if isinstance(item, dict) and item.get("subj")
    ]
    plan_args = _counterfactual_args(response.plan)
    summary = str(plan_args.get("summary", "") or "").strip()
    if not summary:
        summary = (
            f"{len(messages)} counterfactual messages were generated across "
            f"{len({message.actor_id for message in messages})} participants."
        )
    return WhatIfLLMReplayResult(
        status="ok",
        provider=provider,
        model=model,
        prompt=prompt,
        summary=summary,
        messages=messages,
        usage=WhatIfLLMUsage(
            provider=response.usage.provider,
            model=response.usage.model,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens,
            estimated_cost_usd=response.usage.estimated_cost_usd,
        ),
        scheduled_event_count=int(replay_result.get("scheduled", 0)),
        delivered_event_count=sum(tick_result.get("delivered", {}).values()),
        inbox_count=len(inbox),
        top_subjects=top_subjects,
        notes=recipient_notes + notes + _counterfactual_notes(plan_args),
    )


def run_ejepa_proxy_counterfactual(
    root: str | Path,
    *,
    prompt: str,
) -> WhatIfForecastResult:
    workspace_root = Path(root).expanduser().resolve()
    manifest = load_episode_manifest(workspace_root)
    baseline = manifest.forecast.model_copy(deep=True)
    predicted = manifest.forecast.model_copy(
        update={"backend": "e_jepa_proxy"},
        deep=True,
    )
    tags = _intervention_tags(prompt)
    notes: list[str] = []

    event_shift = 0
    escalation_shift = 0
    assignment_shift = 0
    approval_shift = 0
    external_shift = 0
    risk_shift = 0.0

    if {"legal", "compliance"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.18
        notes.append("Compliance involvement reduces uncontrolled escalation.")
    if {"hold", "pause_forward"} & tags:
        external_shift -= max(1, predicted.future_external_event_count)
        event_shift -= max(0, predicted.future_event_count // 3)
        risk_shift -= 0.2
        notes.append("Holding or pausing the thread cuts external exposure.")
    if {"reply_immediately", "clarify_owner"} & tags:
        event_shift -= 1
        assignment_shift -= max(0, predicted.future_assignment_count // 2)
        risk_shift -= 0.12
        notes.append("Fast clarification usually shortens the follow-up tail.")
    if {"executive_gate"} & tags:
        escalation_shift -= max(1, predicted.future_escalation_count // 2)
        approval_shift += 1
        risk_shift -= 0.14
        notes.append("Routing through an executive gate lowers escalation spread.")
    if {"attachment_removed", "external_removed"} & tags:
        external_shift -= max(1, predicted.future_external_event_count)
        risk_shift -= 0.24
        notes.append("Removing the external recipient sharply lowers leak risk.")

    predicted.future_event_count = max(0, predicted.future_event_count + event_shift)
    predicted.future_escalation_count = max(
        0,
        predicted.future_escalation_count + escalation_shift,
    )
    predicted.future_assignment_count = max(
        0,
        predicted.future_assignment_count + assignment_shift,
    )
    predicted.future_approval_count = max(
        0,
        predicted.future_approval_count + approval_shift,
    )
    predicted.future_external_event_count = max(
        0,
        predicted.future_external_event_count + external_shift,
    )
    predicted.risk_score = round(
        max(0.0, min(1.0, predicted.risk_score + risk_shift)),
        3,
    )
    predicted.summary = _forecast_summary_from_counts(predicted)

    delta = WhatIfForecastDelta(
        risk_score_delta=round(predicted.risk_score - baseline.risk_score, 3),
        future_event_delta=predicted.future_event_count - baseline.future_event_count,
        escalation_delta=(
            predicted.future_escalation_count - baseline.future_escalation_count
        ),
        assignment_delta=(
            predicted.future_assignment_count - baseline.future_assignment_count
        ),
        approval_delta=predicted.future_approval_count - baseline.future_approval_count,
        external_event_delta=(
            predicted.future_external_event_count - baseline.future_external_event_count
        ),
    )
    return WhatIfForecastResult(
        status="ok",
        backend="e_jepa_proxy",
        prompt=prompt,
        summary=_forecast_delta_summary(delta),
        baseline=baseline,
        predicted=predicted,
        delta=delta,
        notes=notes
        or [
            "No specific intervention tags were detected; forecast remained close to baseline."
        ],
    )


def run_counterfactual_experiment(
    world: WhatIfWorld,
    *,
    artifacts_root: str | Path,
    label: str,
    counterfactual_prompt: str,
    selection_scenario: str | None = None,
    selection_prompt: str | None = None,
    thread_id: str | None = None,
    mode: WhatIfExperimentMode = "both",
    provider: str = "openai",
    model: str = "gpt-5",
    seed: int = 42042,
) -> WhatIfExperimentResult:
    selection = run_whatif(
        world,
        scenario=selection_scenario,
        prompt=selection_prompt,
    )
    selected_thread_id = thread_id or (
        selection.top_threads[0].thread_id if selection.top_threads else None
    )
    if not selected_thread_id:
        raise ValueError("no matching thread available for the counterfactual run")

    root = Path(artifacts_root).expanduser().resolve() / _slug(label)
    workspace_root = root / "workspace"
    materialization = materialize_episode(
        world,
        root=workspace_root,
        thread_id=selected_thread_id,
    )
    baseline = replay_episode_baseline(
        workspace_root,
        tick_ms=_baseline_tick_ms(materialization.baseline_dataset_path),
        seed=seed,
    )
    llm_result: WhatIfLLMReplayResult | None = None
    if mode in {"llm", "both"}:
        llm_result = run_llm_counterfactual(
            workspace_root,
            prompt=counterfactual_prompt,
            provider=provider,
            model=model,
            seed=seed,
        )
    forecast_result: WhatIfForecastResult | None = None
    if mode in {"e_jepa_proxy", "both"}:
        forecast_result = run_ejepa_proxy_counterfactual(
            workspace_root,
            prompt=counterfactual_prompt,
        )

    result_path = root / "whatif_experiment_result.json"
    overview_path = root / "whatif_experiment_overview.md"
    llm_path = root / "whatif_llm_result.json" if llm_result is not None else None
    forecast_path = (
        root / "whatif_ejepa_proxy_result.json" if forecast_result is not None else None
    )
    root.mkdir(parents=True, exist_ok=True)

    artifacts = WhatIfExperimentArtifacts(
        root=root,
        result_json_path=result_path,
        overview_markdown_path=overview_path,
        llm_json_path=llm_path,
        forecast_json_path=forecast_path,
    )
    result = WhatIfExperimentResult(
        mode=mode,
        label=label,
        intervention=WhatIfInterventionSpec(
            label=label,
            prompt=counterfactual_prompt,
            objective=(
                selection.scenario.description
                if selection.scenario.description
                else "counterfactual replay"
            ),
            scenario_id=selection.scenario.scenario_id,
            thread_id=selected_thread_id,
            branch_event_id=materialization.branch_event_id,
        ),
        selection=selection,
        materialization=materialization,
        baseline=baseline,
        llm_result=llm_result,
        forecast_result=forecast_result,
        artifacts=artifacts,
    )
    result_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    if llm_result is not None and llm_path is not None:
        llm_path.write_text(llm_result.model_dump_json(indent=2), encoding="utf-8")
    if forecast_result is not None and forecast_path is not None:
        forecast_path.write_text(
            forecast_result.model_dump_json(indent=2),
            encoding="utf-8",
        )
    overview_path.write_text(
        _render_experiment_overview(result),
        encoding="utf-8",
    )
    return result


def load_experiment_result(root: str | Path) -> WhatIfExperimentResult:
    result_path = Path(root).expanduser().resolve() / "whatif_experiment_result.json"
    if not result_path.exists():
        raise ValueError(f"what-if experiment result not found: {result_path}")
    return WhatIfExperimentResult.model_validate_json(
        result_path.read_text(encoding="utf-8")
    )


def _build_event(row: dict[str, Any], content: str) -> WhatIfEvent | None:
    event_id = str(row.get("event_id", "")).strip()
    if not event_id:
        return None
    timestamp = row.get("timestamp")
    timestamp_ms = _timestamp_to_ms(timestamp)
    timestamp_text = _timestamp_to_text(timestamp)
    artifacts = _artifact_flags(row.get("artifacts"))
    thread_id = str(row.get("thread_task_id", "") or event_id)
    subject = artifacts.subject or artifacts.norm_subject or thread_id
    return WhatIfEvent(
        event_id=event_id,
        timestamp=timestamp_text,
        timestamp_ms=timestamp_ms,
        actor_id=str(row.get("actor_id", "") or ""),
        target_id=str(row.get("target_id", "") or ""),
        event_type=str(row.get("event_type", "") or ""),
        thread_id=thread_id,
        subject=subject,
        snippet=str(content or ""),
        flags=artifacts,
    )


def _artifact_flags(raw: Any) -> WhatIfArtifactFlags:
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
    elif isinstance(raw, dict):
        payload = dict(raw)
    else:
        payload = {}

    return WhatIfArtifactFlags(
        consult_legal_specialist=bool(payload.get("consult_legal_specialist", False)),
        consult_trading_specialist=bool(
            payload.get("consult_trading_specialist", False)
        ),
        has_attachment_reference=bool(payload.get("has_attachment_reference", False)),
        is_escalation=bool(payload.get("is_escalation", False)),
        is_forward=bool(payload.get("is_forward", False)),
        is_reply=bool(payload.get("is_reply", False)),
        cc_count=_safe_int(payload.get("cc_count")),
        bcc_count=_safe_int(payload.get("bcc_count")),
        to_count=_safe_int(payload.get("to_count")),
        to_recipients=_string_list(payload.get("to_recipients")),
        cc_recipients=_string_list(payload.get("cc_recipients")),
        subject=str(payload.get("subject", "") or ""),
        norm_subject=str(payload.get("norm_subject", "") or ""),
        body_sha1=str(payload.get("body_sha1", "") or ""),
        custodian_id=str(payload.get("custodian_id", "") or ""),
        message_id=str(payload.get("message_id", "") or ""),
        folder=str(payload.get("folder", "") or ""),
        source=str(payload.get("source", "") or ""),
    )


def _build_thread_summaries(events: Sequence[WhatIfEvent]) -> list[WhatIfThreadSummary]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = buckets.setdefault(
            event.thread_id,
            {
                "thread_id": event.thread_id,
                "subject": event.subject or event.thread_id,
                "event_count": 0,
                "actor_ids": set(),
                "first_timestamp": event.timestamp,
                "last_timestamp": event.timestamp,
                "legal_event_count": 0,
                "trading_event_count": 0,
                "escalation_event_count": 0,
                "assignment_event_count": 0,
                "approval_event_count": 0,
                "forward_event_count": 0,
                "attachment_event_count": 0,
                "external_recipient_event_count": 0,
                "event_type_counts": Counter(),
            },
        )
        bucket["event_count"] += 1
        bucket["actor_ids"].add(event.actor_id)
        if event.target_id:
            bucket["actor_ids"].add(event.target_id)
        bucket["last_timestamp"] = event.timestamp
        if event.flags.consult_legal_specialist:
            bucket["legal_event_count"] += 1
        if event.flags.consult_trading_specialist:
            bucket["trading_event_count"] += 1
        if event.flags.is_escalation or event.event_type == "escalation":
            bucket["escalation_event_count"] += 1
        if event.event_type == "assignment":
            bucket["assignment_event_count"] += 1
        if event.event_type == "approval":
            bucket["approval_event_count"] += 1
        if event.flags.is_forward:
            bucket["forward_event_count"] += 1
        if event.flags.has_attachment_reference:
            bucket["attachment_event_count"] += 1
        if _has_external_recipients(event.flags.to_recipients):
            bucket["external_recipient_event_count"] += 1
        bucket["event_type_counts"][event.event_type] += 1

    threads = [
        WhatIfThreadSummary(
            thread_id=payload["thread_id"],
            subject=payload["subject"],
            event_count=payload["event_count"],
            actor_ids=sorted(actor_id for actor_id in payload["actor_ids"] if actor_id),
            first_timestamp=payload["first_timestamp"],
            last_timestamp=payload["last_timestamp"],
            legal_event_count=payload["legal_event_count"],
            trading_event_count=payload["trading_event_count"],
            escalation_event_count=payload["escalation_event_count"],
            assignment_event_count=payload["assignment_event_count"],
            approval_event_count=payload["approval_event_count"],
            forward_event_count=payload["forward_event_count"],
            attachment_event_count=payload["attachment_event_count"],
            external_recipient_event_count=payload["external_recipient_event_count"],
            event_type_counts=dict(payload["event_type_counts"]),
        )
        for payload in buckets.values()
    ]
    return sorted(threads, key=lambda item: (-item.event_count, item.thread_id))


def _build_actor_profiles(events: Sequence[WhatIfEvent]) -> list[WhatIfActorProfile]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        _touch_actor(
            buckets,
            actor_id=event.actor_id,
            sent=True,
            flagged=_event_is_flagged(event),
            custodian_id=event.flags.custodian_id,
        )
        _touch_actor(
            buckets,
            actor_id=event.target_id,
            received=True,
        )
    actors = [
        WhatIfActorProfile(
            actor_id=actor_id,
            email=actor_id,
            display_name=_display_name(actor_id),
            custodian_ids=sorted(payload["custodian_ids"]),
            event_count=payload["event_count"],
            sent_count=payload["sent_count"],
            received_count=payload["received_count"],
            flagged_event_count=payload["flagged_event_count"],
        )
        for actor_id, payload in buckets.items()
        if actor_id
    ]
    return sorted(actors, key=lambda item: (-item.event_count, item.actor_id))


def _touch_actor(
    buckets: dict[str, dict[str, Any]],
    *,
    actor_id: str,
    sent: bool = False,
    received: bool = False,
    flagged: bool = False,
    custodian_id: str = "",
) -> None:
    if not actor_id:
        return
    bucket = buckets.setdefault(
        actor_id,
        {
            "event_count": 0,
            "sent_count": 0,
            "received_count": 0,
            "flagged_event_count": 0,
            "custodian_ids": set(),
        },
    )
    bucket["event_count"] += 1
    if sent:
        bucket["sent_count"] += 1
    if received:
        bucket["received_count"] += 1
    if flagged:
        bucket["flagged_event_count"] += 1
    if custodian_id:
        bucket["custodian_ids"].add(custodian_id)


def _resolve_scenario(
    *,
    scenario: str | None,
    prompt: str | None,
) -> WhatIfScenario:
    if scenario:
        resolved = _SUPPORTED_SCENARIOS.get(scenario.strip().lower())
        if resolved is None:
            raise ValueError(f"unsupported what-if scenario: {scenario}")
        return resolved
    if not prompt:
        raise ValueError("provide --scenario or --prompt")
    lowered = prompt.strip().lower()
    if "legal" in lowered and "trading" in lowered:
        return _SUPPORTED_SCENARIOS["compliance_gateway"]
    if (
        any(token in lowered for token in ("compliance", "review", "audit"))
        and "thread" in lowered
    ):
        return _SUPPORTED_SCENARIOS["compliance_gateway"]
    if any(
        token in lowered
        for token in ("c-suite", "executive", "skilling", "lay", "fastow", "kean")
    ):
        return _SUPPORTED_SCENARIOS["escalation_firewall"]
    if any(token in lowered for token in ("external", "attachment", "dlp", "outside")):
        return _SUPPORTED_SCENARIOS["external_dlp"]
    if any(
        token in lowered for token in ("approval", "sign-off", "handoff", "assignment")
    ):
        return _SUPPORTED_SCENARIOS["approval_chain_enforcement"]
    supported = ", ".join(sorted(_SUPPORTED_SCENARIOS))
    raise ValueError(f"could not map prompt to a supported scenario ({supported})")


def _matched_events_for_scenario(
    events: Sequence[WhatIfEvent],
    thread_by_id: dict[str, WhatIfThreadSummary],
    scenario_id: WhatIfScenarioId,
) -> list[WhatIfEvent]:
    if scenario_id == "compliance_gateway":
        matched_threads = {
            thread_id
            for thread_id, thread in thread_by_id.items()
            if thread.legal_event_count > 0 and thread.trading_event_count > 0
        }
        return [event for event in events if event.thread_id in matched_threads]

    if scenario_id == "escalation_firewall":
        return [
            event
            for event in events
            if _touches_executive(event)
            and (
                event.flags.is_escalation
                or event.flags.is_forward
                or event.event_type == "escalation"
            )
        ]

    if scenario_id == "external_dlp":
        return [
            event
            for event in events
            if event.flags.has_attachment_reference
            and _has_external_recipients(event.flags.to_recipients)
        ]

    if scenario_id == "approval_chain_enforcement":
        matched_threads = {
            thread_id
            for thread_id, thread in thread_by_id.items()
            if thread.assignment_event_count > 0 and thread.approval_event_count == 0
        }
        return [
            event
            for event in events
            if event.thread_id in matched_threads and event.event_type == "assignment"
        ]

    raise ValueError(f"unsupported what-if scenario: {scenario_id}")


def _build_actor_impacts(events: Sequence[WhatIfEvent]) -> list[WhatIfActorImpact]:
    counts: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = counts.setdefault(
            event.actor_id,
            {"count": 0, "threads": set(), "reasons": set()},
        )
        bucket["count"] += 1
        bucket["threads"].add(event.thread_id)
        bucket["reasons"].update(_event_reason_labels(event))
    impacts = [
        WhatIfActorImpact(
            actor_id=actor_id,
            display_name=_display_name(actor_id),
            affected_event_count=payload["count"],
            affected_thread_count=len(payload["threads"]),
            reasons=sorted(payload["reasons"]),
        )
        for actor_id, payload in counts.items()
        if actor_id
    ]
    return sorted(
        impacts,
        key=lambda item: (-item.affected_event_count, item.actor_id),
    )


def _build_thread_impacts(
    events: Sequence[WhatIfEvent],
    thread_by_id: dict[str, WhatIfThreadSummary],
    scenario_id: WhatIfScenarioId,
) -> list[WhatIfThreadImpact]:
    counts: dict[str, dict[str, Any]] = {}
    for event in events:
        bucket = counts.setdefault(
            event.thread_id,
            {"count": 0, "reasons": set()},
        )
        bucket["count"] += 1
        bucket["reasons"].update(_event_reason_labels(event))
    impacts: list[WhatIfThreadImpact] = []
    for thread_id, payload in counts.items():
        thread = thread_by_id.get(thread_id)
        if thread is None:
            continue
        reasons = sorted(
            payload["reasons"] or _thread_reason_labels(thread, scenario_id)
        )
        impacts.append(
            WhatIfThreadImpact(
                thread_id=thread_id,
                subject=thread.subject,
                affected_event_count=payload["count"],
                participant_count=len(thread.actor_ids),
                reasons=reasons,
            )
        )
    return sorted(
        impacts,
        key=lambda item: (-item.affected_event_count, item.thread_id),
    )


def _build_consequences(
    thread_impacts: Sequence[WhatIfThreadImpact],
    actor_impacts: Sequence[WhatIfActorImpact],
) -> list[WhatIfConsequence]:
    consequences: list[WhatIfConsequence] = []
    for impact in thread_impacts[:3]:
        detail = (
            f"{impact.affected_event_count} events across {impact.participant_count} "
            f"participants would move under the alternate rule."
        )
        consequences.append(
            WhatIfConsequence(
                thread_id=impact.thread_id,
                subject=impact.subject,
                detail=detail,
                severity="high" if impact.affected_event_count >= 3 else "medium",
            )
        )
    for impact in actor_impacts[:2]:
        detail = (
            f"{impact.display_name} appears in {impact.affected_event_count} matched "
            "events and would likely see their thread flow change."
        )
        consequences.append(
            WhatIfConsequence(
                thread_id="",
                subject=impact.display_name,
                actor_id=impact.actor_id,
                detail=detail,
                severity="medium",
            )
        )
    return consequences


def _timeline_impact(
    scenario_id: WhatIfScenarioId,
    events: Sequence[WhatIfEvent],
) -> str:
    if not events:
        return "No historical events matched this rule."
    if scenario_id == "compliance_gateway":
        return "Adds a review gate before forwarding or escalation on matched threads."
    if scenario_id == "escalation_firewall":
        return "Introduces one extra approval hop before executive escalation."
    if scenario_id == "external_dlp":
        return "Holds external attachment sends until review completes."
    return "Requires approval before the next assignment handoff proceeds."


def _thread_events(events: Sequence[WhatIfEvent], thread_id: str) -> list[WhatIfEvent]:
    return [
        event
        for event in sorted(events, key=lambda item: (item.timestamp_ms, item.event_id))
        if event.thread_id == thread_id
    ]


def _choose_branch_event(
    events: Sequence[WhatIfEvent],
    *,
    requested_event_id: str | None,
) -> WhatIfEvent:
    if not events:
        raise ValueError("cannot choose a branch event from an empty thread")
    if requested_event_id:
        for event in events:
            if event.event_id == requested_event_id:
                return event
        raise ValueError(f"branch event not found in thread: {requested_event_id}")
    if len(events) == 1:
        return events[0]
    prioritized = [
        event
        for event in events[:-1]
        if event.flags.is_escalation
        or event.flags.is_forward
        or event.event_type in {"assignment", "approval", "reply"}
    ]
    if prioritized:
        return prioritized[0]
    return events[max(0, (len(events) // 2) - 1)]


def _thread_subject(
    threads: Sequence[WhatIfThreadSummary],
    thread_id: str,
    *,
    fallback: str,
) -> str:
    for thread in threads:
        if thread.thread_id == thread_id:
            return thread.subject
    return fallback or thread_id


def _archive_message_payload(
    event: WhatIfEvent,
    *,
    base_time_ms: int,
) -> dict[str, Any]:
    recipient = _primary_recipient(event)
    return {
        "from": event.actor_id or "unknown@enron.com",
        "to": recipient,
        "subject": event.subject or event.thread_id,
        "body_text": _historical_body(event),
        "unread": False,
        "time_ms": base_time_ms,
    }


def _baseline_dataset(
    *,
    thread_subject: str,
    branch_event: WhatIfEvent,
    future_events: Sequence[WhatIfEvent],
) -> VEIDataset:
    baseline_events: list[BaseEvent] = []
    for event in future_events:
        delay_ms = max(1, event.timestamp_ms - branch_event.timestamp_ms)
        baseline_events.append(
            BaseEvent(
                time_ms=delay_ms,
                actor_id=event.actor_id,
                channel="mail",
                type=event.event_type,
                correlation_id=event.thread_id,
                payload={
                    "from": event.actor_id or "unknown@enron.com",
                    "to": _primary_recipient(event),
                    "subj": event.subject or thread_subject,
                    "body_text": _historical_body(event),
                    "thread_id": event.thread_id,
                    "category": "historical",
                },
            )
        )
    return VEIDataset(
        metadata=DatasetMetadata(
            name=f"whatif-baseline-{branch_event.thread_id}",
            description="Historical future events scheduled after the branch point.",
            tags=["whatif", "baseline", "historical"],
            source="enron_rosetta",
        ),
        events=baseline_events,
    )


def _historical_body(event: WhatIfEvent) -> str:
    lines: list[str] = []
    if event.snippet:
        lines.append("[Historical email excerpt]")
        lines.append(event.snippet.strip())
        lines.append("")
        lines.append("[Excerpt limited by source data. Original body may be longer.]")
    else:
        lines.append("[Historical event recorded without body text excerpt]")
    notes = [f"Event type: {event.event_type}"]
    if event.flags.is_forward:
        notes.append("Forward detected in source metadata.")
    if event.flags.is_escalation:
        notes.append("Escalation detected in source metadata.")
    if event.flags.consult_legal_specialist:
        notes.append("Legal specialist signal present.")
    if event.flags.consult_trading_specialist:
        notes.append("Trading specialist signal present.")
    if event.flags.cc_count:
        notes.append(f"CC count: {event.flags.cc_count}.")
    if event.flags.bcc_count:
        notes.append(f"BCC count: {event.flags.bcc_count}.")
    return "\n".join(lines + ["", *notes]).strip()


def _primary_recipient(event: WhatIfEvent) -> str:
    recipients = [item for item in event.flags.to_recipients if item]
    if recipients:
        return recipients[0]
    if event.target_id:
        return event.target_id
    return "archive@enron.com"


def _event_reason_labels(event: WhatIfEvent) -> list[str]:
    labels: list[str] = []
    if event.flags.consult_legal_specialist:
        labels.append("legal")
    if event.flags.consult_trading_specialist:
        labels.append("trading")
    if event.flags.has_attachment_reference:
        labels.append("attachment")
    if event.flags.is_forward:
        labels.append("forward")
    if event.flags.is_escalation or event.event_type == "escalation":
        labels.append("escalation")
    if event.event_type == "assignment":
        labels.append("assignment")
    if event.event_type == "approval":
        labels.append("approval")
    if _has_external_recipients(event.flags.to_recipients):
        labels.append("external_recipient")
    return labels


def _thread_reason_labels(
    thread: WhatIfThreadSummary,
    scenario_id: WhatIfScenarioId,
) -> list[str]:
    if scenario_id == "compliance_gateway":
        return ["legal", "trading"]
    if scenario_id == "escalation_firewall":
        return ["executive_escalation"]
    if scenario_id == "external_dlp":
        return ["attachment", "external_recipient"]
    return ["assignment_without_approval"]


def _event_is_flagged(event: WhatIfEvent) -> bool:
    return bool(_event_reason_labels(event))


def _touches_executive(event: WhatIfEvent) -> bool:
    haystack = " ".join(
        [
            event.actor_id.lower(),
            event.target_id.lower(),
            " ".join(value.lower() for value in event.flags.to_recipients),
            " ".join(value.lower() for value in event.flags.cc_recipients),
        ]
    )
    return any(marker in haystack for marker in _EXECUTIVE_MARKERS)


def _has_external_recipients(recipients: Sequence[str]) -> bool:
    for recipient in recipients:
        if "@" not in recipient:
            continue
        if not recipient.lower().endswith(f"@{_ENRON_DOMAIN}"):
            return True
    return False


def _matches_custodian_filter(
    event: WhatIfEvent,
    tokens: set[str],
) -> bool:
    if event.flags.custodian_id.lower() in tokens:
        return True
    return event.actor_id.lower() in tokens or event.target_id.lower() in tokens


def _resolve_time_window(
    time_window: tuple[str, str] | None,
) -> tuple[int, int] | None:
    if time_window is None:
        return None
    start_raw, end_raw = time_window
    return (_parse_time_value(start_raw), _parse_time_value(end_raw))


def _parse_time_value(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _timestamp_to_ms(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value)
    if not text:
        return 0
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def _timestamp_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _display_name(actor_id: str) -> str:
    token = actor_id.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
    if not token:
        return actor_id
    return " ".join(part.capitalize() for part in token.split())


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]


def _load_episode_context(root: Path) -> dict[str, Any]:
    snapshot_path = root / "context_snapshot.json"
    if not snapshot_path.exists():
        raise ValueError(f"context snapshot not found: {snapshot_path}")
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    sources = payload.get("sources", [])
    for source in sources:
        if not isinstance(source, dict):
            continue
        if source.get("provider") != "mail_archive":
            continue
        data = source.get("data", {})
        return data if isinstance(data, dict) else {}
    raise ValueError("mail archive source is missing from the what-if episode")


def _session_for_episode(
    root: Path,
    *,
    seed: int,
):
    bundle = load_customer_twin(root)
    asset_path = root / bundle.blueprint_asset_path
    asset = BlueprintAsset.model_validate_json(asset_path.read_text(encoding="utf-8"))
    return create_world_session_from_blueprint(asset, seed=seed)


def _allowed_thread_participants(
    *,
    context: dict[str, Any],
    manifest: WhatIfEpisodeManifest,
) -> tuple[list[str], list[str]]:
    actors = sorted(
        {str(actor_id) for actor_id in manifest.actor_ids if str(actor_id).strip()}
    )
    recipients: set[str] = set(actors)
    for thread in context.get("threads", []):
        if (
            not isinstance(thread, dict)
            or thread.get("thread_id") != manifest.thread_id
        ):
            continue
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            for key in ("from", "to"):
                value = str(message.get(key, "")).strip()
                if value:
                    recipients.add(value)
    return actors, sorted(recipients)


def _llm_counterfactual_prompt(
    *,
    context: dict[str, Any],
    manifest: WhatIfEpisodeManifest,
    prompt: str,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> str:
    history_lines: list[str] = []
    for thread in context.get("threads", []):
        if (
            not isinstance(thread, dict)
            or thread.get("thread_id") != manifest.thread_id
        ):
            continue
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            sender = str(message.get("from", "")).strip()
            recipient = str(message.get("to", "")).strip()
            subject = str(message.get("subject", "")).strip()
            body = str(message.get("body_text", "")).strip()
            history_lines.append(
                f"- From: {sender}\n  To: {recipient}\n  Subject: {subject}\n  Body: {body}"
            )
    return "\n".join(
        [
            f"Thread subject: {manifest.thread_subject}",
            f"Branch event id: {manifest.branch_event_id}",
            "Allowed actors:",
            ", ".join(allowed_actors),
            "Allowed recipients:",
            ", ".join(allowed_recipients),
            "Historical thread so far:",
            "\n".join(history_lines[:8]),
            "Counterfactual prompt:",
            prompt,
            "Generate only what happens on this thread after the divergence.",
        ]
    )


def _normalize_llm_messages(
    plan_args: dict[str, Any],
    *,
    manifest: WhatIfEpisodeManifest,
    allowed_actors: Sequence[str],
    allowed_recipients: Sequence[str],
) -> tuple[list[WhatIfLLMGeneratedMessage], list[str]]:
    raw_messages = plan_args.get("messages", plan_args.get("emails", []))
    if not isinstance(raw_messages, list):
        raw_messages = []
    normalized: list[WhatIfLLMGeneratedMessage] = []
    raw_notes = plan_args.get("notes", [])
    notes = (
        [str(item) for item in raw_notes if str(item).strip()]
        if isinstance(raw_notes, list)
        else []
    )
    actor_fallback = allowed_actors[0] if allowed_actors else "counterfactual@enron.com"
    recipient_fallback = allowed_recipients[0] if allowed_recipients else actor_fallback

    for index, raw in enumerate(raw_messages[:3]):
        if not isinstance(raw, dict):
            continue
        actor_id = str(raw.get("actor_id", actor_fallback)).strip()
        if actor_id not in allowed_actors:
            actor_id = actor_fallback
            notes.append(
                f"Message {index + 1} used a non-participant actor; it was clamped to {actor_id}."
            )
        recipient = str(raw.get("to", recipient_fallback)).strip()
        if recipient not in allowed_recipients:
            recipient = recipient_fallback
            notes.append(
                f"Message {index + 1} used a non-thread recipient; it was clamped to {recipient}."
            )
        body_text = str(raw.get("body_text", "")).strip()
        if not body_text:
            continue
        delay_ms = max(1000, _safe_int(raw.get("delay_ms", (index + 1) * 1000)))
        normalized.append(
            WhatIfLLMGeneratedMessage(
                actor_id=actor_id,
                to=recipient,
                subject=_message_subject(
                    raw.get("subject"),
                    fallback=manifest.thread_subject,
                ),
                body_text=body_text,
                delay_ms=delay_ms,
                rationale=str(raw.get("rationale", "")).strip(),
            )
        )
    return normalized, notes


def _message_subject(value: Any, *, fallback: str) -> str:
    subject = str(value or "").strip()
    if subject:
        return subject
    if fallback.lower().startswith("re:"):
        return fallback
    return f"Re: {fallback}"


def _counterfactual_args(plan: dict[str, Any]) -> dict[str, Any]:
    raw_args = plan.get("args")
    if isinstance(raw_args, dict):
        return raw_args
    return plan


def _counterfactual_notes(plan_args: dict[str, Any]) -> list[str]:
    raw_notes = plan_args.get("notes", [])
    if not isinstance(raw_notes, list):
        return []
    return [str(item) for item in raw_notes if str(item).strip()]


def _apply_recipient_scope(
    recipients: Sequence[str],
    *,
    tags: set[str],
) -> tuple[list[str], list[str]]:
    result = [str(item).strip() for item in recipients if str(item).strip()]
    internal_recipients = [
        recipient
        for recipient in result
        if recipient.lower().endswith(f"@{_ENRON_DOMAIN}")
    ]
    internal_only = bool(
        {
            "hold",
            "pause_forward",
            "external_removed",
            "attachment_removed",
            "legal",
            "compliance",
        }
        & tags
    )
    if not internal_only or not internal_recipients:
        return result, []
    return (
        internal_recipients,
        ["Recipient scope was clamped to internal Enron participants."],
    )


def _baseline_tick_ms(dataset_path: Path) -> int:
    dataset = VEIDataset.model_validate_json(dataset_path.read_text(encoding="utf-8"))
    if not dataset.events:
        return 0
    return max(event.time_ms for event in dataset.events) + 1000


def _intervention_tags(prompt: str) -> set[str]:
    lowered = prompt.strip().lower()
    tags: set[str] = set()
    if any(token in lowered for token in ("legal", "compliance")):
        tags.update({"legal", "compliance"})
    if any(token in lowered for token in ("hold", "pause", "stop forward", "freeze")):
        tags.update({"hold", "pause_forward"})
    if any(
        token in lowered
        for token in (
            "reply immediately",
            "respond immediately",
            "same day",
            "right away",
        )
    ):
        tags.add("reply_immediately")
    if any(token in lowered for token in ("owner", "ownership", "clarify owner")):
        tags.add("clarify_owner")
    if any(
        token in lowered
        for token in ("executive gate", "route through", "sign-off", "approval")
    ):
        tags.add("executive_gate")
    if any(token in lowered for token in ("remove attachment", "strip attachment")):
        tags.add("attachment_removed")
    if any(
        token in lowered
        for token in (
            "remove external",
            "pull the outside recipient",
            "internal only",
            "outside recipient",
        )
    ):
        tags.add("external_removed")
    return tags


def _forecast_summary_from_counts(forecast: WhatIfForecast) -> str:
    return (
        f"{forecast.future_event_count} follow-up events remain, with "
        f"{forecast.future_escalation_count} escalations and "
        f"{forecast.future_external_event_count} external sends."
    )


def _forecast_delta_summary(delta: WhatIfForecastDelta) -> str:
    direction = (
        "down"
        if delta.risk_score_delta < 0
        else "up" if delta.risk_score_delta > 0 else "flat"
    )
    return (
        f"Predicted risk moves {direction} by {abs(delta.risk_score_delta):.3f}, "
        f"with escalation delta {delta.escalation_delta} and external-send delta "
        f"{delta.external_event_delta}."
    )


def _render_experiment_overview(result: WhatIfExperimentResult) -> str:
    lines = [
        f"# {result.label}",
        "",
        f"Thread: `{result.intervention.thread_id}`",
        f"Prompt: {result.intervention.prompt}",
        "",
        "## Baseline",
        f"- Scheduled historical future events: {result.baseline.scheduled_event_count}",
        f"- Delivered historical future events: {result.baseline.delivered_event_count}",
        f"- Baseline forecast risk score: {result.baseline.forecast.risk_score}",
    ]
    if result.llm_result is not None:
        lines.extend(
            [
                "",
                "## LLM Actor",
                f"- Status: {result.llm_result.status}",
                f"- Summary: {result.llm_result.summary}",
                f"- Delivered messages: {result.llm_result.delivered_event_count}",
                f"- Inbox count: {result.llm_result.inbox_count}",
            ]
        )
    if result.forecast_result is not None:
        lines.extend(
            [
                "",
                "## E-JEPA Proxy Forecast",
                f"- Status: {result.forecast_result.status}",
                f"- Summary: {result.forecast_result.summary}",
                f"- Baseline risk: {result.forecast_result.baseline.risk_score}",
                f"- Predicted risk: {result.forecast_result.predicted.risk_score}",
            ]
        )
    return "\n".join(lines)


def _slug(value: str) -> str:
    return "_".join(part for part in value.strip().lower().replace("-", " ").split())
