from __future__ import annotations

from .models import (
    WhatIfEpisodeMaterialization,
    WhatIfExperimentResult,
    WhatIfForecastResult,
    WhatIfLLMReplayResult,
    WhatIfReplaySummary,
    WhatIfResult,
    WhatIfWorld,
)


def render_world_summary(world: WhatIfWorld) -> str:
    lines = [
        f"# {world.source.title()} What-If Source",
        "",
        f"- Events: {world.summary.event_count}",
        f"- Threads: {world.summary.thread_count}",
        f"- Actors: {world.summary.actor_count}",
        f"- Custodians: {world.summary.custodian_count}",
    ]
    if world.summary.first_timestamp and world.summary.last_timestamp:
        lines.extend(
            [
                f"- Time range: {world.summary.first_timestamp} to {world.summary.last_timestamp}",
                "",
            ]
        )
    else:
        lines.append("")
    lines.append("## Supported Scenarios")
    for scenario in world.scenarios:
        lines.append(f"- `{scenario.scenario_id}`: {scenario.description}")
    return "\n".join(lines)


def render_result(result: WhatIfResult) -> str:
    lines = [
        f"# {result.scenario.title}",
        "",
        result.scenario.description,
        "",
        f"- Matched events: {result.matched_event_count}",
        f"- Affected threads: {result.affected_thread_count}",
        f"- Affected actors: {result.affected_actor_count}",
        f"- Blocked forwards: {result.blocked_forward_count}",
        f"- Blocked escalations: {result.blocked_escalation_count}",
        f"- Delayed assignments: {result.delayed_assignment_count}",
        "",
        f"Timeline impact: {result.timeline_impact}",
        "",
        "## Top Threads",
    ]
    if not result.top_threads:
        lines.append("- No matched threads.")
    else:
        for thread in result.top_threads:
            lines.append(
                f"- `{thread.thread_id}` {thread.subject} "
                f"({thread.affected_event_count} events, {thread.participant_count} participants)"
            )
    lines.extend(["", "## Top Actors"])
    if not result.top_actors:
        lines.append("- No matched actors.")
    else:
        for actor in result.top_actors:
            lines.append(
                f"- {actor.display_name} ({actor.actor_id}) "
                f"across {actor.affected_thread_count} threads"
            )
    lines.extend(["", "## Decision Branches"])
    for branch in result.decision_branches:
        lines.append(f"- {branch}")
    return "\n".join(lines)


def render_episode(materialization: WhatIfEpisodeMaterialization) -> str:
    return "\n".join(
        [
            "# What-If Episode Materialized",
            "",
            f"- Workspace: {materialization.workspace_root}",
            f"- Thread: `{materialization.thread_id}`",
            f"- Branch event: `{materialization.branch_event_id}`",
            f"- Seeded historical messages: {materialization.history_message_count}",
            f"- Scheduled future events: {materialization.future_event_count}",
            f"- Forecast risk score: {materialization.forecast.risk_score}",
        ]
    )


def render_replay(summary: WhatIfReplaySummary) -> str:
    lines = [
        "# What-If Replay",
        "",
        f"- Scheduled future events: {summary.scheduled_event_count}",
        f"- Delivered after tick: {summary.delivered_event_count}",
        f"- Current time: {summary.current_time_ms} ms",
        f"- Inbox count: {summary.inbox_count}",
        f"- Forecast risk score: {summary.forecast.risk_score}",
    ]
    if summary.top_subjects:
        lines.extend(["", "## Top Subjects"])
        for subject in summary.top_subjects:
            lines.append(f"- {subject}")
    return "\n".join(lines)


def render_llm_result(result: WhatIfLLMReplayResult) -> str:
    lines = [
        "# LLM Counterfactual Replay",
        "",
        f"- Status: {result.status}",
        f"- Provider: {result.provider}",
        f"- Model: {result.model}",
        f"- Summary: {result.summary}",
        f"- Generated messages: {len(result.messages)}",
        f"- Delivered messages: {result.delivered_event_count}",
        f"- Inbox count: {result.inbox_count}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    if result.messages:
        lines.extend(["", "## Messages"])
        for message in result.messages:
            lines.append(
                f"- `{message.actor_id}` -> `{message.to}` after {message.delay_ms} ms: "
                f"{message.subject}"
            )
    return "\n".join(lines)


def render_forecast_result(result: WhatIfForecastResult) -> str:
    lines = [
        "# E-JEPA Proxy Forecast",
        "",
        f"- Status: {result.status}",
        f"- Summary: {result.summary}",
        f"- Baseline risk: {result.baseline.risk_score}",
        f"- Predicted risk: {result.predicted.risk_score}",
        f"- Escalation delta: {result.delta.escalation_delta}",
        f"- External-send delta: {result.delta.external_event_delta}",
    ]
    if result.notes:
        lines.extend(["", "## Notes"])
        for note in result.notes:
            lines.append(f"- {note}")
    return "\n".join(lines)


def render_experiment(result: WhatIfExperimentResult) -> str:
    lines = [
        f"# {result.label}",
        "",
        f"- Selected thread: `{result.intervention.thread_id}`",
        f"- Counterfactual prompt: {result.intervention.prompt}",
        f"- Baseline scheduled events: {result.baseline.scheduled_event_count}",
        f"- Baseline delivered events: {result.baseline.delivered_event_count}",
        f"- Baseline risk score: {result.baseline.forecast.risk_score}",
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
                "## E-JEPA Proxy",
                f"- Status: {result.forecast_result.status}",
                f"- Summary: {result.forecast_result.summary}",
                f"- Predicted risk: {result.forecast_result.predicted.risk_score}",
            ]
        )
    lines.extend(
        [
            "",
            "## Artifacts",
            f"- Result JSON: {result.artifacts.result_json_path}",
            f"- Overview Markdown: {result.artifacts.overview_markdown_path}",
        ]
    )
    if result.artifacts.llm_json_path is not None:
        lines.append(f"- LLM JSON: {result.artifacts.llm_json_path}")
    if result.artifacts.forecast_json_path is not None:
        lines.append(f"- Forecast JSON: {result.artifacts.forecast_json_path}")
    return "\n".join(lines)
