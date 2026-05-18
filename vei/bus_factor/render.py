"""Render a BusFactorReport as operator-facing markdown."""

from __future__ import annotations

from datetime import datetime, timezone

from .models import ActorRiskProfile, BusFactorReport


def _format_ts(ts_ms: int) -> str:
    if not ts_ms:
        return "unknown"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _days_between(start_ms: int, end_ms: int) -> float:
    if not start_ms or not end_ms or end_ms <= start_ms:
        return 0.0
    return round((end_ms - start_ms) / (24 * 60 * 60 * 1000), 1)


def _redact_actor_identity(
    profile: ActorRiskProfile, anon_index: int
) -> tuple[str, str]:
    """Return (display_label, identity_line) for a redacted profile."""

    display_label = f"Actor {anon_index}"
    return display_label, "_anonymized; original actor_id elided_"


def _identity_line(profile: ActorRiskProfile) -> tuple[str, str]:
    display = profile.display_name or profile.actor_id
    identity_bits: list[str] = []
    if profile.email and profile.email != display:
        identity_bits.append(profile.email)
    if (
        profile.actor_id
        and profile.actor_id != display
        and profile.actor_id != profile.email
    ):
        identity_bits.append(f"actor_id={profile.actor_id}")
    identity = " — ".join(identity_bits) if identity_bits else ""
    return display, identity


def render_report_markdown(report: BusFactorReport, *, redact: bool = False) -> str:
    out: list[str] = []
    window_label = f"{_format_ts(report.window_start_ts_ms)} → {_format_ts(report.window_end_ts_ms)}"
    corpus_span = _days_between(
        report.corpus_first_event_ts_ms, report.corpus_last_event_ts_ms
    )

    out.append(f"# Bus-factor report — {report.tenant_id}")
    out.append("")
    out.append(f"_Generated {report.generated_at}_")
    out.append("")
    out.append("## Scope")
    out.append("")
    out.append(f"- Window: **{report.window_days} days** ({window_label})")
    out.append(
        f"- Corpus span: **{corpus_span} days** "
        f"({report.total_events_in_corpus} events total, "
        f"{report.total_events_in_window} in window)"
    )
    out.append(f"- Sole-owner definition: {report.sole_owner_definition}")
    out.append(
        f"- Activity-share threshold for workflow primary driver: "
        f"**{report.activity_share_threshold:.0%}**"
    )
    if not redact:
        out.append(f"- Snapshot: `{report.snapshot_path}`")
        if report.skill_map_path:
            out.append(f"- Skill map: `{report.skill_map_path}`")
        if report.workflow_candidates_path:
            out.append(f"- Workflow candidates: `{report.workflow_candidates_path}`")
    out.append("")

    if report.notes:
        out.append("## Notes")
        out.append("")
        for note in report.notes:
            out.append(f"- {note}")
        out.append("")

    out.append("## Flagged actors")
    out.append("")

    if not report.actor_profiles:
        out.append(
            "_No actors met the v1 sole-ownership criteria in this window. "
            "Either required artifacts are missing or unlabeled, the corpus is "
            "too small, the skill map is empty, or the tenant genuinely has "
            "shared coverage on every surface._"
        )
        out.append("")
        return "\n".join(out).rstrip() + "\n"

    for idx, profile in enumerate(report.actor_profiles, start=1):
        if redact:
            display, identity = _redact_actor_identity(profile, idx)
        else:
            display, identity = _identity_line(profile)
        out.append(f"### {idx}. {display}")
        if identity:
            out.append(identity)
        out.append("")
        out.append(
            f"- Last active: **{_format_ts(profile.last_active_ts_ms)}** "
            f"({profile.sent_event_count_in_window} sent events in window)"
        )
        if profile.flag_predicates:
            out.append(f"- Triggered: {', '.join(profile.flag_predicates)}")
        out.append("")

        if profile.sole_owned_skills:
            out.append("**Sole owner of skills:**")
            out.append("")
            for skill in profile.sole_owned_skills:
                evidence_label = (
                    f"{skill.evidence_event_count} cited events"
                    if skill.evidence_event_count != 1
                    else "1 cited event"
                )
                line = f"- *{skill.title}* — {evidence_label}"
                if not redact and skill.evidence_event_ids:
                    sample = ", ".join(skill.evidence_event_ids[:4])
                    suffix = ", ..." if len(skill.evidence_event_ids) > 4 else ""
                    line += f" (`{sample}{suffix}`)"
                out.append(line)
            out.append("")

        if profile.sole_owned_workflows:
            out.append("**Primary driver on workflows:**")
            out.append("")
            for workflow in profile.sole_owned_workflows:
                label_pretty = workflow.label.replace("_", " ")
                share_pct = f"{workflow.activity_share:.0%}"
                line = (
                    f"- *{workflow.title}* ({label_pretty}) — "
                    f"{share_pct} of {workflow.total_event_count} events "
                    f"in window ({workflow.sent_event_count} from this actor)"
                )
                if not redact:
                    line += f" — `{workflow.candidate_id}`"
                out.append(line)
            out.append("")

    return "\n".join(out).rstrip() + "\n"
