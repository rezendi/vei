from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from vei.bus_factor import compute_bus_factor_report, render_report_markdown
from vei.bus_factor.api import resolve_tenant_snapshot
from vei.events.api import ActorRef, EventDomain, ObjectRef, build_event

pytestmark = pytest.mark.unit


_ALICE = ActorRef(actor_id="alice@py-insights.com", display_name="Alice Chen")
_BOB = ActorRef(actor_id="bob@py-insights.com", display_name="Bob Patel")
_CARA = ActorRef(actor_id="cara@py-insights.com", display_name="Cara Liu")


def _ts(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _write_snapshot(root: Path) -> Path:
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "version": "1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "sources": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_path


def _write_events(root: Path) -> list[str]:
    events = [
        # Alice sends 8 events grounding the "Quarterly customer health" skill.
        *[
            build_event(
                event_id=f"evt-health-{i}",
                tenant_id="py-insights",
                case_id="case-health",
                ts_ms=_ts(2026, 5, 1 + i),
                domain=EventDomain.COMM_GRAPH,
                kind="gmail.message",
                actor_ref=_ALICE,
                object_refs=[
                    ObjectRef(
                        object_id="thread-health", domain="comm_graph", kind="mail"
                    )
                ],
                delta_data={
                    "surface": "mail",
                    "subject": "Customer health review",
                    "snippet": "quarterly checkin",
                },
            ).with_hash()
            for i in range(8)
        ],
        # Bob and Cara both send on the "Onboarding flow" skill — shared, not sole.
        build_event(
            event_id="evt-onboard-1",
            tenant_id="py-insights",
            case_id="case-onboard",
            ts_ms=_ts(2026, 5, 3),
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=_BOB,
            delta_data={
                "surface": "mail",
                "subject": "Onboarding",
                "snippet": "welcome flow",
            },
        ).with_hash(),
        build_event(
            event_id="evt-onboard-2",
            tenant_id="py-insights",
            case_id="case-onboard",
            ts_ms=_ts(2026, 5, 4),
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=_CARA,
            delta_data={
                "surface": "mail",
                "subject": "Onboarding follow-up",
                "snippet": "next steps",
            },
        ).with_hash(),
        # Renewal escalation workflow: Alice sends 9 of 10 events => 90% share.
        *[
            build_event(
                event_id=f"evt-renewal-{i}",
                tenant_id="py-insights",
                case_id="case-renewal",
                ts_ms=_ts(2026, 5, 6 + i),
                domain=EventDomain.COMM_GRAPH,
                kind="gmail.message",
                actor_ref=_ALICE,
                delta_data={
                    "surface": "mail",
                    "subject": "Renewal escalation",
                    "snippet": "escalate",
                },
            ).with_hash()
            for i in range(9)
        ],
        build_event(
            event_id="evt-renewal-other",
            tenant_id="py-insights",
            case_id="case-renewal",
            ts_ms=_ts(2026, 5, 14),
            domain=EventDomain.COMM_GRAPH,
            kind="gmail.message",
            actor_ref=_BOB,
            delta_data={"surface": "mail", "subject": "Renewal", "snippet": "FYI"},
        ).with_hash(),
    ]
    event_path = root / "canonical_events.jsonl"
    event_path.write_text(
        "".join(event.model_dump_json() + "\n" for event in events),
        encoding="utf-8",
    )
    return [event.event_id for event in events]


def _write_skill_map(root: Path) -> Path:
    skill_dir = root / "skill_map"
    skill_dir.mkdir()
    path = skill_dir / "company_skill_map.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "company_skill_map_v1",
                "organization_name": "Py Insights",
                "organization_domain": "py-insights.com",
                "generated_at": "2026-05-15T00:00:00Z",
                "source_ref": "test",
                "canonical_event_count": 12,
                "skill_count": 2,
                "skills": [
                    {
                        "skill_id": "skill:health-review",
                        "title": "Quarterly customer health review",
                        "summary": "Reviews customer health and surfaces churn risk.",
                        "status": "draft",
                        "candidate_type": "workflow",
                        "usefulness_score": 0.9,
                        "trigger": {"description": "Quarterly cadence."},
                        "goal": "Surface health risk.",
                        "evidence_refs": [
                            {
                                "ref_type": "event",
                                "ref_id": f"evt-health-{i}",
                                "title": "Customer health review",
                            }
                            for i in range(8)
                        ],
                    },
                    {
                        "skill_id": "skill:onboarding",
                        "title": "Onboarding flow",
                        "summary": "Onboards new customers across the team.",
                        "status": "draft",
                        "candidate_type": "workflow",
                        "usefulness_score": 0.8,
                        "trigger": {"description": "New customer signup."},
                        "goal": "Activate the customer.",
                        "evidence_refs": [
                            {
                                "ref_type": "event",
                                "ref_id": "evt-onboard-1",
                                "title": "Onboarding",
                            },
                            {
                                "ref_type": "event",
                                "ref_id": "evt-onboard-2",
                                "title": "Onboarding",
                            },
                        ],
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_workflow_artifacts(root: Path) -> tuple[Path, Path]:
    workflows_dir = root / "workflows"
    workflows_dir.mkdir()
    candidates_path = workflows_dir / "workflow_candidates.json"
    candidates_path.write_text(
        json.dumps(
            {
                "source_dir": str(root / "context_snapshot.json"),
                "company_name": "Py Insights",
                "company_domain": "py-insights.com",
                "event_count": 12,
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": "wfc-renewal",
                        "title": "Renewal escalation playbook",
                        "company_name": "Py Insights",
                        "company_domain": "py-insights.com",
                        "group_key": "case:case-renewal",
                        "source_case_ids": ["case-renewal"],
                        "source_event_ids": [
                            *[f"evt-renewal-{i}" for i in range(9)],
                            "evt-renewal-other",
                        ],
                        "thread_refs": [],
                        "surfaces": ["mail"],
                        "event_kinds": ["gmail.message"],
                        "actor_ids": [_ALICE.actor_id, _BOB.actor_id],
                        "rank_score": 0.7,
                    }
                ],
                "metadata": {"selected_backend": "semantic"},
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    labels_path = workflows_dir / "workflow_labels.json"
    labels_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "labels": [
                    {
                        "label_id": "wfl-1",
                        "candidate_id": "wfc-renewal",
                        "label": "good_example",
                        "note": "operator-marked",
                        "event_ids": [],
                        "example_ids": [],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return candidates_path, labels_path


def test_report_flags_sole_owner_and_primary_driver(tmp_path: Path) -> None:
    snapshot = _write_snapshot(tmp_path)
    _write_events(tmp_path)
    _write_skill_map(tmp_path)
    _write_workflow_artifacts(tmp_path)

    report = compute_bus_factor_report(
        context_path=snapshot,
        tenant_id="py-insights",
    )

    assert report.tenant_id == "py-insights"
    assert report.total_events_in_corpus == 20
    assert report.total_events_in_window == 20

    by_actor = {profile.actor_id: profile for profile in report.actor_profiles}
    assert _ALICE.actor_id in by_actor
    # Bob and Cara split the onboarding skill, so neither is the sole owner.
    assert _BOB.actor_id not in by_actor
    assert _CARA.actor_id not in by_actor

    alice = by_actor[_ALICE.actor_id]
    assert {skill.skill_id for skill in alice.sole_owned_skills} == {
        "skill:health-review"
    }
    assert {wf.candidate_id for wf in alice.sole_owned_workflows} == {"wfc-renewal"}
    renewal = alice.sole_owned_workflows[0]
    assert renewal.label == "good_example"
    assert renewal.activity_share == pytest.approx(0.9, abs=0.001)
    assert "sole_owner_of_1_skill(s)" in alice.flag_predicates
    assert "primary_driver_on_labeled_good_workflow" in alice.flag_predicates


def test_report_omits_missing_artifact_sections(tmp_path: Path) -> None:
    snapshot = _write_snapshot(tmp_path)
    _write_events(tmp_path)
    # No skill map, no workflow artifacts.

    report = compute_bus_factor_report(
        context_path=snapshot,
        tenant_id="py-insights",
    )
    assert report.actor_profiles == []
    assert any("skill map" in note for note in report.notes)
    assert any("workflow" in note for note in report.notes)


def test_markdown_redaction_elides_identities(tmp_path: Path) -> None:
    snapshot = _write_snapshot(tmp_path)
    _write_events(tmp_path)
    _write_skill_map(tmp_path)
    _write_workflow_artifacts(tmp_path)
    report = compute_bus_factor_report(context_path=snapshot, tenant_id="py-insights")

    redacted = render_report_markdown(report, redact=True)
    unredacted = render_report_markdown(report, redact=False)

    assert "Alice Chen" in unredacted
    assert "alice@py-insights.com" in unredacted
    assert "Alice Chen" not in redacted
    assert "alice@py-insights.com" not in redacted
    assert "Actor 1" in redacted


def test_resolve_tenant_snapshot_finds_direct_and_combined(tmp_path: Path) -> None:
    base = tmp_path / "_vei_out"
    direct = base / "yourco" / "context_snapshot.json"
    direct.parent.mkdir(parents=True)
    direct.write_text("{}", encoding="utf-8")
    assert resolve_tenant_snapshot("yourco", root=base) == direct.resolve()

    combined = (
        base
        / "py-insights"
        / "combined-live-20260504-20260513"
        / "context_snapshot.json"
    )
    combined.parent.mkdir(parents=True)
    combined.write_text("{}", encoding="utf-8")
    older = (
        base
        / "py-insights"
        / "combined-live-20260401-20260408"
        / "context_snapshot.json"
    )
    older.parent.mkdir(parents=True)
    older.write_text("{}", encoding="utf-8")
    resolved = resolve_tenant_snapshot("py-insights", root=base)
    assert resolved == combined.resolve()
    # Underscore alias should also resolve.
    assert resolve_tenant_snapshot("py_insights", root=base) == combined.resolve()


def test_resolve_tenant_snapshot_raises_when_missing(tmp_path: Path) -> None:
    base = tmp_path / "_vei_out"
    base.mkdir()
    with pytest.raises(FileNotFoundError, match="Could not resolve tenant"):
        resolve_tenant_snapshot("ghost", root=base)
