from __future__ import annotations

import json
from pathlib import Path

import typer.testing

from vei.cli.vei_discover import app as discover_cli_app
from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)
from vei.discovery.api import discover_company_workflows_and_skills
from vei.discovery.models import DiscoverySourceInput, DiscoverySourceRole


def test_discovery_combines_historical_poy_and_live_py_into_one_lineage(
    tmp_path: Path,
) -> None:
    historical = _write_historical_poy_bundle(tmp_path / "poy")
    live = _write_live_py_bundle(tmp_path / "py")
    output = tmp_path / "discovery"

    result = discover_company_workflows_and_skills(
        [
            DiscoverySourceInput(
                path=str(historical),
                role=DiscoverySourceRole.HISTORICAL,
            ),
            DiscoverySourceInput(
                path=str(live),
                role=DiscoverySourceRole.LIVE,
            ),
        ],
        company_name="Py Insights",
        company_domain="py-insights.com",
        aliases=["PoY", "Powr of You"],
        output=output,
        workflow_limit=8,
        skill_limit=6,
    )

    assert result.lineage.canonical_name == "Py Insights"
    assert result.lineage.canonical_domain == "py-insights.com"
    assert "Powr of You" in result.lineage.aliases
    assert "PoY" in result.lineage.aliases
    assert {source.role for source in result.lineage.sources} == {
        DiscoverySourceRole.HISTORICAL,
        DiscoverySourceRole.LIVE,
    }
    assert result.lineage.event_count >= 14
    assert result.workflows

    workflow_titles = {workflow.title for workflow in result.workflows}
    assert "Study And Research Operations" in workflow_titles
    assert "Privacy, Consent, And Provenance Review" in workflow_titles
    assert "Product Support, QA, And Test Case Triage" in workflow_titles
    assert all(workflow.evidence_refs for workflow in result.workflows)
    assert all(
        "Hi" not in evidence.subject
        for workflow in result.workflows
        for evidence in workflow.evidence_refs
    )

    assert result.skill_map.skill_count >= 3
    skill_titles = {skill.title for skill in result.skill_map.skills}
    assert "Research Study Coordinator" in skill_titles
    assert "Privacy And Evidence Gate" in skill_titles
    assert "Product Triage Packet Builder" in skill_titles
    assert all(skill.skill_path for skill in result.skill_map.skills)
    assert all(Path(skill.skill_path).is_file() for skill in result.skill_map.skills)
    first_skill = Path(result.skill_map.skills[0].skill_path).read_text(
        encoding="utf-8"
    )
    assert first_skill.startswith("---\nname:")
    assert "description:" in first_skill.split("---", 2)[1]

    stale_skill_dir = output / "skills" / "recurring-work-pattern-summarizer"
    stale_skill_dir.mkdir(parents=True)
    (stale_skill_dir / "SKILL.md").write_text("# stale\n", encoding="utf-8")
    discover_company_workflows_and_skills(
        [
            DiscoverySourceInput(
                path=str(historical),
                role=DiscoverySourceRole.HISTORICAL,
            ),
            DiscoverySourceInput(
                path=str(live),
                role=DiscoverySourceRole.LIVE,
            ),
        ],
        company_name="Py Insights",
        company_domain="py-insights.com",
        aliases=["PoY", "Powr of You"],
        output=output,
        workflow_limit=8,
        skill_limit=6,
    )
    assert not stale_skill_dir.exists()

    assert (output / "company_lineage.json").is_file()
    assert (output / "workflow_families.json").is_file()
    assert (output / "workflow_families.md").is_file()
    assert (output / "skill_map.json").is_file()
    assert (output / "skill_map.md").is_file()
    assert (output / "discovery_manifest.json").is_file()


def test_discovery_cli_writes_company_artifacts(tmp_path: Path) -> None:
    historical = _write_historical_poy_bundle(tmp_path / "poy")
    live = _write_live_py_bundle(tmp_path / "py")
    output = tmp_path / "cli_discovery"
    runner = typer.testing.CliRunner()

    result = runner.invoke(
        discover_cli_app,
        [
            "run",
            "--historical-source",
            str(historical),
            "--live-source",
            str(live),
            "--company-name",
            "Py Insights",
            "--company-domain",
            "py-insights.com",
            "--alias",
            "Powr of You",
            "--output",
            str(output),
            "--workflow-limit",
            "6",
            "--skill-limit",
            "4",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Discovered" in result.output
    lineage = json.loads((output / "company_lineage.json").read_text(encoding="utf-8"))
    assert lineage["canonical_name"] == "Py Insights"
    assert "Powr of You" in lineage["aliases"]
    skill_map = json.loads((output / "skill_map.json").read_text(encoding="utf-8"))
    assert skill_map["skill_count"] >= 3


def _write_historical_poy_bundle(root: Path) -> Path:
    root.mkdir(parents=True)
    snapshot = ContextSnapshot(
        organization_name="Powr of You",
        organization_domain="powrofyou.com",
        captured_at="2026-05-01T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="mail_archive",
                captured_at="2026-05-01T00:00:00Z",
                record_counts={"threads": 4, "messages": 8},
                data={
                    "threads": [
                        _mail_thread(
                            "poy-study-1",
                            "Diary study participant scheduling",
                            [
                                "The diary study needs participant interviews and consent confirmation.",
                                "Research sample is ready; schedule the next study session.",
                            ],
                        ),
                        _mail_thread(
                            "poy-study-2",
                            "Consumer research sample recruitment",
                            [
                                "Recruit respondent sample for the interview study.",
                                "Screener complete; participant follow-up is next.",
                            ],
                        ),
                        _mail_thread(
                            "poy-privacy-1",
                            "Privacy consent and provenance review",
                            [
                                "Privacy consent evidence is needed before sharing this claim.",
                                "GDPR and data protection review should cite the source events.",
                            ],
                        ),
                        _mail_thread(
                            "poy-partner-1",
                            "Partner pilot proposal discussion",
                            [
                                "Partner pilot demo needs a customer proposal and launch brief.",
                                "Sales follow-up should include the pilot next step.",
                            ],
                        ),
                    ]
                },
            ),
            ContextSourceResult(
                provider="clickup",
                captured_at="2026-05-01T00:00:00Z",
                record_counts={"tasks": 2},
                data={
                    "tasks": [
                        {
                            "id": "CU-101",
                            "name": "Chrome extension test case error",
                            "description": (
                                "QA test case failed: steps field bug in the extension dashboard."
                            ),
                            "assignee": "product@powrofyou.com",
                            "date_updated": "2026-04-28T10:00:00Z",
                            "comments": [
                                {
                                    "id": "CU-101-C1",
                                    "user": "qa@powrofyou.com",
                                    "date": "2026-04-28T11:00:00Z",
                                    "comment_text": (
                                        "Reproduction steps and support impact added."
                                    ),
                                }
                            ],
                        },
                        {
                            "id": "CU-102",
                            "name": "Daily status update blocker",
                            "description": (
                                "Daily task update: blocker on release owner and next action."
                            ),
                            "assignee": "ops@powrofyou.com",
                            "date_updated": "2026-04-29T10:00:00Z",
                        },
                    ]
                },
            ),
        ],
    )
    return _write_snapshot(root, snapshot)


def _write_live_py_bundle(root: Path) -> Path:
    root.mkdir(parents=True)
    snapshot = ContextSnapshot(
        organization_name="Py Insights",
        organization_domain="py-insights.com",
        captured_at="2026-05-12T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="teams",
                captured_at="2026-05-12T00:00:00Z",
                record_counts={"channels": 1, "messages": 6},
                data={
                    "channels": [
                        {
                            "channel": "research-ops",
                            "messages": [
                                {
                                    "ts": "2026-05-10T09:00:00Z",
                                    "user": "ops@py-insights.com",
                                    "text": "Hi",
                                },
                                {
                                    "ts": "2026-05-10T09:05:00Z",
                                    "user": "ops@py-insights.com",
                                    "text": (
                                        "Interview study schedule needs participant consent "
                                        "and a reminder."
                                    ),
                                },
                                {
                                    "ts": "2026-05-10T09:08:00Z",
                                    "user": "legal@py-insights.com",
                                    "text": (
                                        "Privacy notice details required for this study "
                                        "before external sharing."
                                    ),
                                },
                                {
                                    "ts": "2026-05-10T09:12:00Z",
                                    "user": "product@py-insights.com",
                                    "text": (
                                        "Test case steps field bug appears in the extension dashboard."
                                    ),
                                },
                                {
                                    "ts": "2026-05-10T09:15:00Z",
                                    "user": "support@py-insights.com",
                                    "text": (
                                        "Support issue has reproduction steps and a failed QA test."
                                    ),
                                },
                                {
                                    "ts": "2026-05-10T09:20:00Z",
                                    "user": "sales@py-insights.com",
                                    "text": (
                                        "Partner demo follow-up needs pilot proposal context."
                                    ),
                                },
                            ],
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="onedrive",
                captured_at="2026-05-12T00:00:00Z",
                record_counts={"documents": 2},
                data={
                    "documents": [
                        {
                            "id": "OD-1",
                            "title": "Study consent package",
                            "summary": (
                                "Research study consent package and participant interview "
                                "follow-up materials."
                            ),
                            "owner": "ops@py-insights.com",
                            "updated_at": "2026-05-11T10:00:00Z",
                        },
                        {
                            "id": "OD-2",
                            "title": "Product support test plan",
                            "summary": (
                                "QA test plan for extension dashboard support issue and "
                                "reproduction steps."
                            ),
                            "owner": "product@py-insights.com",
                            "updated_at": "2026-05-11T11:00:00Z",
                        },
                    ]
                },
            ),
        ],
    )
    return _write_snapshot(root, snapshot)


def _mail_thread(thread_id: str, subject: str, bodies: list[str]) -> dict[str, object]:
    return {
        "thread_id": thread_id,
        "subject": subject,
        "messages": [
            {
                "from": "ops@powrofyou.com",
                "to": ["team@powrofyou.com"],
                "date": f"2026-04-{20 + index:02d}T10:00:00Z",
                "subject": subject,
                "body_text": body,
            }
            for index, body in enumerate(bodies)
        ],
    }


def _write_snapshot(root: Path, snapshot: ContextSnapshot) -> Path:
    snapshot_path = root / "context_snapshot.json"
    snapshot_path.write_text(
        snapshot.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    write_canonical_history_sidecars(snapshot, snapshot_path)
    return root
