from __future__ import annotations

import pytest

from vei.router.core import Router
from vei.world.api import get_catalog_scenario

pytestmark = pytest.mark.integration


def test_act_and_observe_basic():
    r = Router(seed=1, artifacts_dir=None)
    ao = r.act_and_observe("browser.read", {})
    assert "result" in ao and "observation" in ao
    assert "title" in ao["result"]
    assert "action_menu" in ao["observation"]


def test_act_and_observe_supports_graph_native_tools():
    r = Router(
        seed=1,
        artifacts_dir=None,
        scenario=get_catalog_scenario("acquired_sales_onboarding"),
    )

    plan = r.act_and_observe(
        "vei.graph_plan",
        {"domain": "identity_graph", "limit": 4},
    )
    assert any(
        step["action"] == "assign_application"
        for step in plan["result"]["available_actions"]
    )

    ao = r.act_and_observe(
        "vei.graph_action",
        {
            "domain": "identity_graph",
            "action": "assign_application",
            "args": {"user_id": "USR-ACQ-1", "app_id": "APP-crm"},
        },
    )

    assert ao["result"]["ok"] is True
    assert ao["result"]["tool"] == "okta.assign_application"
    assert ao["result"]["result"]["app_id"] == "APP-crm"
    assert ao["result"]["result"]["assignments"] == 2
    assert ao["observation"]["focus"] == "okta"


def test_pending_and_tick_mail_delivery(tmp_path):
    r = Router(seed=1, artifacts_dir=str(tmp_path / "artifacts"))
    # Compose schedules a mail reply in the future
    r.call_and_step(
        "mail.compose",
        {
            "to": "sales@macrocompute.example",
            "subj": "Quote request",
            "body_text": "Please send latest price and ETA.",
        },
    )
    p = r.pending()
    assert p["mail"] >= 1
    # Advance enough time to deliver
    res = r.tick(15000)
    assert res["pending"]["mail"] == 0
    # Ensure the message was delivered to inbox
    inbox = r.mail.list()
    assert len(inbox) >= 1


def test_tick_delivers_new_twin_targets_and_tracks_pending_counts() -> None:
    r = Router(seed=7, artifacts_dir=None)
    r.bus.schedule(
        0, "docs", {"title": "Policy update", "body": "v2", "tags": ["policy"]}
    )
    r.bus.schedule(
        0,
        "calendar",
        {
            "title": "Approval Sync",
            "start_ms": 10_000,
            "end_ms": 11_000,
            "attendees": ["ops@example.com"],
        },
    )
    r.bus.schedule(0, "tickets", {"title": "Follow up approval", "assignee": "sam"})
    r.bus.schedule(0, "custom_target", {"payload": "noop"})

    pending = r.pending()
    assert pending["docs"] == 1
    assert pending["calendar"] == 1
    assert pending["tickets"] == 1
    assert pending["custom_target"] == 1
    assert pending["total"] >= 4

    delivered = r.tick(1000)["delivered"]
    assert delivered["docs"] == 1
    assert delivered["calendar"] == 1
    assert delivered["tickets"] == 1
    assert delivered["custom_target"] == 1

    assert any(doc["title"] == "Policy update" for doc in r.docs.list())
    assert any(event["title"] == "Approval Sync" for event in r.calendar.list_events())
    assert any(ticket["title"] == "Follow up approval" for ticket in r.tickets.list())


def test_router_without_explicit_artifacts_dir_ignores_env_artifacts_dir(
    tmp_path, monkeypatch
) -> None:
    shared_artifacts = tmp_path / "shared-artifacts"
    monkeypatch.setenv("VEI_ARTIFACTS_DIR", str(shared_artifacts))

    router = Router(seed=11, artifacts_dir=None)
    router.call_and_step("browser.read", {})

    assert not shared_artifacts.exists()
