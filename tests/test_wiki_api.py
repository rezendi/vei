"""Route tests for the Studio Company Wiki API."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.ui.api import create_ui_app


@pytest.fixture
def workspace_with_snapshot(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    snapshot = ContextSnapshot(
        organization_name="Acme Renewals",
        organization_domain="acme.example",
        captured_at="2026-04-01T00:00:00Z",
        metadata={"snapshot_role": "company_history_bundle"},
        sources=[
            ContextSourceResult(
                provider="granola",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "transcripts": [
                        {
                            "id": "G-1",
                            "title": "CASE-456 weekly sync",
                            "body": "Renewal review pending legal sign-off.",
                            "owner": "alice@acme.example",
                            "updated_at": "2026-03-30T15:00:00Z",
                        }
                    ]
                },
            ),
            ContextSourceResult(
                provider="slack",
                captured_at="2026-04-01T00:00:00Z",
                status="ok",
                data={
                    "channels": [
                        {
                            "channel": "renewals",
                            "messages": [
                                {
                                    "ts": "1712017800",
                                    "user": "alice",
                                    "text": "Tracking CASE-456 renewal review",
                                }
                            ],
                        }
                    ],
                    "users": [
                        {"id": "alice", "email": "alice@acme.example", "name": "Alice"}
                    ],
                },
            ),
        ],
    )
    (workspace / "context_snapshot.json").write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return workspace


@pytest.fixture
def client(workspace_with_snapshot: Path) -> TestClient:
    return TestClient(create_ui_app(workspace_with_snapshot))


def test_get_workspace_wiki_returns_snapshot(client: TestClient) -> None:
    response = client.get("/api/workspace/wiki")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["organization_name"] == "Acme Renewals"
    assert payload["schema_version"] == "company_wiki_v1"
    page_kinds = [page["page_kind"] for page in payload["pages"]]
    assert page_kinds == [
        "overview",
        "recent_changes",
        "cases",
        "people",
        "knowledge",
        "skills",
        "evidence_index",
    ]


def test_get_wiki_pages_returns_index(client: TestClient) -> None:
    response = client.get("/api/workspace/wiki/pages")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "pages" in payload
    assert {page["page_id"] for page in payload["pages"]} == {
        "overview",
        "recent-changes",
        "cases",
        "people",
        "knowledge",
        "skills",
        "evidence-index",
    }


def test_get_wiki_page_returns_full_page(client: TestClient) -> None:
    response = client.get("/api/workspace/wiki/pages/overview")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["page_id"] == "overview"
    assert payload["page_kind"] == "overview"
    assert isinstance(payload["sections"], list)
    assert payload["sections"]


def test_get_unknown_page_returns_404(client: TestClient) -> None:
    response = client.get("/api/workspace/wiki/pages/does-not-exist")
    assert response.status_code == 404


def test_post_wiki_query_returns_hits(client: TestClient) -> None:
    response = client.post(
        "/api/workspace/wiki/query",
        json={"query": "knowledge", "limit": 5},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["query"] == "knowledge"
    assert payload["total_hits"] >= 1


def test_post_wiki_refresh_writes_artifacts(
    client: TestClient,
    workspace_with_snapshot: Path,
) -> None:
    response = client.post("/api/workspace/wiki/refresh")
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["status"] in {"ok", "partial"}
    assert report["page_count"] == 7
    artifact_path = (
        workspace_with_snapshot / ".artifacts" / "wiki" / "company_wiki.json"
    )
    assert artifact_path.exists()


def test_get_wiki_persisted_artifact_is_used(
    client: TestClient,
    workspace_with_snapshot: Path,
) -> None:
    # Refresh once to write artifacts.
    refresh_response = client.post("/api/workspace/wiki/refresh")
    assert refresh_response.status_code == 200, refresh_response.text

    # Subsequent GET should serve the persisted snapshot.
    get_response = client.get("/api/workspace/wiki")
    assert get_response.status_code == 200
    payload = get_response.json()
    assert payload["organization_name"] == "Acme Renewals"
