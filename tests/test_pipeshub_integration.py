from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest
from typer.testing import CliRunner

from vei.cli.vei import app
from vei.context.pipeshub import (
    PipesHubClient,
    _extract_total,
    _pipeshub_connector_filter_values,
    capture_pipeshub_context,
    write_pipeshub_capture,
)


class _Response:
    def __init__(self, payload: Any = None, body: bytes | None = None) -> None:
        self.payload = payload
        self.body = body
        self.headers = {}

    def read(self) -> bytes:
        if self.body is not None:
            return self.body
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def test_pipeshub_launcher_dry_run_writes_local_profile(tmp_path: Path) -> None:
    runner = CliRunner()
    runtime_dir = tmp_path / "pipeshub"

    result = runner.invoke(
        app,
        [
            "connectors",
            "pipeshub",
            "up",
            "--runtime-dir",
            str(runtime_dir),
            "--dry-run",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["base_image"] == "pipeshubai/pipeshub-ai:0.4.0"
    assert payload["image"] == "vei-pipeshub-ai:0.4.0"
    assert (runtime_dir / ".env").exists()
    assert (runtime_dir / "Dockerfile.pipeshub").exists()
    assert (runtime_dir / "patch-pipeshub-deployment-config.js").exists()
    compose_text = (runtime_dir / "docker-compose.yml").read_text(encoding="utf-8")
    assert "\t" not in compose_text
    assert "build:" in compose_text
    assert (
        "PIPESHUB_BASE_IMAGE: pipeshubai/pipeshub-ai:${IMAGE_TAG:-0.4.0}"
        in compose_text
    )
    assert "arangosh --server.endpoint tcp://127.0.0.1:8529" in compose_text
    assert "pipeshub-config-init:" in compose_text
    assert '\\"dataStoreType\\":\\"$${DATA_STORE}\\"' in compose_text
    assert "condition: service_completed_successfully" in compose_text
    assert "KAFKA_BROKERS=${KAFKA_BROKERS:-kafka-1:9092}" in compose_text
    assert "DATA_STORE=${DATA_STORE:-arangodb}" in compose_text
    assert "MESSAGE_BROKER=${MESSAGE_BROKER:-redis}" in compose_text
    assert (
        "REDISCLI_AUTH=$${REDIS_PASSWORD:-} redis-cli --raw incr ping" in compose_text
    )
    assert "SANDBOX_MODE=${SANDBOX_MODE:-subprocess}" in compose_text
    assert "zookeeper" not in compose_text.lower()
    assert "confluentinc/cp-kafka" not in compose_text.lower()
    patch_text = (runtime_dir / "patch-pipeshub-deployment-config.js").read_text(
        encoding="utf-8"
    )
    assert "if (typeof parsed === 'string')" in patch_text
    assert "getDeploymentConfig and readDeploymentConfig" in patch_text
    assert "process.exit(0)" not in patch_text
    assert "include_granted_scopes" in patch_text
    assert "google/drive/individual/connector.py" in patch_text
    assert "google/gmail/individual/connector.py" in patch_text
    assert "auth user id" in patch_text
    assert "user_id=user_key" in patch_text
    assert "user_id=user_id" in patch_text
    assert "record.connectorId IN @connectors" in patch_text
    assert "CONNECTOR_OWNER" in patch_text
    assert "LET connectorAccess = recordDoc" in patch_text
    assert "LET connectorAppIds = UNIQUE" in patch_text
    assert "{{ type: 'CONNECTOR_OWNER'" in patch_text
    assert "main connector records bind" in patch_text
    assert "bind_vars=count_bind" in patch_text


def test_pipeshub_launcher_overwrite_preserves_local_secrets(tmp_path: Path) -> None:
    runner = CliRunner()
    runtime_dir = tmp_path / "pipeshub"

    first = runner.invoke(
        app,
        [
            "connectors",
            "pipeshub",
            "up",
            "--runtime-dir",
            str(runtime_dir),
            "--dry-run",
        ],
    )
    assert first.exit_code == 0, first.output
    first_env = _read_env(runtime_dir / ".env")

    second = runner.invoke(
        app,
        [
            "connectors",
            "pipeshub",
            "up",
            "--runtime-dir",
            str(runtime_dir),
            "--image-tag",
            "0.4.1",
            "--dry-run",
            "--overwrite",
        ],
    )
    assert second.exit_code == 0, second.output
    second_env = _read_env(runtime_dir / ".env")

    assert second_env["IMAGE_TAG"] == "0.4.1"
    for key in ("SECRET_KEY", "ARANGO_PASSWORD", "MONGO_PASSWORD", "QDRANT_API_KEY"):
        assert second_env[key] == first_env[key]


def test_pipeshub_inspect_reports_configured_connectors(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PIPESHUB_BEARER_AUTH", "token-123")
    requested_scopes: list[str] = []

    def fake_urlopen(request, timeout=30):  # noqa: ANN001, ARG001
        parsed = urlparse(request.full_url)
        query = parse_qs(parsed.query)
        assert parsed.path.endswith("/api/v1/connectors")
        assert query["page"] == ["1"]
        assert query["limit"] == ["200"]
        scope = query["scope"][0]
        requested_scopes.append(scope)
        assert request.get_header("Authorization") == "Bearer token-123"
        if scope == "personal":
            return _Response(
                {
                    "connectors": [
                        {
                            "connectorName": "onedrive",
                            "connectorId": "conn-one",
                            "isConfigured": True,
                            "isAuthenticated": True,
                            "isActive": True,
                        },
                        {"connectorName": "clickup", "connectorId": "conn-clickup"},
                    ]
                }
            )
        return _Response(
            {
                "connectors": [
                    {
                        "name": "Company Gmail",
                        "type": "google_gmail",
                        "_key": "conn-gmail",
                        "isConfigured": True,
                        "isAuthenticated": True,
                    },
                    {
                        "connectorName": "microsoftTeams",
                        "connectorId": "conn-teams",
                    },
                ]
            }
        )

    monkeypatch.setattr("vei.context.pipeshub.urlopen", fake_urlopen)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["context", "pipeshub", "inspect", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert requested_scopes == ["team", "personal"]
    assert payload["reachable"] is True
    assert "supported_connectors" not in payload
    assert "clickup" in payload["unsupported_ingestion_connectors"]
    connectors = {item["name"]: item for item in payload["configured_connectors"]}
    assert connectors["google_gmail"]["display_name"] == "Company Gmail"
    assert connectors["google_gmail"]["connector_id"] == "conn-gmail"
    assert connectors["google_gmail"]["supported_by_vei"] is True
    assert connectors["onedrive"]["is_active"] is True
    assert connectors["microsoftteams"]["connector_id"] == "conn-teams"
    assert connectors["microsoftteams"]["supported_by_vei"] is True
    assert connectors["clickup"]["connector_id"] == "conn-clickup"
    assert connectors["clickup"]["supported_by_vei"] is False
    assert not any("microsoftteams" in warning for warning in payload["warnings"])
    assert any("clickup" in warning for warning in payload["warnings"])


def test_pipeshub_inspect_auth_error_names_token_env(monkeypatch) -> None:
    def fake_urlopen(request, timeout=30):  # noqa: ANN001, ARG001
        raise HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr("vei.context.pipeshub.urlopen", fake_urlopen)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["context", "pipeshub", "inspect"],
    )

    assert result.exit_code != 0
    assert "PIPESHUB_BEARER_AUTH" in result.output


def test_pipeshub_capture_maps_records_to_context_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    records = [
        {
            "recordId": "gmail-1",
            "recordType": "MAIL",
            "connectorName": "google_gmail",
            "recordName": "Renewal thread",
            "subject": "Renewal thread",
            "threadId": "thread-1",
            "fromEmail": "maya@yourco.example",
            "toEmails": ["buyer@example.com"],
            "sourceCreatedAtTimestamp": "2026-03-01T10:00:00Z",
            "snippet": "Can legal review the renewal?",
        },
        {
            "recordId": "gmail-2",
            "recordType": "MAIL",
            "connectorName": "GMAIL WORKSPACE",
            "recordName": "Renewal thread follow-up",
            "subject": "Renewal thread follow-up",
            "threadId": "thread-2",
            "fromEmail": "legal@yourco.example",
            "toEmails": ["maya@yourco.example"],
            "sourceCreatedAtTimestamp": "2026-03-01T10:30:00Z",
            "snippet": "Legal can review today.",
        },
        {
            "recordId": "drive-1",
            "recordType": "FILE",
            "connectorName": "google_drive",
            "recordName": "Renewal plan",
            "mimeType": "application/vnd.google-apps.document",
            "sourceLastModifiedTimestamp": "2026-03-01T11:00:00Z",
            "snippet": "Renewal plan requires approval.",
            "permissions": [{"email": "legal@yourco.example", "type": "reader"}],
        },
        {
            "recordId": "drive-2",
            "recordType": "FILE",
            "connectorName": "DRIVE WORKSPACE",
            "recordName": "Renewal addendum",
            "mimeType": "application/vnd.google-apps.document",
            "sourceLastModifiedTimestamp": "2026-03-01T11:30:00Z",
            "snippet": "Renewal addendum requires approval.",
        },
        {
            "recordId": "jira-1",
            "recordType": "TICKET",
            "connectorName": "jira",
            "recordName": "LEGAL-7 review",
            "status": "open",
            "assigneeEmail": "maya@yourco.example",
            "sourceLastModifiedTimestamp": "2026-03-01T12:00:00Z",
            "description": "Legal approval blocks renewal.",
        },
        {
            "recordId": "conf-1",
            "recordType": "CONFLUENCE_PAGE",
            "connectorName": "confluence",
            "recordName": "Approval policy",
            "sourceLastModifiedTimestamp": "2026-03-01T13:00:00Z",
            "snippet": "Approvals require finance signoff.",
        },
        {
            "recordId": "sf-1",
            "recordType": "DEAL",
            "connectorName": "salesforce",
            "recordName": "Acme expansion",
            "stage": "legal_review",
            "owner": "maya@yourco.example",
            "sourceLastModifiedTimestamp": "2026-03-01T14:00:00Z",
        },
        {
            "recordId": "onedrive-1",
            "recordType": "FILE",
            "connectorName": "onedrive",
            "recordName": "MS account notes",
            "sourceLastModifiedTimestamp": "2026-03-01T15:00:00Z",
            "permissions": [
                {
                    "accessType": "CONNECTOR_OWNER",
                    "id": "onedrive-owner",
                    "name": "MS account notes",
                    "relationship": "OWNER",
                    "type": "FILE",
                }
            ],
        },
        {
            "recordId": "sharepoint-1",
            "recordType": "SHAREPOINT_PAGE",
            "connectorName": "sharepoint",
            "recordName": "SharePoint policy",
            "sourceLastModifiedTimestamp": "2026-03-01T15:30:00Z",
            "snippet": "SharePoint policy requires finance review.",
        },
        {
            "recordId": "box-1",
            "recordType": "FILE",
            "connectorName": "box",
            "recordName": "Box security checklist",
            "sourceLastModifiedTimestamp": "2026-03-01T15:40:00Z",
        },
        {
            "recordId": "dropbox-1",
            "recordType": "FILE",
            "connectorName": "dropbox",
            "recordName": "Dropbox renewal notes",
            "sourceLastModifiedTimestamp": "2026-03-01T15:50:00Z",
        },
        {
            "recordId": "outlook-1",
            "recordType": "MAIL",
            "connectorName": "outlook",
            "recordName": "Outlook renewal",
            "subject": "Outlook renewal",
            "threadId": "thread-2",
            "fromEmail": "sales@yourco.example",
            "toEmails": ["buyer@example.com"],
            "sourceCreatedAtTimestamp": "2026-03-01T16:00:00Z",
        },
        {
            "recordId": "sn-1",
            "recordType": "TICKET",
            "connectorName": "servicenow",
            "recordName": "INC001 renewal access",
            "status": "in_progress",
            "assigneeEmail": "it@yourco.example",
            "sourceLastModifiedTimestamp": "2026-03-01T17:00:00Z",
            "description": "Provision renewal workspace access.",
        },
        {
            "recordId": "teams-1",
            "recordType": "CHAT_MESSAGE",
            "connectorName": "microsoftTeams",
            "recordName": "Teams renewal standup",
            "sourceCreatedAtTimestamp": "2026-03-01T18:00:00Z",
            "chatRecord": {
                "teamName": "Sales",
                "channelName": "Renewals",
                "channelId": "teams-channel-1",
                "threadId": "teams-thread-1",
                "from": "maya@yourco.example",
                "body": {
                    "contentType": "html",
                    "content": "<p>Renewal owner confirmed.</p>",
                },
            },
        },
        {
            "recordId": "teams-2",
            "recordType": "CHANNEL_MESSAGE",
            "connectorName": "MICROSOFT_TEAMS",
            "recordName": "Teams renewal reply",
            "sourceCreatedAtTimestamp": "2026-03-01T18:05:00Z",
            "channelMessageRecord": {
                "teamName": "Sales",
                "channelName": "Renewals",
                "channelId": "teams-channel-1",
                "replyToId": "teams-thread-1",
                "from": "legal@yourco.example",
                "content": "Legal is unblocked.",
            },
        },
        {
            "recordId": "outlook-old",
            "recordType": "MAIL",
            "connectorName": "outlook",
            "recordName": "Old Outlook note",
            "subject": "Old Outlook note",
            "threadId": "thread-old",
            "sourceCreatedAtTimestamp": "2026-02-01T16:00:00Z",
        },
    ]

    def fake_urlopen(request, timeout=30):  # noqa: ANN001, ARG001
        url = request.full_url
        parsed = urlparse(url)
        if parsed.path.endswith("/api/v1/connectors"):
            return _Response(
                {
                    "connectors": [
                        {
                            "_key": "conn-gmail",
                            "type": "Gmail Workspace",
                            "name": "Company Gmail",
                        },
                        {
                            "_key": "conn-drive",
                            "type": "Drive Workspace",
                            "name": "Company Drive",
                        },
                        {"_key": "conn-jira", "type": "Jira"},
                        {"_key": "conn-confluence", "type": "Confluence"},
                        {"_key": "conn-salesforce", "type": "Salesforce"},
                        {"_key": "conn-onedrive", "type": "OneDrive"},
                        {"_key": "conn-sharepoint", "type": "SharePoint Online"},
                        {"_key": "conn-box", "type": "Box"},
                        {"_key": "conn-dropbox", "type": "Dropbox"},
                        {"_key": "conn-outlook", "type": "Outlook"},
                        {"_key": "conn-servicenow", "type": "ServiceNow"},
                        {"_key": "conn-teams", "type": "Microsoft Teams"},
                    ]
                }
            )
        if parsed.path.endswith("/api/v1/knowledgeBase/records"):
            query = parse_qs(parsed.query)
            assert "dateFrom" not in query
            assert "dateTo" not in query
            connector_filter = query["connectors"][0].split(",")
            for expected in (
                "conn-gmail",
                "GMAIL WORKSPACE",
                "google_gmail",
                "conn-drive",
                "DRIVE WORKSPACE",
                "google_drive",
                "conn-jira",
                "JIRA",
                "conn-confluence",
                "CONFLUENCE",
                "conn-salesforce",
                "SALESFORCE",
                "conn-onedrive",
                "ONEDRIVE",
                "conn-sharepoint",
                "SHAREPOINT ONLINE",
                "conn-box",
                "BOX",
                "conn-dropbox",
                "DROPBOX",
                "conn-outlook",
                "OUTLOOK",
                "conn-servicenow",
                "SERVICENOW",
                "conn-teams",
                "MICROSOFT TEAMS",
                "microsoftTeams",
            ):
                assert expected in connector_filter
            return _Response({"records": records, "total": len(records)})
        if "/api/v1/knowledgeBase/record/" in parsed.path:
            record_id = parsed.path.rsplit("/", 1)[-1]
            record = next(item for item in records if item["recordId"] == record_id)
            return _Response(
                {"record": record, "permissions": record.get("permissions", [])}
            )
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr("vei.context.pipeshub.urlopen", fake_urlopen)
    runner = CliRunner()
    workspace = tmp_path / "yourco"
    result = runner.invoke(
        app,
        [
            "context",
            "pipeshub",
            "capture",
            "--workspace",
            str(workspace),
            "--org",
            "YourCo",
            "--domain",
            "yourco.example",
            "--connector",
            "gmailworkspace",
            "--connector",
            "driveworkspace",
            "--connector",
            "jira",
            "--connector",
            "confluence",
            "--connector",
            "salesforce",
            "--connector",
            "onedrive",
            "--connector",
            "sharepoint",
            "--connector",
            "box",
            "--connector",
            "dropbox",
            "--connector",
            "outlook",
            "--connector",
            "servicenow",
            "--connector",
            "teams",
            "--since",
            "2026-03-01T00:00:00Z",
            "--until",
            "2026-03-02",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    snapshot_path = Path(payload["snapshot_path"])
    assert snapshot_path.exists()
    assert Path(payload["canonical_events_path"]).exists()
    assert Path(payload["canonical_index_path"]).exists()
    assert Path(payload["raw_records_path"]).exists()
    assert Path(payload["capture_manifest_path"]).exists()
    assert payload["raw_record_count"] == 15
    assert payload["pages_completed"] == 1
    assert payload["source_counts"]["gmail"] == 2
    assert payload["source_counts"]["google"] == 2
    assert payload["source_counts"]["outlook"] == 1
    assert payload["source_counts"]["teams"] == 2
    assert payload["source_counts"]["box"] == 1
    assert payload["source_counts"]["dropbox"] == 1
    assert payload["source_counts"]["servicenow"] == 1

    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    providers = {source["provider"] for source in snapshot["sources"]}
    assert {
        "gmail",
        "google",
        "jira",
        "salesforce",
        "confluence",
        "onedrive",
        "sharepoint",
        "box",
        "dropbox",
        "outlook",
        "teams",
        "servicenow",
    } <= providers
    assert snapshot["metadata"]["source_gateway"] == "pipeshub"

    events_path = Path(payload["canonical_events_path"])
    events = events_path.read_text(encoding="utf-8")
    event_payloads = [json.loads(line) for line in events.splitlines() if line.strip()]
    event_kinds = {event["kind"] for event in event_payloads}
    assert "gmail.message" in events
    assert "google.document" in events
    assert "jira.open" in events
    assert "salesforce.deal" in events
    assert "confluence.document" in events
    assert "onedrive.document" in events
    assert "sharepoint.document" in events
    assert "box.document" in events
    assert "dropbox.document" in events
    assert "outlook.message" in events
    assert "teams.message" in events
    assert "teams.reply" in events
    assert "servicenow.in_progress" in events
    assert "onedrive.share" not in event_kinds
    assert min(event["ts_ms"] for event in event_payloads) > 1_700_000_000_000
    index_payload = json.loads(
        Path(payload["canonical_index_path"]).read_text(encoding="utf-8")
    )
    assert index_payload["surface_counts"]["teams"] == 2

    verify_result = runner.invoke(
        app,
        [
            "context",
            "verify",
            "--snapshot",
            str(snapshot_path),
        ],
    )
    assert verify_result.exit_code == 0, verify_result.output
    verification = json.loads(verify_result.output)
    assert verification["ok"] is True
    assert not [
        check
        for check in verification["checks"]
        if check["code"] == "source.timestamp_span"
        and check["detail"] == "no parseable timestamps found"
    ]


def test_pipeshub_capture_resume_cli_requires_run_id(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "context",
            "pipeshub",
            "capture",
            "--workspace",
            str(tmp_path / "workspace"),
            "--org",
            "YourCo",
            "--resume",
        ],
    )

    assert result.exit_code != 0
    assert "--resume requires --run-id" in result.output


def test_pipeshub_capture_reads_nested_record_payloads(monkeypatch) -> None:
    records = [
        {
            "recordId": "outlook-nested-1",
            "recordType": "MAIL",
            "connectorName": "OUTLOOK",
            "recordName": "Nested Outlook parent",
            "sourceCreatedAtTimestamp": "2026-05-04T10:00:00Z",
            "mailRecord": {
                "threadId": "nested-thread-1",
                "subject": "Nested Outlook thread",
                "from": "sender@py-insights.example",
                "to": [{"email": "recipient@example.com"}],
                "cc": [{"address": "observer@example.com"}],
            },
        },
        {
            "recordId": "outlook-nested-2",
            "recordType": "MAIL",
            "connectorName": "OUTLOOK",
            "recordName": "Nested Outlook reply",
            "sourceCreatedAtTimestamp": "2026-05-04T10:15:00Z",
            "mailRecord": {
                "threadId": "nested-thread-1",
                "subject": "Nested Outlook thread",
                "from": "recipient@example.com",
                "to": [{"email": "sender@py-insights.example"}],
            },
        },
    ]

    def fake_urlopen(request, timeout=30):  # noqa: ANN001, ARG001
        parsed = urlparse(request.full_url)
        if parsed.path.endswith("/api/v1/connectors"):
            return _Response(
                {"connectors": [{"_key": "conn-outlook", "type": "Outlook"}]}
            )
        if parsed.path.endswith("/api/v1/knowledgeBase/records"):
            return _Response({"records": records, "total": len(records)})
        if "/api/v1/knowledgeBase/record/" in parsed.path:
            record_id = parsed.path.rsplit("/", 1)[-1]
            record = next(item for item in records if item["recordId"] == record_id)
            return _Response({"record": record})
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr("vei.context.pipeshub.urlopen", fake_urlopen)
    capture = capture_pipeshub_context(
        PipesHubClient(base_url="http://pipeshub.test", bearer_token="token"),
        organization_name="Py Insights",
        organization_domain="py-insights.example",
        connectors=["outlook"],
        since="2026-05-04T00:00:00Z",
        until="2026-05-05T00:00:00Z",
    )

    outlook = next(
        source for source in capture.snapshot.sources if source.provider == "outlook"
    )
    threads = outlook.data["threads"]
    assert len(threads) == 1
    assert threads[0]["thread_id"] == "nested-thread-1"
    assert threads[0]["subject"] == "Nested Outlook thread"
    assert len(threads[0]["messages"]) == 2
    assert threads[0]["messages"][0]["from"] == "sender@py-insights.example"
    assert threads[0]["messages"][0]["to"] == ["recipient@example.com"]
    assert threads[0]["messages"][0]["cc"] == ["observer@example.com"]


def test_pipeshub_capture_resumes_large_snapshot_without_content_backfill(
    tmp_path: Path,
    monkeypatch,
) -> None:
    records = _large_pipeshub_records()
    content_requests: list[str] = []

    def fake_urlopen(request, timeout=30):  # noqa: ANN001, ARG001
        parsed = urlparse(request.full_url)
        if parsed.path.endswith("/api/v1/connectors"):
            return _Response(
                {
                    "connectors": [
                        {"_key": "conn-outlook", "type": "Outlook"},
                        {"_key": "conn-onedrive", "type": "OneDrive"},
                        {"_key": "conn-jira", "type": "Jira"},
                    ]
                }
            )
        if parsed.path.endswith("/api/v1/knowledgeBase/records"):
            query = parse_qs(parsed.query)
            assert "dateFrom" not in query
            assert "dateTo" not in query
            assert set(query["connectors"][0].split(",")) >= {
                "conn-outlook",
                "conn-onedrive",
                "conn-jira",
                "OUTLOOK",
                "ONEDRIVE",
                "JIRA",
            }
            page = int(query["page"][0])
            limit = int(query["limit"][0])
            start = (page - 1) * limit
            return _Response(
                {
                    "records": records[start : start + limit],
                    "pagination": {"totalCount": len(records)},
                }
            )
        if "/api/v1/knowledgeBase/stream/record/" in parsed.path:
            content_requests.append(parsed.path.rsplit("/", 1)[-1])
            return _Response(body=b"streamed content")
        if "/api/v1/knowledgeBase/record/" in parsed.path:
            record_id = parsed.path.rsplit("/", 1)[-1]
            record = next(item for item in records if item["recordId"] == record_id)
            return _Response({"record": record, "permissions": []})
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr("vei.context.pipeshub.urlopen", fake_urlopen)
    client = PipesHubClient(base_url="http://pipeshub.test", bearer_token="token")
    workspace = tmp_path / "py-insights"
    run_id = "pipeshub_scale_rehearsal"
    manifest_path = (
        workspace
        / "imports"
        / "source_syncs"
        / "pipeshub"
        / run_id
        / "capture_manifest.json"
    )
    raw_records_path = manifest_path.with_name("records.jsonl")
    common_kwargs = {
        "organization_name": "Py Insights",
        "organization_domain": "py-insights.com",
        "connectors": ["outlook", "onedrive", "jira"],
        "since": "2026-03-01T00:00:00Z",
        "until": "2026-03-31T23:59:59Z",
        "include_content": False,
        "limit": 1000,
        "page_size": 75,
        "run_id": run_id,
        "manifest_path": manifest_path,
        "raw_records_path": raw_records_path,
    }

    with pytest.raises(RuntimeError, match="simulated PipesHub capture interruption"):
        capture_pipeshub_context(
            client,
            **common_kwargs,
            _interrupt_after_pages=3,
        )

    interrupted_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert interrupted_manifest["status"] == "interrupted"
    assert interrupted_manifest["pages_completed"] == 3
    assert interrupted_manifest["next_page"] == 4
    assert interrupted_manifest["raw_record_count"] == 225
    assert len(raw_records_path.read_text(encoding="utf-8").splitlines()) == 225

    resumed_capture = capture_pipeshub_context(
        client,
        **common_kwargs,
        resume=True,
    )
    report = write_pipeshub_capture(resumed_capture, workspace=workspace)

    clean_workspace = tmp_path / "clean"
    clean_capture = capture_pipeshub_context(
        client,
        **{
            **common_kwargs,
            "run_id": "pipeshub_scale_clean",
            "manifest_path": clean_workspace
            / "imports"
            / "source_syncs"
            / "pipeshub"
            / "pipeshub_scale_clean"
            / "capture_manifest.json",
            "raw_records_path": clean_workspace
            / "imports"
            / "source_syncs"
            / "pipeshub"
            / "pipeshub_scale_clean"
            / "records.jsonl",
        },
    )
    clean_report = write_pipeshub_capture(clean_capture, workspace=clean_workspace)

    assert report.resumed is True
    assert report.raw_record_count == 630
    assert report.duplicate_record_count == 1
    assert report.skipped_source_window_count == 25
    assert report.pages_completed == 9
    assert report.total_available == len(records)
    assert report.content_record_count == 0
    assert content_requests == []

    complete_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert complete_manifest["status"] == "complete"
    assert complete_manifest["raw_record_count"] == 630
    assert complete_manifest["skipped_source_window_count"] == 25

    resumed_ids = {record["recordId"] for record in resumed_capture.records}
    clean_ids = {record["recordId"] for record in clean_capture.records}
    assert resumed_ids == clean_ids
    assert all("vei_content_text" not in record for record in resumed_capture.records)

    runner = CliRunner()
    readiness_result = runner.invoke(
        app,
        ["context", "readiness", "--root", str(workspace), "--format", "json"],
    )
    assert readiness_result.exit_code == 0, readiness_result.output
    readiness = json.loads(readiness_result.output)
    assert readiness["ready_for_world_modeling"] is True
    assert readiness["event_count"] == 630
    assert readiness["surface_count"] >= 3
    assert readiness["readiness_label"] in {"ready", "rich"}

    workflow_dir = workspace / ".artifacts" / "workflow_mining"
    workflow_result = runner.invoke(
        app,
        [
            "workflow",
            "mine",
            "--source-dir",
            str(workspace),
            "--output",
            str(workflow_dir),
            "--limit",
            "10",
        ],
    )
    assert workflow_result.exit_code == 0, workflow_result.output
    workflow_payload = json.loads(
        (workflow_dir / "workflow_candidates.json").read_text()
    )
    assert workflow_payload["event_count"] == 630
    assert workflow_payload["candidate_count"] > 0

    wiki_result = runner.invoke(
        app,
        [
            "wiki",
            "build",
            "--source-dir",
            str(workspace),
            "--output",
            str(workspace / ".artifacts" / "wiki"),
        ],
    )
    assert wiki_result.exit_code == 0, wiki_result.output
    assert (workspace / ".artifacts" / "wiki" / "company_wiki.json").exists()

    events_path = Path(report.canonical_events_path)
    clean_events_path = Path(clean_report.canonical_events_path)
    events = {
        json.loads(line)["event_id"]
        for line in events_path.read_text(encoding="utf-8").splitlines()
    }
    clean_events = {
        json.loads(line)["event_id"]
        for line in clean_events_path.read_text(encoding="utf-8").splitlines()
    }
    assert events == clean_events


@pytest.mark.parametrize("connector", ["clickup"])
def test_pipeshub_capture_rejects_known_non_ingestion_connectors(
    connector: str,
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "context",
            "pipeshub",
            "capture",
            "--workspace",
            "unused",
            "--org",
            "YourCo",
            "--connector",
            connector,
            "--format",
            "json",
        ],
    )

    assert result.exit_code != 0
    assert "unsupported PipesHub ingestion connector" in result.output


def test_pipeshub_filter_expands_pipeshub_google_type_names() -> None:
    filters = _pipeshub_connector_filter_values(["google_gmail", "google_drive"])

    assert "GMAIL" in filters
    assert "GMAIL WORKSPACE" in filters
    assert "google_gmail" in filters
    assert "DRIVE" in filters
    assert "DRIVE WORKSPACE" in filters
    assert "google_drive" in filters


def test_pipeshub_filter_prefers_configured_connector_ids() -> None:
    filters = _pipeshub_connector_filter_values(
        ["google_gmail", "google_drive"],
        [
            {"_key": "gmail-instance", "type": "Gmail", "name": "Company Gmail"},
            {"_key": "drive-instance", "type": "Drive", "name": "Company Drive"},
        ],
    )

    assert filters.index("gmail-instance") < filters.index("GMAIL")
    assert filters.index("drive-instance") < filters.index("DRIVE")


def test_pipeshub_extract_total_reads_pagination_total_count() -> None:
    assert _extract_total({"pagination": {"totalCount": 655}}) == 655


def _large_pipeshub_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for thread_index in range(60):
        day = (thread_index % 28) + 1
        for message_index in range(8):
            records.append(
                {
                    "recordId": f"outlook-{thread_index:03d}-{message_index}",
                    "recordType": "MAIL",
                    "connectorName": "microsoft_outlook",
                    "recordName": f"Customer expansion thread {thread_index}",
                    "subject": f"Customer expansion thread {thread_index}",
                    "threadId": f"thread-{thread_index:03d}",
                    "fromEmail": "shruti@py-insights.com",
                    "toEmails": [f"customer-{thread_index}@example.com"],
                    "sourceCreatedAtTimestamp": (
                        f"2026-03-{day:02d}T10:{message_index:02d}:00Z"
                    ),
                    "snippet": (
                        "Expansion discussion with onboarding, pricing, "
                        "and implementation follow-up."
                    ),
                }
            )
    for document_index in range(80):
        day = (document_index % 28) + 1
        records.append(
            {
                "recordId": f"onedrive-{document_index:03d}",
                "recordType": "FILE",
                "connectorName": "microsoft_onedrive",
                "recordName": f"Implementation plan {document_index}",
                "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sourceLastModifiedTimestamp": f"2026-03-{day:02d}T12:00:00Z",
                "snippet": "Customer implementation plan and action owners.",
                "webUrl": f"https://py-insights.example/drive/{document_index}",
            }
        )
    for ticket_index in range(70):
        day = (ticket_index % 28) + 1
        records.append(
            {
                "recordId": f"jira-{ticket_index:03d}",
                "recordType": "TICKET",
                "connectorName": "jira",
                "recordName": f"PYI-{ticket_index} customer request",
                "ticketId": f"PYI-{ticket_index}",
                "status": "open",
                "assigneeEmail": "ops@py-insights.com",
                "sourceLastModifiedTimestamp": f"2026-03-{day:02d}T14:00:00Z",
                "description": "Customer implementation blocker requiring follow-up.",
            }
        )
    for old_index in range(20):
        records.append(
            {
                "recordId": f"outlook-old-{old_index:03d}",
                "recordType": "MAIL",
                "connectorName": "microsoft_outlook",
                "recordName": f"February thread {old_index}",
                "subject": f"February thread {old_index}",
                "threadId": f"old-thread-{old_index:03d}",
                "sourceCreatedAtTimestamp": f"2026-02-{old_index + 1:02d}T10:00:00Z",
            }
        )
    for missing_index in range(5):
        records.append(
            {
                "recordId": f"missing-time-{missing_index}",
                "recordType": "FILE",
                "connectorName": "microsoft_onedrive",
                "recordName": f"Undated import {missing_index}",
                "snippet": "Undated records are not eligible for bounded capture.",
            }
        )
    records.append(dict(records[0]))
    return records


def _read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values
