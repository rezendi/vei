from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.context.api import write_canonical_history_sidecars
from vei.context.providers.base import iso_now, join_url

DEFAULT_PIPESHUB_BASE_URL = "http://127.0.0.1:3000"
DEFAULT_PIPESHUB_TOKEN_ENV = "PIPESHUB_BEARER_AUTH"
DEFAULT_PAGE_SIZE = 100

PIPESHUB_SUPPORTED_CONNECTORS: dict[str, str] = {
    "drive": "Google Drive personal file sync",
    "driveworkspace": "Google Drive Workspace file sync",
    "gmail": "Gmail personal mail sync",
    "gmailworkspace": "Gmail Workspace mail sync",
    "confluence": "Confluence pages/blogposts/comments/files",
    "jira": "Jira projects/issues/comments/files",
    "salesforce": "Salesforce CRM objects",
    "onedrive": "Microsoft OneDrive files",
    "sharepointonline": "Microsoft SharePoint Online files/pages",
    "outlook": "Microsoft Outlook tenant mail",
    "outlookpersonal": "Microsoft Outlook personal mail",
    "box": "Box files",
    "dropbox": "Dropbox files",
    "dropboxpersonal": "Dropbox personal files",
    "notion": "Notion pages/databases",
    "servicenow": "ServiceNow work records",
    "linear": "Linear issues/projects",
    "github": "GitHub issues/pull requests",
    "gitlab": "GitLab issues/merge requests",
}

PIPESHUB_UNSUPPORTED_INGESTION: dict[str, str] = {
    "teams": "PipesHub exposes Microsoft Teams agent actions, but not a mature normalized Teams sync connector in the inspected build.",
    "microsoftteams": "PipesHub exposes Microsoft Teams agent actions, but not a mature normalized Teams sync connector in the inspected build.",
    "microsoft_teams": "PipesHub exposes Microsoft Teams agent actions, but not a mature normalized Teams sync connector in the inspected build.",
    "clickup": "PipesHub exposes ClickUp agent/tool code, but not a mature normalized ClickUp ingestion connector in the inspected build; use VEI's direct ClickUp provider for now.",
}

_CONNECTOR_ALIASES = {
    "google_drive": "driveworkspace",
    "googledrive": "driveworkspace",
    "google-drive": "driveworkspace",
    "google_gmail": "gmailworkspace",
    "googlegmail": "gmailworkspace",
    "google_mail": "gmailworkspace",
    "googlemail": "gmailworkspace",
    "google-mail": "gmailworkspace",
    "drive_workspace": "driveworkspace",
    "gmail_workspace": "gmailworkspace",
    "gmail-workspace": "gmailworkspace",
    "sharepoint": "sharepointonline",
    "sharepoint_online": "sharepointonline",
    "outlook_personal": "outlookpersonal",
    "microsoft_outlook": "outlook",
    "microsoft_onedrive": "onedrive",
    "microsoft_sharepoint": "sharepointonline",
    "microsoft_teams": "microsoftteams",
}

_MAIL_CONNECTORS = {"gmail", "gmailworkspace", "outlook", "outlookpersonal"}
_DOC_CONNECTORS = {
    "drive",
    "driveworkspace",
    "onedrive",
    "sharepointonline",
    "box",
    "dropbox",
    "dropboxpersonal",
    "confluence",
    "notion",
}
_WORK_CONNECTORS = {"jira", "servicenow", "linear", "github", "gitlab"}
_CRM_CONNECTORS = {"salesforce"}


class PipesHubConnectorSummary(BaseModel):
    name: str
    connector_id: str = ""
    display_name: str = ""
    status: str = ""
    is_configured: bool | None = None
    is_authenticated: bool | None = None
    is_active: bool | None = None
    record_count: int | None = None
    supported_by_vei: bool = False
    support_note: str = ""


class PipesHubInspectReport(BaseModel):
    base_url: str
    checked_at: str
    configured_connectors: list[PipesHubConnectorSummary] = Field(default_factory=list)
    supported_connectors: list[str] = Field(default_factory=list)
    unsupported_ingestion_connectors: dict[str, str] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class PipesHubCaptureReport(BaseModel):
    base_url: str
    run_id: str
    captured_at: str
    requested_connectors: list[str] = Field(default_factory=list)
    source_counts: dict[str, int] = Field(default_factory=dict)
    raw_record_count: int = 0
    detail_record_count: int = 0
    content_record_count: int = 0
    skipped_records: int = 0
    warnings: list[str] = Field(default_factory=list)
    raw_records_path: str = ""
    snapshot_path: str = ""
    canonical_events_path: str = ""
    canonical_index_path: str = ""


@dataclass(frozen=True)
class PipesHubCapture:
    snapshot: ContextSnapshot
    report: PipesHubCaptureReport
    records: list[dict[str, Any]]


class PipesHubClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_PIPESHUB_BASE_URL,
        bearer_token: str = "",
        timeout_s: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.timeout_s = timeout_s

    @classmethod
    def from_env(
        cls,
        *,
        base_url: str = "",
        token_env: str = DEFAULT_PIPESHUB_TOKEN_ENV,
        timeout_s: int = 30,
    ) -> "PipesHubClient":
        return cls(
            base_url=base_url
            or os.environ.get("PIPESHUB_BASE_URL", "")
            or DEFAULT_PIPESHUB_BASE_URL,
            bearer_token=os.environ.get(token_env, ""),
            timeout_s=timeout_s,
        )

    def list_connector_instances(self) -> list[dict[str, Any]]:
        payload = self.get_json("/api/v1/connectors", params={"page": 1, "limit": 200})
        return _extract_items(payload, keys=("connectors", "items", "data", "results"))

    def list_records(
        self,
        *,
        connectors: list[str],
        page: int,
        limit: int,
        date_from: str = "",
        date_to: str = "",
    ) -> tuple[list[dict[str, Any]], int | None]:
        params: dict[str, Any] = {"page": page, "limit": limit}
        if connectors:
            params["connectors"] = ",".join(connectors)
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to
        payload = self.get_json("/api/v1/knowledgeBase/records", params=params)
        records = _extract_items(payload, keys=("records", "items", "data", "results"))
        return records, _extract_total(payload)

    def get_record(self, record_id: str) -> dict[str, Any]:
        payload = self.get_json(f"/api/v1/knowledgeBase/record/{record_id}")
        if isinstance(payload, dict):
            record = payload.get("record")
            if isinstance(record, dict):
                merged = dict(record)
                for key in ("permissions", "metadata", "knowledgeBase", "folder"):
                    if key in payload and key not in merged:
                        merged[key] = payload[key]
                return merged
            return payload
        return {}

    def stream_record_text(self, record_id: str) -> str:
        url = self.url(
            f"/api/v1/knowledgeBase/stream/record/{record_id}",
            params={"convertTo": "txt"},
        )
        request = Request(url, headers=self.headers(), method="GET")
        try:
            with urlopen(request, timeout=self.timeout_s) as response:  # nosec B310
                return response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            raise RuntimeError(_http_error_message(exc)) from exc

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        request = Request(
            self.url(path, params=params), headers=self.headers(), method="GET"
        )
        try:
            with urlopen(request, timeout=self.timeout_s) as response:  # nosec B310
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(_http_error_message(exc)) from exc

    def url(self, path: str, *, params: dict[str, Any] | None = None) -> str:
        url = join_url(self.base_url, path)
        if not params:
            return url
        clean = {key: value for key, value in params.items() if value not in ("", None)}
        if not clean:
            return url
        return f"{url}?{urlencode(clean)}"

    def headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.bearer_token:
            token = self.bearer_token
            headers["Authorization"] = (
                token if token.lower().startswith("bearer ") else f"Bearer {token}"
            )
        return headers


def inspect_pipeshub(client: PipesHubClient) -> PipesHubInspectReport:
    warnings: list[str] = []
    raw_connectors = client.list_connector_instances()
    summaries = [_connector_summary(item) for item in raw_connectors]
    names = {item.name for item in summaries if item.name}
    for unsupported, reason in PIPESHUB_UNSUPPORTED_INGESTION.items():
        if unsupported in names:
            warnings.append(f"{unsupported}: {reason}")
    return PipesHubInspectReport(
        base_url=client.base_url,
        checked_at=iso_now(),
        configured_connectors=summaries,
        supported_connectors=sorted(PIPESHUB_SUPPORTED_CONNECTORS),
        unsupported_ingestion_connectors=dict(PIPESHUB_UNSUPPORTED_INGESTION),
        warnings=warnings,
    )


def capture_pipeshub_context(
    client: PipesHubClient,
    *,
    organization_name: str,
    organization_domain: str = "",
    connectors: list[str] | None = None,
    since: str = "",
    until: str = "",
    include_content: bool = False,
    limit: int = 1000,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> PipesHubCapture:
    normalized_connectors = _normalize_connectors(connectors or [])
    date_from = _date_bound_to_pipeshub_ms(since, "--since")
    date_to = _date_bound_to_pipeshub_ms(until, "--until")
    if date_from and date_to and int(date_to) < int(date_from):
        raise ValueError("--until must be greater than or equal to --since")
    unsupported = [
        name for name in normalized_connectors if name in PIPESHUB_UNSUPPORTED_INGESTION
    ]
    if unsupported:
        details = "; ".join(
            f"{name}: {PIPESHUB_UNSUPPORTED_INGESTION[name]}" for name in unsupported
        )
        raise ValueError(f"unsupported PipesHub ingestion connector(s): {details}")
    unknown = [
        name
        for name in normalized_connectors
        if name and name not in PIPESHUB_SUPPORTED_CONNECTORS
    ]
    if unknown:
        raise ValueError(
            "unknown or unsupported PipesHub connector(s): " + ", ".join(unknown)
        )

    records: list[dict[str, Any]] = []
    detail_count = 0
    content_count = 0
    warnings: list[str] = []
    seen_ids: set[str] = set()
    page = 1
    page_size = max(1, min(page_size, 200))
    while len(records) < limit:
        page_records, total = client.list_records(
            connectors=normalized_connectors,
            page=page,
            limit=min(page_size, limit - len(records)),
            date_from=date_from,
            date_to=date_to,
        )
        if not page_records:
            break
        for listed in page_records:
            record_id = _record_id(listed)
            if record_id and record_id in seen_ids:
                continue
            detail = listed
            if record_id:
                try:
                    detail = _merge_record(listed, client.get_record(record_id))
                    detail_count += 1
                except Exception as exc:  # pragma: no cover - covered through warnings
                    warnings.append(f"detail fetch failed for {record_id}: {exc}")
            if include_content and record_id:
                try:
                    text = client.stream_record_text(record_id)
                    if text:
                        detail["vei_content_text"] = text
                        content_count += 1
                except Exception as exc:  # pragma: no cover - covered through warnings
                    warnings.append(f"content fetch failed for {record_id}: {exc}")
            if record_id:
                seen_ids.add(record_id)
            records.append(detail)
            if len(records) >= limit:
                break
        if total is not None and page * page_size >= total:
            break
        page += 1

    sources, skipped = _records_to_sources(records)
    snapshot = ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=iso_now(),
        sources=sources,
        metadata={
            "snapshot_role": "company_history_bundle",
            "source_gateway": "pipeshub",
            "pipeshub": {
                "base_url": client.base_url,
                "requested_connectors": normalized_connectors,
                "include_content": include_content,
                "since": since,
                "until": until,
                "date_from_ms": date_from,
                "date_to_ms": date_to,
            },
        },
    )
    source_counts = {
        source.provider: _source_capture_count(source) for source in sources
    }
    report = PipesHubCaptureReport(
        base_url=client.base_url,
        run_id=_run_id(),
        captured_at=snapshot.captured_at,
        requested_connectors=normalized_connectors,
        source_counts=source_counts,
        raw_record_count=len(records),
        detail_record_count=detail_count,
        content_record_count=content_count,
        skipped_records=skipped,
        warnings=warnings,
    )
    return PipesHubCapture(snapshot=snapshot, report=report, records=records)


def write_pipeshub_capture(
    capture: PipesHubCapture,
    *,
    workspace: str | Path,
    output: str | Path | None = None,
) -> PipesHubCaptureReport:
    workspace_path = Path(workspace).expanduser().resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    sync_root = (
        workspace_path / "imports" / "source_syncs" / "pipeshub" / capture.report.run_id
    )
    sync_root.mkdir(parents=True, exist_ok=True)
    raw_path = sync_root / "records.jsonl"
    raw_path.write_text(
        "\n".join(json.dumps(record, sort_keys=True) for record in capture.records)
        + ("\n" if capture.records else ""),
        encoding="utf-8",
    )
    report_path = sync_root / "capture_report.json"
    snapshot_path = (
        Path(output).expanduser().resolve()
        if output
        else workspace_path / "context_snapshot.json"
    )
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        capture.snapshot.model_dump_json(indent=2), encoding="utf-8"
    )
    paths = write_canonical_history_sidecars(capture.snapshot, snapshot_path)
    report = capture.report.model_copy(
        update={
            "raw_records_path": str(raw_path),
            "snapshot_path": str(snapshot_path),
            "canonical_events_path": str(paths.events_path),
            "canonical_index_path": str(paths.index_path),
        }
    )
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    _write_source_registry(workspace_path, report, report_path=report_path)
    return report


def _write_source_registry(
    workspace: Path,
    report: PipesHubCaptureReport,
    *,
    report_path: Path,
) -> None:
    imports_dir = workspace / "imports"
    imports_dir.mkdir(parents=True, exist_ok=True)
    registry_path = imports_dir / "source_registry.json"
    history_path = imports_dir / "source_sync_history.json"
    now = report.captured_at
    source_entry = {
        "source_id": "pipeshub_live",
        "connector": "pipeshub",
        "config_path": "",
        "connector_mode": "live",
        "created_at": now,
        "updated_at": now,
        "metadata": {
            "base_url": report.base_url,
            "requested_connectors": report.requested_connectors,
        },
    }
    registry = _read_json_list(registry_path)
    registry = [
        item
        for item in registry
        if not (isinstance(item, dict) and item.get("source_id") == "pipeshub_live")
    ]
    registry.append(source_entry)
    registry_path.write_text(
        json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8"
    )
    history = _read_json_list(history_path)
    history.append(
        {
            "source_id": "pipeshub_live",
            "connector": "pipeshub",
            "synced_at": now,
            "status": "ok",
            "package_path": str(report_path.relative_to(workspace)),
            "record_counts": dict(report.source_counts),
            "metadata": {
                "sync_root": str(report_path.parent.relative_to(workspace)),
                "snapshot_path": (
                    str(Path(report.snapshot_path).relative_to(workspace))
                    if _is_relative_to(Path(report.snapshot_path), workspace)
                    else report.snapshot_path
                ),
            },
        }
    )
    history_path.write_text(
        json.dumps(history, indent=2, sort_keys=True), encoding="utf-8"
    )


def _records_to_sources(
    records: list[dict[str, Any]],
) -> tuple[list[ContextSourceResult], int]:
    mail_threads: dict[str, dict[str, Any]] = {}
    documents: list[dict[str, Any]] = []
    jira_issues: dict[str, dict[str, Any]] = {}
    crm_companies: list[dict[str, Any]] = []
    crm_contacts: list[dict[str, Any]] = []
    crm_deals: list[dict[str, Any]] = []
    linear_issues: list[dict[str, Any]] = []
    github_issues: list[dict[str, Any]] = []
    gitlab_issues: list[dict[str, Any]] = []
    skipped = 0

    for index, record in enumerate(records):
        connector = _connector_name(record)
        record_type = _record_type(record)
        if connector in _MAIL_CONNECTORS or record_type in {
            "mail",
            "group_mail",
            "email",
        }:
            _add_mail_record(mail_threads, record, fallback=index)
            continue
        if connector in _DOC_CONNECTORS or record_type in {
            "file",
            "webpage",
            "confluence_page",
            "confluence_blogpost",
            "sharepoint_page",
        }:
            documents.append(_document_record(record, fallback=index))
            continue
        if connector == "jira" or record_type in {"ticket"}:
            _add_ticket_record(jira_issues, record, fallback=index)
            continue
        if connector == "linear":
            linear_issues.append(_issue_record(record, fallback=index))
            continue
        if connector == "github":
            github_issues.append(_issue_record(record, fallback=index))
            continue
        if connector == "gitlab":
            gitlab_issues.append(_issue_record(record, fallback=index))
            continue
        if connector in _CRM_CONNECTORS or record_type in {"deal", "case", "product"}:
            if record_type in {"contact"}:
                crm_contacts.append(_contact_record(record, fallback=index))
            elif record_type in {"account", "company", "organization"}:
                crm_companies.append(_company_record(record, fallback=index))
            else:
                crm_deals.append(_deal_record(record, fallback=index))
            continue
        skipped += 1

    sources: list[ContextSourceResult] = []
    if mail_threads:
        threads = list(mail_threads.values())
        message_count = sum(len(thread.get("messages", [])) for thread in threads)
        sources.append(
            ContextSourceResult(
                provider="gmail",
                captured_at=iso_now(),
                status="ok",
                record_counts={"threads": len(threads), "messages": message_count},
                data={"threads": threads, "profile": {"source_gateway": "pipeshub"}},
            )
        )
    if documents:
        sources.append(
            ContextSourceResult(
                provider="google",
                captured_at=iso_now(),
                status="ok",
                record_counts={"documents": len(documents)},
                data={"documents": documents, "users": [], "drive_shares": []},
            )
        )
    if jira_issues:
        issues = list(jira_issues.values())
        sources.append(
            ContextSourceResult(
                provider="jira",
                captured_at=iso_now(),
                status="ok",
                record_counts={"issues": len(issues), "projects": 0},
                data={"issues": issues, "projects": []},
            )
        )
    if linear_issues:
        sources.append(
            ContextSourceResult(
                provider="linear",
                captured_at=iso_now(),
                status="ok",
                record_counts={"issues": len(linear_issues), "projects": 0},
                data={"issues": linear_issues, "projects": [], "cycles": []},
            )
        )
    if github_issues:
        sources.append(
            ContextSourceResult(
                provider="github",
                captured_at=iso_now(),
                status="ok",
                record_counts={"issues": len(github_issues), "pull_requests": 0},
                data={"issues": github_issues, "pull_requests": [], "repositories": []},
            )
        )
    if gitlab_issues:
        sources.append(
            ContextSourceResult(
                provider="gitlab",
                captured_at=iso_now(),
                status="ok",
                record_counts={"issues": len(gitlab_issues), "merge_requests": 0},
                data={"issues": gitlab_issues, "merge_requests": [], "projects": []},
            )
        )
    if crm_companies or crm_contacts or crm_deals:
        sources.append(
            ContextSourceResult(
                provider="salesforce",
                captured_at=iso_now(),
                status="ok",
                record_counts={
                    "companies": len(crm_companies),
                    "contacts": len(crm_contacts),
                    "deals": len(crm_deals),
                },
                data={
                    "companies": crm_companies,
                    "contacts": crm_contacts,
                    "deals": crm_deals,
                },
            )
        )
    return sources, skipped


def _add_mail_record(
    threads: dict[str, dict[str, Any]], record: dict[str, Any], *, fallback: int
) -> None:
    thread_id = _text_field(
        record, "threadId", "thread_id", "conversationId"
    ) or _record_id(record)
    if not thread_id:
        thread_id = f"pipeshub-mail-{fallback + 1}"
    subject = (
        _text_field(record, "subject", "recordName", "record_name", "name") or thread_id
    )
    thread = threads.setdefault(
        thread_id,
        {"thread_id": thread_id, "subject": subject, "messages": []},
    )
    timestamp = _timestamp(record)
    message = {
        "message_id": _record_id(record)
        or f"{thread_id}-{len(thread['messages']) + 1}",
        "thread_id": thread_id,
        "subject": subject,
        "from": _text_field(
            record, "fromEmail", "from_email", "sender", "creatorEmail"
        ),
        "to": _list_field(record, "toEmails", "to_emails", "recipients"),
        "cc": _list_field(record, "ccEmails", "cc_emails"),
        "timestamp": timestamp,
        "date": timestamp,
        "body_text": _body(record),
        "metadata": _provenance(record),
    }
    thread["messages"].append(message)


def _document_record(record: dict[str, Any], *, fallback: int) -> dict[str, Any]:
    record_id = _record_id(record) or f"pipeshub-doc-{fallback + 1}"
    return {
        "doc_id": record_id,
        "title": _text_field(record, "recordName", "record_name", "name", "title")
        or record_id,
        "body": _body(record),
        "owner": _text_field(
            record, "owner", "createdBy", "creatorEmail", "ownerEmail"
        ),
        "modified_time": _timestamp(record),
        "mime_type": _text_field(record, "mimeType", "mime_type"),
        "web_url": _text_field(record, "webUrl", "weburl", "web_url", "url"),
        "permissions": _permissions(record),
        "metadata": _provenance(record),
    }


def _add_ticket_record(
    issues: dict[str, dict[str, Any]], record: dict[str, Any], *, fallback: int
) -> None:
    ticket_id = (
        _text_field(record, "ticketId", "ticket_id", "key", "externalRecordId")
        or _record_id(record)
        or f"pipeshub-ticket-{fallback + 1}"
    )
    body = _body(record)
    record_type = _record_type(record)
    if record_type in {"comment", "inline_comment"}:
        parent = _text_field(
            record, "parentExternalRecordId", "parent_record_id", "parentId"
        )
        timestamp = _timestamp(record)
        ticket = issues.setdefault(
            parent or ticket_id,
            {
                "ticket_id": parent or ticket_id,
                "title": parent or ticket_id,
                "status": "",
                "assignee": "",
                "updated_at": timestamp,
                "updated": timestamp,
                "description": "",
                "comments": [],
            },
        )
        ticket["comments"].append(
            {
                "id": _record_id(record),
                "author": _text_field(record, "author", "creatorEmail", "createdBy"),
                "body": body,
                "created": timestamp,
            }
        )
        return
    timestamp = _timestamp(record)
    issues[ticket_id] = {
        "ticket_id": ticket_id,
        "title": _text_field(record, "recordName", "record_name", "summary", "title")
        or ticket_id,
        "status": _text_field(record, "status", "deliveryStatus", "delivery_status"),
        "assignee": _text_field(record, "assignee", "assigneeEmail", "assignee_email"),
        "updated_at": timestamp,
        "updated": timestamp,
        "description": body,
        "comments": [],
        "metadata": _provenance(record),
    }


def _issue_record(record: dict[str, Any], *, fallback: int) -> dict[str, Any]:
    issue_id = _record_id(record) or f"pipeshub-issue-{fallback + 1}"
    timestamp = _timestamp(record)
    return {
        "id": issue_id,
        "title": _text_field(record, "recordName", "record_name", "title") or issue_id,
        "body": _body(record),
        "state": _text_field(record, "status", "state"),
        "author": _text_field(record, "creatorEmail", "createdBy", "author"),
        "updated_at": timestamp,
        "updated": timestamp,
        "comments": [],
        "metadata": _provenance(record),
    }


def _company_record(record: dict[str, Any], *, fallback: int) -> dict[str, Any]:
    company_id = _record_id(record) or f"pipeshub-company-{fallback + 1}"
    created_at = _created_timestamp(record)
    updated_at = _timestamp(record)
    return {
        "id": company_id,
        "name": _text_field(record, "recordName", "record_name", "name") or company_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "created_ms": created_at,
        "updated_ms": updated_at,
        "metadata": _provenance(record),
    }


def _contact_record(record: dict[str, Any], *, fallback: int) -> dict[str, Any]:
    contact_id = _record_id(record) or f"pipeshub-contact-{fallback + 1}"
    created_at = _created_timestamp(record)
    updated_at = _timestamp(record)
    return {
        "id": contact_id,
        "email": _text_field(record, "email", "contactEmail", "recordName"),
        "first_name": _text_field(record, "firstName", "first_name"),
        "last_name": _text_field(record, "lastName", "last_name"),
        "company_id": _text_field(
            record, "companyId", "accountId", "parentExternalRecordId"
        ),
        "created_at": created_at,
        "updated_at": updated_at,
        "created_ms": created_at,
        "updated_ms": updated_at,
        "metadata": _provenance(record),
    }


def _deal_record(record: dict[str, Any], *, fallback: int) -> dict[str, Any]:
    deal_id = _record_id(record) or f"pipeshub-deal-{fallback + 1}"
    created_at = _created_timestamp(record)
    updated_at = _timestamp(record)
    return {
        "id": deal_id,
        "name": _text_field(record, "recordName", "record_name", "name", "subject")
        or deal_id,
        "stage": _text_field(record, "stage", "status", "type"),
        "owner": _text_field(record, "owner", "assignee", "creatorEmail"),
        "amount": _text_field(record, "amount", "value"),
        "company_id": _text_field(
            record, "companyId", "accountId", "parentExternalRecordId"
        ),
        "contact_id": _text_field(record, "contactId"),
        "created_at": created_at,
        "updated_at": updated_at,
        "created_ms": created_at,
        "updated_ms": updated_at,
        "history": [],
        "metadata": _provenance(record),
    }


def _connector_summary(item: dict[str, Any]) -> PipesHubConnectorSummary:
    name = _normalize_connector(
        _text_field(
            item,
            "connectorName",
            "connector_name",
            "connectorType",
            "connector_type",
            "type",
            "app",
            "origin",
            "source",
            "name",
        )
    )
    connector_id = _text_field(item, "connectorId", "connector_id", "id", "_key")
    supported = name in PIPESHUB_SUPPORTED_CONNECTORS
    support_note = PIPESHUB_SUPPORTED_CONNECTORS.get(
        name, PIPESHUB_UNSUPPORTED_INGESTION.get(name, "Not mapped by VEI yet.")
    )
    return PipesHubConnectorSummary(
        name=name,
        connector_id=connector_id,
        display_name=_text_field(item, "displayName", "display_name", "name") or name,
        status=_text_field(item, "status", "state", "syncStatus", "sync_status"),
        is_configured=_bool_or_none(
            item, "isConfigured", "is_configured", "configured"
        ),
        is_authenticated=_bool_or_none(
            item, "isAuthenticated", "is_authenticated", "authenticated"
        ),
        is_active=_bool_or_none(item, "isActive", "is_active", "active"),
        record_count=_int_or_none(item, "recordCount", "record_count", "records"),
        supported_by_vei=supported,
        support_note=support_note,
    )


def _normalize_connectors(connectors: list[str]) -> list[str]:
    normalized: list[str] = []
    for connector in connectors:
        name = _normalize_connector(connector)
        if not name:
            continue
        if name not in normalized:
            normalized.append(name)
    return normalized


def _normalize_connector(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    compact = re.sub(r"[^a-z0-9]+", "", raw.lower())
    lowered = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return _CONNECTOR_ALIASES.get(lowered) or _CONNECTOR_ALIASES.get(compact) or lowered


def _extract_items(payload: Any, *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_items(value, keys=keys)
            if nested:
                return nested
    return []


def _extract_total(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("total", "totalRecords", "total_records", "count"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_total(data)
    return None


def _merge_record(listed: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    merged = dict(listed)
    merged.update(detail)
    for key in ("permissions", "metadata"):
        if key in listed and key not in detail:
            merged[key] = listed[key]
    return merged


def _record_id(record: dict[str, Any]) -> str:
    return _text_field(record, "recordId", "record_id", "id", "_key")


def _record_type(record: dict[str, Any]) -> str:
    return _normalize_connector(
        _text_field(record, "recordType", "record_type", "type")
    )


def _connector_name(record: dict[str, Any]) -> str:
    return _normalize_connector(
        _text_field(
            record,
            "connectorName",
            "connector_name",
            "connectorType",
            "connector_type",
            "connector",
            "app",
            "origin",
            "source",
        )
    )


def _timestamp(record: dict[str, Any]) -> str:
    return _text_field(
        record,
        "sourceLastModifiedTimestamp",
        "source_updated_at",
        "sourceUpdatedAtTimestamp",
        "updatedAt",
        "updated_at",
        "updated",
        "modifiedTime",
        "modified_time",
        "sourceCreatedAtTimestamp",
        "source_created_at",
        "createdAt",
        "created_at",
        "created",
    )


def _created_timestamp(record: dict[str, Any]) -> str:
    return _text_field(
        record,
        "sourceCreatedAtTimestamp",
        "source_created_at",
        "createdAt",
        "created_at",
        "created",
        "sourceLastModifiedTimestamp",
        "source_updated_at",
    )


def _body(record: dict[str, Any]) -> str:
    direct = _text_field(
        record,
        "vei_content_text",
        "body",
        "bodyText",
        "body_text",
        "description",
        "snippet",
        "summary",
        "text",
        "content",
    )
    if direct:
        return direct
    containers = record.get("blockContainers") or record.get("block_containers") or []
    if isinstance(containers, list):
        parts: list[str] = []
        for container in containers:
            if isinstance(container, dict):
                text = _text_field(container, "text", "content", "body")
                if text:
                    parts.append(text)
        return "\n".join(parts)
    return ""


def _permissions(record: dict[str, Any]) -> list[dict[str, Any]]:
    permissions = record.get("permissions") or []
    if not isinstance(permissions, list):
        return []
    parsed: list[dict[str, Any]] = []
    for index, permission in enumerate(permissions):
        if not isinstance(permission, dict):
            continue
        shared_with = _text_field(
            permission, "email", "entityId", "entity_id", "name", "principal"
        )
        parsed.append(
            {
                "id": _text_field(permission, "id", "_key")
                or f"permission-{index + 1}",
                "shared_with": [shared_with] if shared_with else [],
                "granted_by": _text_field(permission, "grantedBy", "granted_by"),
                "role": _text_field(permission, "permissionType", "type", "role"),
                "created": _text_field(permission, "createdAt", "created_at"),
            }
        )
    return parsed


def _provenance(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_gateway": "pipeshub",
        "pipeshub_record_id": _record_id(record),
        "pipeshub_connector": _connector_name(record),
        "pipeshub_connector_id": _text_field(record, "connectorId", "connector_id"),
        "external_record_id": _text_field(
            record, "externalRecordId", "external_record_id"
        ),
        "record_type": _record_type(record),
        "web_url": _text_field(record, "webUrl", "weburl", "web_url", "url"),
    }


def _text_field(record: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _field(record, key)
        if value in (None, ""):
            continue
        if isinstance(value, (list, dict)):
            continue
        return str(value).strip()
    return ""


def _date_bound_to_pipeshub_ms(value: str, option_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    parseable = text
    if parseable.endswith("Z"):
        parseable = parseable[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(parseable)
    except ValueError as exc:
        raise ValueError(
            f"{option_name} must be an ISO-8601 date/datetime or a millisecond timestamp"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return str(int(parsed.timestamp() * 1000))


def _source_capture_count(source: ContextSourceResult) -> int:
    counts = source.record_counts
    if source.provider == "gmail":
        return counts.get("messages") or counts.get("threads") or 0
    if source.provider == "google":
        return counts.get("documents") or 0
    if source.provider in {"jira", "linear"}:
        return counts.get("issues") or 0
    if source.provider == "github":
        return counts.get("issues", 0) + counts.get("pull_requests", 0)
    if source.provider == "gitlab":
        return counts.get("issues", 0) + counts.get("merge_requests", 0)
    return sum(count for count in counts.values() if count > 0)


def _http_error_message(exc: HTTPError) -> str:
    if exc.code == 401:
        return (
            "PipesHub returned 401 Unauthorized. Set PIPESHUB_BEARER_AUTH to a "
            "PipesHub bearer token, or pass --token-env with the environment variable "
            "that contains it."
        )
    return f"PipesHub returned HTTP {exc.code}: {exc.reason}"


def _list_field(record: dict[str, Any], *keys: str) -> list[str]:
    for key in keys:
        value = _field(record, key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str) and value.strip():
            return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _field(record: dict[str, Any], key: str) -> Any:
    if key in record:
        return record[key]
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
    if snake in record:
        return record[snake]
    metadata = record.get("metadata")
    if isinstance(metadata, dict):
        if key in metadata:
            return metadata[key]
        if snake in metadata:
            return metadata[snake]
    semantic = record.get("semanticMetadata") or record.get("semantic_metadata")
    if isinstance(semantic, dict):
        if key in semantic:
            return semantic[key]
        if snake in semantic:
            return semantic[snake]
    return None


def _bool_or_none(record: dict[str, Any], *keys: str) -> bool | None:
    for key in keys:
        value = _field(record, key)
        if isinstance(value, bool):
            return value
    return None


def _int_or_none(record: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _field(record, key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _read_json_list(path: Path) -> list[Any]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _run_id() -> str:
    stamp = datetime.now(UTC).replace(microsecond=0).isoformat()
    return "pipeshub_" + stamp.replace("+00:00", "Z").replace(":", "-")
