from __future__ import annotations

import json
import os
import re
import time
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

# Map a PipesHub connectorName to a VEI provider name only where the names
# actually differ. Anything not listed passes through unchanged, so adding
# a new PipesHub connector requires no VEI patch.
_PIPESHUB_TO_VEI_PROVIDER: dict[str, str] = {
    "drive": "google",
    "driveworkspace": "google",
    "drive_workspace": "google",
    "google_drive": "google",
    "google_drive_workspace": "google",
    "gmail": "gmail",
    "gmailworkspace": "gmail",
    "gmail_workspace": "gmail",
    "google_gmail": "gmail",
    "google_mail": "gmail",
    "google_gmail_workspace": "gmail",
    "google_mail_workspace": "gmail",
    "microsoft_onedrive": "onedrive",
    "microsoft_outlook": "outlook",
    "microsoft_sharepoint": "sharepoint",
    "sharepointonline": "sharepoint",
    "sharepoint_online": "sharepoint",
    "outlookpersonal": "outlook",
    "outlook_personal": "outlook",
    "dropboxpersonal": "dropbox",
    "dropbox_personal": "dropbox",
    "microsoftteams": "teams",
    "microsoft_teams": "teams",
}

_PIPESHUB_CONNECTOR_FILTER_VALUES: dict[str, tuple[str, ...]] = {
    "gmail": ("GMAIL", "GMAIL WORKSPACE", "google_gmail", "gmail"),
    "google_gmail": ("GMAIL", "GMAIL WORKSPACE", "google_gmail", "gmail"),
    "google_mail": ("GMAIL", "GMAIL WORKSPACE", "google_gmail", "gmail"),
    "gmailworkspace": (
        "GMAIL WORKSPACE",
        "google_gmail",
        "gmailworkspace",
        "gmail_workspace",
    ),
    "gmail_workspace": (
        "GMAIL WORKSPACE",
        "google_gmail",
        "gmailworkspace",
        "gmail_workspace",
    ),
    "google_gmail_workspace": (
        "GMAIL WORKSPACE",
        "google_gmail",
        "gmailworkspace",
        "gmail_workspace",
    ),
    "google_mail_workspace": (
        "GMAIL WORKSPACE",
        "google_gmail",
        "gmailworkspace",
        "gmail_workspace",
    ),
    "drive": ("DRIVE", "DRIVE WORKSPACE", "google_drive", "drive"),
    "google_drive": ("DRIVE", "DRIVE WORKSPACE", "google_drive", "drive"),
    "driveworkspace": (
        "DRIVE WORKSPACE",
        "google_drive",
        "driveworkspace",
        "drive_workspace",
    ),
    "drive_workspace": (
        "DRIVE WORKSPACE",
        "google_drive",
        "driveworkspace",
        "drive_workspace",
    ),
    "google_drive_workspace": (
        "DRIVE WORKSPACE",
        "google_drive",
        "driveworkspace",
        "drive_workspace",
    ),
    "confluence": ("CONFLUENCE", "confluence"),
    "jira": ("JIRA", "jira"),
    "salesforce": ("SALESFORCE", "salesforce"),
    "onedrive": ("ONEDRIVE", "microsoft_onedrive", "onedrive"),
    "microsoft_onedrive": ("ONEDRIVE", "microsoft_onedrive", "onedrive"),
    "sharepoint": (
        "SHAREPOINT ONLINE",
        "microsoft_sharepoint",
        "sharepointonline",
        "sharepoint_online",
    ),
    "sharepointonline": (
        "SHAREPOINT ONLINE",
        "microsoft_sharepoint",
        "sharepointonline",
        "sharepoint_online",
    ),
    "sharepoint_online": (
        "SHAREPOINT ONLINE",
        "microsoft_sharepoint",
        "sharepointonline",
        "sharepoint_online",
    ),
    "microsoft_sharepoint": (
        "SHAREPOINT ONLINE",
        "microsoft_sharepoint",
        "sharepointonline",
        "sharepoint_online",
    ),
    "outlook": ("OUTLOOK", "microsoft_outlook", "outlook"),
    "microsoft_outlook": ("OUTLOOK", "microsoft_outlook", "outlook"),
    "outlookpersonal": (
        "OUTLOOK PERSONAL",
        "microsoft_outlook",
        "outlookpersonal",
        "outlook_personal",
    ),
    "outlook_personal": (
        "OUTLOOK PERSONAL",
        "microsoft_outlook",
        "outlookpersonal",
        "outlook_personal",
    ),
    "box": ("BOX", "box"),
    "dropbox": ("DROPBOX", "dropbox"),
    "dropboxpersonal": ("DROPBOX PERSONAL", "dropboxpersonal", "dropbox_personal"),
    "dropbox_personal": ("DROPBOX PERSONAL", "dropboxpersonal", "dropbox_personal"),
    "notion": ("NOTION", "notion"),
    "servicenow": ("SERVICENOW", "servicenow"),
    "linear": ("LINEAR", "linear"),
    "github": ("GITHUB", "github"),
    "gitlab": ("GITLAB", "gitlab"),
}

PIPESHUB_UNSUPPORTED_INGESTION: dict[str, str] = {
    "teams": "PipesHub exposes Microsoft Teams agent/actions code, but not a mature normalized Teams sync connector in the inspected build.",
    "microsoftteams": "PipesHub exposes Microsoft Teams agent/actions code, but not a mature normalized Teams sync connector in the inspected build.",
    "microsoft_teams": "PipesHub exposes Microsoft Teams agent/actions code, but not a mature normalized Teams sync connector in the inspected build.",
    "clickup": "PipesHub exposes ClickUp agent/tool code, but not a mature normalized ClickUp ingestion connector in the inspected build; use VEI's direct ClickUp provider for now.",
}

# PipesHub recordType → shape. Drives which converter runs and which
# bucket the record lands in within its provider's source. Unknown types
# go to "other" and are emitted under the source's `other` key.
_SHAPE_BY_TYPE: dict[str, str] = {
    "mail": "mail",
    "email": "mail",
    "group_mail": "mail",
    "file": "document",
    "webpage": "document",
    "confluence_page": "document",
    "confluence_blogpost": "document",
    "sharepoint_page": "document",
    "notion_page": "document",
    "drive_file": "document",
    "onedrive_file": "document",
    "box_file": "document",
    "dropbox_file": "document",
    "ticket": "ticket",
    "comment": "ticket",
    "inline_comment": "ticket",
    "issue": "issue",
    "pull_request": "issue",
    "merge_request": "issue",
    "contact": "contact",
    "account": "company",
    "company": "company",
    "organization": "company",
    "deal": "deal",
    "case": "deal",
    "product": "deal",
}


class PipesHubConnectorSummary(BaseModel):
    name: str
    connector_id: str = ""
    display_name: str = ""
    status: str = ""
    is_configured: bool | None = None
    is_authenticated: bool | None = None
    is_active: bool | None = None
    record_count: int | None = None
    supported_by_vei: bool | None = None
    support_note: str = ""


class PipesHubInspectReport(BaseModel):
    base_url: str
    checked_at: str
    reachable: bool = True
    configured_connectors: list[PipesHubConnectorSummary] = Field(default_factory=list)
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
        connectors: list[dict[str, Any]] = []
        seen: set[str] = set()
        for scope in ("team", "personal"):
            payload = self.get_json(
                "/api/v1/connectors",
                params={"scope": scope, "page": 1, "limit": 200},
            )
            for item in _extract_items(
                payload, keys=("connectors", "items", "data", "results")
            ):
                if not isinstance(item, dict):
                    continue
                item_with_scope = dict(item)
                item_with_scope.setdefault("scope", scope)
                connector_key = str(
                    item_with_scope.get("connectorId")
                    or item_with_scope.get("_id")
                    or item_with_scope.get("_key")
                    or item_with_scope.get("id")
                    or (
                        item_with_scope.get("connectorName"),
                        item_with_scope.get("type"),
                        item_with_scope.get("name"),
                        item_with_scope.get("scope"),
                    )
                )
                if connector_key in seen:
                    continue
                seen.add(connector_key)
                connectors.append(item_with_scope)
        return connectors

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
        for attempt in range(4):
            try:
                with urlopen(request, timeout=self.timeout_s) as response:  # nosec B310
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:
                if exc.code == 429 and attempt < 3:
                    retry_after = exc.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else 2.0 * (attempt + 1)
                    time.sleep(delay)
                    continue
                raise RuntimeError(_http_error_message(exc)) from exc
        return {}

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
    raw_connectors = client.list_connector_instances()
    summaries = [_connector_summary(item) for item in raw_connectors]
    warnings = []
    for connector in summaries:
        if connector.supported_by_vei is False and connector.support_note:
            warnings.append(f"{connector.name}: {connector.support_note}")
    return PipesHubInspectReport(
        base_url=client.base_url,
        checked_at=iso_now(),
        reachable=True,
        configured_connectors=summaries,
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
    unsupported = [
        name for name in normalized_connectors if _unsupported_ingestion_reason(name)
    ]
    if unsupported:
        details = "; ".join(
            f"{name}: {_unsupported_ingestion_reason(name)}" for name in unsupported
        )
        raise ValueError(f"unsupported PipesHub ingestion connector(s): {details}")
    date_from = _date_bound_to_pipeshub_ms(since, "--since")
    date_to = _date_bound_to_pipeshub_ms(until, "--until")
    if date_from and date_to and int(date_to) < int(date_from):
        raise ValueError("--until must be greater than or equal to --since")

    records: list[dict[str, Any]] = []
    detail_count = 0
    content_count = 0
    warnings: list[str] = []
    raw_connectors: list[dict[str, Any]] = []
    if normalized_connectors:
        try:
            raw_connectors = client.list_connector_instances()
        except Exception as exc:
            warnings.append(
                "configured connector lookup failed; falling back to connector names: "
                f"{exc}"
            )
    query_connectors = _pipeshub_connector_filter_values(
        normalized_connectors, raw_connectors
    )
    seen_ids: set[str] = set()
    page = 1
    page_size = max(1, min(page_size, 200))
    while len(records) < limit:
        request_limit = page_size
        page_records, total = client.list_records(
            connectors=query_connectors,
            page=page,
            limit=request_limit,
        )
        if not page_records:
            break
        for listed in page_records:
            record_id = _record_id(listed)
            if record_id and record_id in seen_ids:
                continue
            listed_has_source_time = bool(_record_source_timestamps_ms(listed))
            if listed_has_source_time and not _record_in_source_window(
                listed, date_from, date_to
            ):
                if record_id:
                    seen_ids.add(record_id)
                continue
            detail = listed
            if record_id:
                try:
                    detail = _merge_record(listed, client.get_record(record_id))
                    detail_count += 1
                except Exception as exc:  # pragma: no cover - covered through warnings
                    warnings.append(f"detail fetch failed for {record_id}: {exc}")
            if record_id:
                seen_ids.add(record_id)
            if not listed_has_source_time and not _record_in_source_window(
                detail, date_from, date_to
            ):
                continue
            if include_content and record_id:
                try:
                    text = client.stream_record_text(record_id)
                    if text:
                        detail["vei_content_text"] = text
                        content_count += 1
                except Exception as exc:  # pragma: no cover - covered through warnings
                    warnings.append(f"content fetch failed for {record_id}: {exc}")
            records.append(detail)
            if len(records) >= limit:
                break
        if len(page_records) < request_limit:
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
                "query_connectors": query_connectors,
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
    """Bucket records by (upstream provider, shape).

    Provider comes from the record's connectorName, normalized to whatever
    VEI already calls that upstream system. Shape comes from recordType.
    Records whose recordType isn't recognized still get emitted, under
    their provider's `other` key.
    """
    from collections import defaultdict

    buckets: dict[str, dict[str, Any]] = defaultdict(_empty_bucket)
    unmapped = 0

    for index, record in enumerate(records):
        provider = _vei_provider(_connector_name(record))
        bucket = buckets[provider]
        shape = _SHAPE_BY_TYPE.get(_record_type(record), "other")

        if shape == "mail":
            _add_mail_record(bucket["mail_threads"], record, fallback=index)
        elif shape == "document":
            bucket["documents"].append(_document_record(record, fallback=index))
        elif shape == "ticket":
            _add_ticket_record(bucket["tickets"], record, fallback=index)
        elif shape == "issue":
            bucket["issues"].append(_issue_record(record, fallback=index))
        elif shape == "company":
            bucket["companies"].append(_company_record(record, fallback=index))
        elif shape == "contact":
            bucket["contacts"].append(_contact_record(record, fallback=index))
        elif shape == "deal":
            bucket["deals"].append(_deal_record(record, fallback=index))
        else:
            bucket["other"].append(
                {
                    "record_type": _record_type(record) or "unknown",
                    "payload": record,
                    "metadata": _provenance(record),
                }
            )
            unmapped += 1

    sources = [
        _assemble_source(provider, bucket)
        for provider, bucket in sorted(buckets.items())
    ]
    return sources, unmapped


def _empty_bucket() -> dict[str, Any]:
    return {
        "mail_threads": {},
        "documents": [],
        "tickets": {},
        "issues": [],
        "companies": [],
        "contacts": [],
        "deals": [],
        "other": [],
    }


def _assemble_source(provider: str, bucket: dict[str, Any]) -> ContextSourceResult:
    """Pack a provider's buckets into the data shape its readers expect.

    Each provider keeps the data keys VEI's existing capture pipelines
    use (gmail → threads, jira/work providers → issues, salesforce →
    companies/contacts/deals). Unknown-recordType records land under
    `other` so they're still discoverable downstream.
    """
    threads = list(bucket["mail_threads"].values())
    tickets = list(bucket["tickets"].values())

    data: dict[str, Any] = {}
    counts: dict[str, int] = {}

    if threads:
        data["threads"] = threads
        data["profile"] = {"source_gateway": "pipeshub"}
        counts["threads"] = len(threads)
        counts["messages"] = sum(len(t.get("messages", [])) for t in threads)
    if bucket["documents"]:
        data["documents"] = bucket["documents"]
        data.setdefault("users", [])
        data.setdefault("drive_shares", [])
        counts["documents"] = len(bucket["documents"])
    # Tickets and issues both use the `issues` data key — provider name
    # disambiguates jira tickets from github/linear/gitlab issues.
    if tickets:
        data.setdefault("issues", []).extend(tickets)
        data.setdefault("projects", [])
        counts["issues"] = counts.get("issues", 0) + len(tickets)
    if bucket["issues"]:
        data.setdefault("issues", []).extend(bucket["issues"])
        data.setdefault("pull_requests", [])
        data.setdefault("merge_requests", [])
        counts["issues"] = counts.get("issues", 0) + len(bucket["issues"])
    if bucket["companies"] or bucket["contacts"] or bucket["deals"]:
        data["companies"] = bucket["companies"]
        data["contacts"] = bucket["contacts"]
        data["deals"] = bucket["deals"]
        counts["companies"] = len(bucket["companies"])
        counts["contacts"] = len(bucket["contacts"])
        counts["deals"] = len(bucket["deals"])
    if bucket["other"]:
        data["other"] = bucket["other"]
        counts["other"] = len(bucket["other"])

    return ContextSourceResult(
        provider=provider,
        captured_at=iso_now(),
        status="ok",
        record_counts=counts,
        data=data,
    )


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
    name = _normalize_token(
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
    unsupported_reason = _unsupported_ingestion_reason(name)
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
        supported_by_vei=False if unsupported_reason else True,
        support_note=unsupported_reason
        or "PipesHub records are ingested under their upstream provider; canonical timeline coverage depends on the provider.",
    )


def _normalize_connectors(connectors: list[str]) -> list[str]:
    """Lowercase + dedupe CLI connector inputs. Pass-through to PipesHub."""
    normalized: list[str] = []
    for connector in connectors:
        name = _normalize_token(connector)
        if name and name not in normalized:
            normalized.append(name)
    return normalized


def _pipeshub_connector_filter_values(
    connectors: list[str],
    configured_connectors: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Expand friendly CLI names into PipesHub record-filter values.

    PipesHub 0.4.0 filters records by ``record.connectorId`` even though the
    public option is named ``connectors``. Prefer configured connector instance
    ids when we can resolve them, and keep friendly/name aliases as a fallback
    for older or future builds that filter by connector name.
    """
    expanded: list[str] = []
    configured_connectors = configured_connectors or []
    for connector in connectors:
        for connector_id in _matching_connector_ids(connector, configured_connectors):
            if connector_id and connector_id not in expanded:
                expanded.append(connector_id)
        values = _PIPESHUB_CONNECTOR_FILTER_VALUES.get(connector)
        if not values:
            values = (connector, connector.replace("_", " ").upper())
        for value in values:
            if value and value not in expanded:
                expanded.append(value)
    return expanded


def _matching_connector_ids(
    connector: str, configured_connectors: list[dict[str, Any]]
) -> list[str]:
    aliases = {_normalize_token(connector)}
    aliases.update(
        _normalize_token(value)
        for value in _PIPESHUB_CONNECTOR_FILTER_VALUES.get(connector, ())
    )
    matches: list[str] = []
    for item in configured_connectors:
        connector_id = _text_field(item, "connectorId", "connector_id", "id", "_key")
        tokens = {
            _normalize_token(
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
                )
            ),
            _normalize_token(_text_field(item, "name", "displayName", "display_name")),
            _normalize_token(connector_id),
        }
        if aliases.intersection(token for token in tokens if token):
            matches.append(connector_id)
    return matches


def _normalize_token(value: Any) -> str:
    """Lowercase, strip whitespace, collapse non-alphanumerics to underscores."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")


def _vei_provider(connector_name: str) -> str:
    """Map a PipesHub connector name to the upstream system VEI calls it."""
    name = (connector_name or "").strip().lower()
    if not name:
        return "pipeshub"
    return _PIPESHUB_TO_VEI_PROVIDER.get(name, name)


def _unsupported_ingestion_reason(connector_name: str) -> str:
    return PIPESHUB_UNSUPPORTED_INGESTION.get(_normalize_token(connector_name), "")


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
    for key in (
        "total",
        "totalCount",
        "total_count",
        "totalRecords",
        "total_records",
        "totalItems",
        "total_items",
        "count",
    ):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_total(data)
    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        return _extract_total(pagination)
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
    return _normalize_token(_text_field(record, "recordType", "record_type", "type"))


def _connector_name(record: dict[str, Any]) -> str:
    return _normalize_token(
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
        if _is_connector_owner_permission(permission):
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
                "role": _text_field(
                    permission,
                    "permissionType",
                    "role",
                    "accessType",
                    "relationship",
                    "type",
                ),
                "created": _text_field(permission, "createdAt", "created_at"),
            }
        )
    return parsed


def _is_connector_owner_permission(permission: dict[str, Any]) -> bool:
    access_type = _text_field(permission, "accessType", "access_type").lower()
    relationship = _text_field(permission, "relationship").lower()
    if access_type == "connector_owner":
        return True
    return relationship == "owner"


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


def _record_in_source_window(
    record: dict[str, Any], date_from_ms: str, date_to_ms: str
) -> bool:
    if not date_from_ms and not date_to_ms:
        return True
    lower = int(date_from_ms) if date_from_ms else None
    upper = int(date_to_ms) if date_to_ms else None
    timestamps = _record_source_timestamps_ms(record)
    if not timestamps:
        return False
    for timestamp in timestamps:
        if lower is not None and timestamp < lower:
            continue
        if upper is not None and timestamp > upper:
            continue
        return True
    return False


def _record_source_timestamps_ms(record: dict[str, Any]) -> list[int]:
    values: list[int] = []
    for key in (
        "sourceLastModifiedTimestamp",
        "source_last_modified_timestamp",
        "sourceUpdatedAtTimestamp",
        "source_updated_at_timestamp",
        "sourceCreatedAtTimestamp",
        "source_created_at_timestamp",
        "receivedDateTime",
        "received_date_time",
        "lastModifiedDateTime",
        "last_modified_date_time",
        "createdDateTime",
        "created_date_time",
        "updatedAtTimestamp",
        "updated_at_timestamp",
        "createdAtTimestamp",
        "created_at_timestamp",
    ):
        timestamp = _coerce_timestamp_ms(_field(record, key))
        if timestamp is not None:
            values.append(timestamp)
    return values


def _coerce_timestamp_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        timestamp = int(value)
        return (
            timestamp * 1000 if timestamp and timestamp < 10_000_000_000 else timestamp
        )
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        timestamp = int(text)
        return (
            timestamp * 1000 if timestamp and timestamp < 10_000_000_000 else timestamp
        )
    parseable = text
    if parseable.endswith("Z"):
        parseable = parseable[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(parseable)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _source_capture_count(source: ContextSourceResult) -> int:
    counts = source.record_counts
    if "messages" in counts:
        return counts.get("messages") or counts.get("threads") or 0
    if "documents" in counts:
        return counts.get("documents") or 0
    if "issues" in counts:
        issue_like = counts.get("issues", 0)
        issue_like += counts.get("pull_requests", 0)
        issue_like += counts.get("merge_requests", 0)
        return issue_like
    return sum(count for count in counts.values() if count > 0)


def _http_error_message(exc: HTTPError) -> str:
    if exc.code == 401:
        return (
            "PipesHub returned 401 Unauthorized. Set PIPESHUB_BEARER_AUTH to a "
            "PipesHub bearer token, or pass --token-env with the environment variable "
            "that contains it."
        )
    detail = _http_error_detail(exc)
    if detail:
        return f"PipesHub returned HTTP {exc.code}: {exc.reason}: {detail}"
    return f"PipesHub returned HTTP {exc.code}: {exc.reason}"


def _http_error_detail(exc: HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:
        return ""
    if not body:
        return ""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body[:500]
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            code = error.get("code")
            if message and code:
                return f"{code}: {message}"
            if message:
                return str(message)
        message = parsed.get("message")
        if message:
            return str(message)
    return body[:500]


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
