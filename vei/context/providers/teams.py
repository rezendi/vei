from __future__ import annotations

import html
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

from vei.context.api import ContextSnapshot, ContextSourceResult
from vei.context.api import write_canonical_history_sidecars
from vei.context.models import ContextProviderConfig

from .base import api_get_json, iso_now, resolve_token

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
LOGIN_BASE = "https://login.microsoftonline.com"
DEFAULT_TEAMS_TOKEN_ENV = "VEI_TEAMS_TOKEN"
DEFAULT_TENANT_ENV = "VEI_MSFT_TENANT_ID"
DEFAULT_CLIENT_ID_ENV = "VEI_MSFT_CLIENT_ID"
DEFAULT_CLIENT_SECRET_ENV = "VEI_MSFT_CLIENT_SECRET"  # pragma: allowlist secret
DEFAULT_PAGE_SIZE = 250
DEFAULT_RUN_ID_PREFIX = "teams_graph"

logger = logging.getLogger(__name__)


class TeamsGraphInspectReport(BaseModel):
    graph_base_url: str
    tenant_id: str
    checked_at: str
    reachable: bool = True
    teams_sample: list[dict[str, str]] = Field(default_factory=list)
    users_sample: list[dict[str, str]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TeamsGraphCaptureManifest(BaseModel):
    version: str = "1"
    run_id: str
    tenant_id: str
    captured_at: str
    since: str = ""
    until: str = ""
    include_channels: bool = True
    include_chats: bool = True
    completed_scopes: list[str] = Field(default_factory=list)
    raw_record_count: int = 0
    duplicate_record_count: int = 0
    complete: bool = True
    warnings: list[str] = Field(default_factory=list)


class TeamsGraphCaptureReport(BaseModel):
    run_id: str
    tenant_id: str
    captured_at: str
    organization_name: str
    organization_domain: str = ""
    source_counts: dict[str, int] = Field(default_factory=dict)
    raw_record_count: int = 0
    duplicate_record_count: int = 0
    teams_scanned: int = 0
    users_scanned: int = 0
    complete: bool = True
    warnings: list[str] = Field(default_factory=list)
    capture_manifest_path: str = ""
    raw_records_path: str = ""
    snapshot_path: str = ""
    canonical_events_path: str = ""
    canonical_index_path: str = ""


@dataclass(frozen=True)
class TeamsGraphCaptureResult:
    snapshot: ContextSnapshot
    raw_records: list[dict[str, Any]]
    manifest: TeamsGraphCaptureManifest
    teams_scanned: int
    users_scanned: int


@dataclass(frozen=True)
class TeamsGraphCredentials:
    tenant_id: str
    client_id: str
    client_secret: str

    @classmethod
    def from_env(
        cls,
        *,
        tenant_env: str = DEFAULT_TENANT_ENV,
        client_id_env: str = DEFAULT_CLIENT_ID_ENV,
        client_secret_env: str = DEFAULT_CLIENT_SECRET_ENV,
    ) -> TeamsGraphCredentials:
        _load_dotenv_if_present()
        values = {
            "tenant_id": os.environ.get(tenant_env, "").strip(),
            "client_id": os.environ.get(client_id_env, "").strip(),
            "client_secret": os.environ.get(client_secret_env, "").strip(),
        }
        missing = [
            env
            for env, value in (
                (tenant_env, values["tenant_id"]),
                (client_id_env, values["client_id"]),
                (client_secret_env, values["client_secret"]),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "missing Microsoft Graph app credential env var(s): "
                + ", ".join(missing)
            )
        return cls(**values)


class TeamsGraphClient:
    """Small Microsoft Graph client for read-only Teams export capture."""

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        graph_base_url: str = GRAPH_BASE,
        login_base_url: str = LOGIN_BASE,
        timeout_s: int = 30,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.graph_base_url = graph_base_url.rstrip("/")
        self.login_base_url = login_base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._access_token: str = ""

    @classmethod
    def from_env(
        cls,
        *,
        tenant_env: str = DEFAULT_TENANT_ENV,
        client_id_env: str = DEFAULT_CLIENT_ID_ENV,
        client_secret_env: str = DEFAULT_CLIENT_SECRET_ENV,
        timeout_s: int = 30,
    ) -> TeamsGraphClient:
        credentials = TeamsGraphCredentials.from_env(
            tenant_env=tenant_env,
            client_id_env=client_id_env,
            client_secret_env=client_secret_env,
        )
        return cls(
            tenant_id=credentials.tenant_id,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            timeout_s=timeout_s,
        )

    def token(self) -> str:
        if self._access_token:
            return self._access_token
        token_url = (
            f"{self.login_base_url}/{quote(self.tenant_id, safe='')}"
            "/oauth2/v2.0/token"
        )
        body = urlencode(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            }
        ).encode("utf-8")
        request = Request(
            token_url,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        payload = _request_json(request, timeout_s=self.timeout_s)
        token = str(payload.get("access_token", "")).strip()
        if not token:
            raise ValueError(
                "Microsoft Graph token response did not include access_token"
            )
        self._access_token = token
        return token

    def get_json(
        self,
        path_or_url: str,
        *,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = self._url(path_or_url, params=params)
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token()}",
            },
            method="GET",
        )
        payload = _request_json(request, timeout_s=self.timeout_s)
        if not isinstance(payload, dict):
            raise ValueError(f"Microsoft Graph returned non-object payload for {url}")
        return payload

    def iter_collection(
        self,
        path_or_url: str,
        *,
        params: dict[str, str] | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        emitted = 0
        next_url: str | None = path_or_url
        next_params = params
        while next_url:
            payload = self.get_json(next_url, params=next_params)
            next_params = None
            value = payload.get("value", [])
            if not isinstance(value, list):
                value = []
            for item in value:
                if isinstance(item, dict):
                    yield item
                    emitted += 1
                    if limit is not None and emitted >= limit:
                        return
            raw_next = payload.get("@odata.nextLink") or payload.get("@odata.nextlink")
            next_url = str(raw_next).strip() if raw_next else None

    def _url(self, path_or_url: str, *, params: dict[str, str] | None = None) -> str:
        if urlparse(path_or_url).scheme:
            base = path_or_url
        else:
            base = urljoin(self.graph_base_url + "/", path_or_url.lstrip("/"))
        if not params:
            return base
        separator = "&" if "?" in base else "?"
        return f"{base}{separator}{urlencode(params)}"


class TeamsContextProvider:
    name = "teams"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        auth_mode = str(config.filters.get("auth", "")).strip().lower()
        if auth_mode == "client_credentials":
            client = TeamsGraphClient.from_env(timeout_s=config.timeout_s)
            result = capture_teams_graph_context(
                client,
                organization_name=str(config.filters.get("org", "")),
                organization_domain=str(config.filters.get("domain", "")),
                since=str(config.filters.get("since", "")),
                until=str(config.filters.get("until", "")),
                team_filters=_coerce_filter_list(config.filters.get("teams")),
                user_filters=_coerce_filter_list(config.filters.get("users")),
                include_channels=bool(config.filters.get("include_channels", True)),
                include_chats=bool(config.filters.get("include_chats", True)),
                limit=config.limit,
                page_size=min(config.limit, DEFAULT_PAGE_SIZE),
            )
            source = result.snapshot.source_for("teams")
            if source is None:
                return ContextSourceResult(
                    provider="teams",
                    captured_at=iso_now(),
                    status="empty",
                    record_counts={},
                    data={
                        "channels": [],
                        "profile": {"auth_mode": "client_credentials"},
                    },
                )
            return source
        return _capture_delegated_user_context(config)


def inspect_teams_graph(
    client: TeamsGraphClient | None = None,
    *,
    team_limit: int = 10,
    user_limit: int = 10,
) -> TeamsGraphInspectReport:
    client = client or TeamsGraphClient.from_env()
    warnings: list[str] = []
    teams_sample: list[dict[str, str]] = []
    users_sample: list[dict[str, str]] = []
    try:
        teams_sample = [
            _team_summary(team)
            for team in client.iter_collection(
                "/teams",
                params={"$top": str(max(1, min(team_limit, 100)))},
                limit=team_limit,
            )
        ]
    except Exception as exc:
        warnings.append(f"team discovery failed: {type(exc).__name__}: {exc}")
    try:
        users_sample = [
            _user_summary(user)
            for user in client.iter_collection(
                "/users",
                params={
                    "$top": str(max(1, min(user_limit, 999))),
                    "$select": "id,displayName,userPrincipalName,mail",
                },
                limit=user_limit,
            )
        ]
    except Exception as exc:
        warnings.append(f"user discovery failed: {type(exc).__name__}: {exc}")
    return TeamsGraphInspectReport(
        graph_base_url=client.graph_base_url,
        tenant_id=client.tenant_id,
        checked_at=iso_now(),
        reachable=not warnings,
        teams_sample=teams_sample,
        users_sample=users_sample,
        warnings=warnings,
    )


def capture_teams_graph_context(
    client: TeamsGraphClient | None = None,
    *,
    organization_name: str,
    organization_domain: str = "",
    since: str = "",
    until: str = "",
    team_filters: list[str] | None = None,
    user_filters: list[str] | None = None,
    include_channels: bool = True,
    include_chats: bool = True,
    limit: int = 1000,
    page_size: int = DEFAULT_PAGE_SIZE,
    team_limit: int = 250,
    user_limit: int = 250,
    run_id: str = "",
    manifest_path: Path | None = None,
    raw_records_path: Path | None = None,
    resume: bool = False,
) -> TeamsGraphCaptureResult:
    if not include_channels and not include_chats:
        raise ValueError(
            "at least one of include_channels/include_chats must be enabled"
        )
    client = client or TeamsGraphClient.from_env()
    resolved_run_id = run_id or new_teams_graph_run_id()
    since_dt = _graph_datetime(since, end=False)
    until_dt = _graph_datetime(until, end=True)
    manifest = _load_or_new_manifest(
        manifest_path,
        run_id=resolved_run_id,
        tenant_id=client.tenant_id,
        since=since_dt,
        until=until_dt,
        include_channels=include_channels,
        include_chats=include_chats,
        resume=resume,
    )
    raw_records, seen_keys = (
        _load_existing_records(raw_records_path) if resume else ([], set())
    )
    if not resume and raw_records_path is not None:
        raw_records_path.parent.mkdir(parents=True, exist_ok=True)
        raw_records_path.write_text("", encoding="utf-8")
    completed_scopes = set(manifest.completed_scopes)
    duplicates = manifest.duplicate_record_count
    warnings = list(manifest.warnings)
    page_size = max(1, min(page_size, DEFAULT_PAGE_SIZE))
    remaining = max(0, limit - len(raw_records))
    teams_scanned = 0
    users_scanned = 0

    def add_record(record: dict[str, Any]) -> bool:
        nonlocal duplicates, remaining
        key = _record_key(record)
        if key in seen_keys:
            duplicates += 1
            return False
        if remaining <= 0:
            return False
        seen_keys.add(key)
        raw_records.append(record)
        _append_jsonl(raw_records_path, record)
        remaining -= 1
        return True

    team_index: dict[str, dict[str, Any]] = {}
    channel_index: dict[tuple[str, str], dict[str, Any]] = {}
    if include_channels and remaining > 0:
        teams = _discover_teams(
            client,
            filters=team_filters or [],
            team_limit=team_limit,
        )
        for team in teams:
            teams_scanned += 1
            team_id = str(team.get("id", "")).strip()
            if not team_id:
                continue
            team_index[team_id] = team
            channels = _discover_channels(client, team_id=team_id)
            for channel in channels:
                channel_id = str(channel.get("id", "")).strip()
                if channel_id:
                    channel_index[(team_id, channel_id)] = channel
            scope = f"channel_team:{team_id}"
            if scope in completed_scopes:
                continue
            try:
                for message in client.iter_collection(
                    f"/teams/{quote(team_id, safe='')}/channels/getAllMessages",
                    params=_message_query_params(
                        since=since_dt,
                        until=until_dt,
                        page_size=page_size,
                    ),
                    limit=remaining,
                ):
                    record = _channel_message_record(
                        message,
                        fallback_team=team,
                        team_index=team_index,
                        channel_index=channel_index,
                    )
                    if record:
                        add_record(record)
                completed_scopes.add(scope)
                _write_manifest(
                    manifest_path,
                    manifest,
                    completed_scopes=completed_scopes,
                    raw_record_count=len(raw_records),
                    duplicates=duplicates,
                    complete=remaining > 0,
                    warnings=warnings,
                )
            except Exception as exc:
                warnings.append(
                    f"team {team.get('displayName') or team_id} message export failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                logger.warning(
                    "teams graph channel export failed for %s (%s)",
                    team_id,
                    type(exc).__name__,
                    extra={
                        "source": "context_capture",
                        "provider": "teams",
                        "file_path": team_id,
                        "exception_type": type(exc).__name__,
                    },
                    exc_info=True,
                )
            if remaining <= 0:
                break

    if include_chats and remaining > 0:
        users = _discover_users(
            client,
            filters=user_filters or [],
            user_limit=user_limit,
        )
        for user in users:
            users_scanned += 1
            user_id = str(user.get("id", "")).strip()
            user_ref = user_id or str(user.get("userPrincipalName", "")).strip()
            if not user_ref:
                continue
            scope = f"chat_user:{user_ref}"
            if scope in completed_scopes:
                continue
            try:
                for message in client.iter_collection(
                    f"/users/{quote(user_ref, safe='')}/chats/getAllMessages",
                    params=_message_query_params(
                        since=since_dt,
                        until=until_dt,
                        page_size=page_size,
                    ),
                    limit=remaining,
                ):
                    record = _chat_message_record(message, source_user=user)
                    if record:
                        add_record(record)
                completed_scopes.add(scope)
                _write_manifest(
                    manifest_path,
                    manifest,
                    completed_scopes=completed_scopes,
                    raw_record_count=len(raw_records),
                    duplicates=duplicates,
                    complete=remaining > 0,
                    warnings=warnings,
                )
            except Exception as exc:
                warnings.append(
                    f"user {user.get('userPrincipalName') or user_ref} chat export failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                logger.warning(
                    "teams graph chat export failed for %s (%s)",
                    user_ref,
                    type(exc).__name__,
                    extra={
                        "source": "context_capture",
                        "provider": "teams",
                        "file_path": user_ref,
                        "exception_type": type(exc).__name__,
                    },
                    exc_info=True,
                )
            if remaining <= 0:
                break

    complete = remaining > 0
    manifest = manifest.model_copy(
        update={
            "completed_scopes": sorted(completed_scopes),
            "raw_record_count": len(raw_records),
            "duplicate_record_count": duplicates,
            "complete": complete,
            "warnings": warnings,
        }
    )
    _write_manifest(
        manifest_path,
        manifest,
        completed_scopes=completed_scopes,
        raw_record_count=len(raw_records),
        duplicates=duplicates,
        complete=complete,
        warnings=warnings,
    )
    source = _records_to_source(raw_records)
    snapshot = ContextSnapshot(
        organization_name=organization_name,
        organization_domain=organization_domain,
        captured_at=iso_now(),
        sources=[source],
        metadata={
            "snapshot_role": "company_history_bundle",
            "source_gateway": "microsoft_graph",
            "source_system": "teams",
            "run_id": resolved_run_id,
            "since": since_dt,
            "until": until_dt,
        },
    )
    return TeamsGraphCaptureResult(
        snapshot=snapshot,
        raw_records=raw_records,
        manifest=manifest,
        teams_scanned=teams_scanned,
        users_scanned=users_scanned,
    )


def write_teams_graph_capture(
    capture: TeamsGraphCaptureResult,
    *,
    workspace: Path,
    output: Path | None = None,
) -> TeamsGraphCaptureReport:
    workspace = workspace.expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    run_id = capture.manifest.run_id
    sync_root = workspace / "imports" / "source_syncs" / "microsoft_teams" / run_id
    sync_root.mkdir(parents=True, exist_ok=True)
    raw_records_path = sync_root / "records.jsonl"
    manifest_path = sync_root / "capture_manifest.json"
    if not raw_records_path.exists():
        raw_records_path.write_text(
            "".join(
                json.dumps(record, sort_keys=True) + "\n"
                for record in capture.raw_records
            ),
            encoding="utf-8",
        )
    manifest_path.write_text(
        capture.manifest.model_dump_json(indent=2), encoding="utf-8"
    )
    snapshot_path = (
        output.expanduser().resolve() if output else workspace / "context_snapshot.json"
    )
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(
        capture.snapshot.model_dump_json(indent=2), encoding="utf-8"
    )
    paths = write_canonical_history_sidecars(capture.snapshot, snapshot_path)
    report = TeamsGraphCaptureReport(
        run_id=run_id,
        tenant_id=capture.manifest.tenant_id,
        captured_at=iso_now(),
        organization_name=capture.snapshot.organization_name,
        organization_domain=capture.snapshot.organization_domain,
        source_counts=(
            dict(capture.snapshot.sources[0].record_counts)
            if capture.snapshot.sources
            else {}
        ),
        raw_record_count=len(capture.raw_records),
        duplicate_record_count=capture.manifest.duplicate_record_count,
        teams_scanned=capture.teams_scanned,
        users_scanned=capture.users_scanned,
        complete=capture.manifest.complete,
        warnings=list(capture.manifest.warnings),
        capture_manifest_path=str(manifest_path),
        raw_records_path=str(raw_records_path),
        snapshot_path=str(snapshot_path),
        canonical_events_path=str(paths.events_path),
        canonical_index_path=str(paths.index_path),
    )
    report_path = sync_root / "capture_report.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    _update_source_registry(workspace, report, sync_root=sync_root)
    return report


def new_teams_graph_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{DEFAULT_RUN_ID_PREFIX}_{stamp}"


def _capture_delegated_user_context(
    config: ContextProviderConfig,
) -> ContextSourceResult:
    token = resolve_token(config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    timeout = config.timeout_s
    limit = min(config.limit, 50)

    teams = _fetch_joined_teams(headers, timeout)
    team_filter = config.filters.get("teams")

    captured_channels: list[dict[str, Any]] = []
    total_messages = 0

    for team in teams:
        team_name = team.get("displayName", "")
        if team_filter and team_name not in team_filter:
            continue
        team_id = team.get("id", "")
        channels = _fetch_channels(headers, timeout, team_id)
        for channel in channels:
            channel_id = channel.get("id", "")
            channel_name = channel.get("displayName", "")
            messages = _fetch_channel_messages(
                headers, timeout, team_id, channel_id, limit=limit
            )
            total_messages += len(messages)
            captured_channels.append(
                {
                    "channel": f"#{team_name}/{channel_name}",
                    "channel_id": channel_id,
                    "team_id": team_id,
                    "team_name": team_name,
                    "unread": 0,
                    "messages": messages,
                }
            )

    profile = _fetch_me(headers, timeout)

    return ContextSourceResult(
        provider="teams",
        captured_at=iso_now(),
        status="ok",
        record_counts={
            "teams": len(teams),
            "channels": len(captured_channels),
            "messages": total_messages,
        },
        data={
            "channels": captured_channels,
            "profile": profile,
        },
    )


def _fetch_joined_teams(
    headers: dict[str, str],
    timeout: int,
) -> list[dict[str, Any]]:
    url = f"{GRAPH_BASE}/me/joinedTeams"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    return result.get("value", []) if isinstance(result, dict) else []


def _fetch_channels(
    headers: dict[str, str],
    timeout: int,
    team_id: str,
) -> list[dict[str, Any]]:
    url = f"{GRAPH_BASE}/teams/{team_id}/channels"
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    return result.get("value", []) if isinstance(result, dict) else []


def _fetch_channel_messages(
    headers: dict[str, str],
    timeout: int,
    team_id: str,
    channel_id: str,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    url = (
        f"{GRAPH_BASE}/teams/{team_id}/channels/{channel_id}/messages" f"?$top={limit}"
    )
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
    except Exception as exc:
        logger.warning(
            "context teams channel message fetch failed for %s/%s (%s)",
            team_id,
            channel_id,
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "teams",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return []
    raw_messages = result.get("value", []) if isinstance(result, dict) else []
    return [
        _message_payload(
            m,
            fallback_thread=str(m.get("replyToId") or m.get("id") or ""),
        )
        for m in raw_messages
        if isinstance(m, dict) and m.get("messageType") == "message"
    ]


def _fetch_me(
    headers: dict[str, str],
    timeout: int,
) -> dict[str, Any]:
    url = f"{GRAPH_BASE}/me"
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
        return {
            "email": str(result.get("mail", result.get("userPrincipalName", ""))),
            "name": str(result.get("displayName", "")),
        }
    except Exception as exc:
        logger.warning(
            "context teams profile fetch failed (%s)",
            type(exc).__name__,
            extra={
                "source": "context_capture",
                "provider": "teams",
                "file_path": url,
                "exception_type": type(exc).__name__,
            },
            exc_info=True,
        )
        return {}


def _discover_teams(
    client: TeamsGraphClient,
    *,
    filters: list[str],
    team_limit: int,
) -> list[dict[str, Any]]:
    filters_lc = {item.lower() for item in filters if item}
    teams = list(
        client.iter_collection(
            "/teams",
            params={"$top": str(max(1, min(team_limit, 100)))},
            limit=team_limit,
        )
    )
    if not filters_lc:
        return teams
    return [
        team
        for team in teams
        if _matches_any(
            filters_lc,
            [
                team.get("id"),
                team.get("displayName"),
                team.get("description"),
            ],
        )
    ]


def _discover_channels(
    client: TeamsGraphClient,
    *,
    team_id: str,
) -> list[dict[str, Any]]:
    return list(
        client.iter_collection(
            f"/teams/{quote(team_id, safe='')}/channels",
            params={"$top": "100"},
        )
    )


def _discover_users(
    client: TeamsGraphClient,
    *,
    filters: list[str],
    user_limit: int,
) -> list[dict[str, Any]]:
    filters_lc = {item.lower() for item in filters if item}
    users = list(
        client.iter_collection(
            "/users",
            params={
                "$top": str(max(1, min(user_limit, 999))),
                "$select": "id,displayName,userPrincipalName,mail",
            },
            limit=user_limit,
        )
    )
    if not filters_lc:
        return users
    return [
        user
        for user in users
        if _matches_any(
            filters_lc,
            [
                user.get("id"),
                user.get("displayName"),
                user.get("userPrincipalName"),
                user.get("mail"),
            ],
        )
    ]


def _channel_message_record(
    message: dict[str, Any],
    *,
    fallback_team: dict[str, Any],
    team_index: dict[str, dict[str, Any]],
    channel_index: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any] | None:
    if str(message.get("messageType") or "message") != "message":
        return None
    identity = message.get("channelIdentity") or {}
    if not isinstance(identity, dict):
        identity = {}
    team_id = str(identity.get("teamId") or fallback_team.get("id") or "").strip()
    channel_id = str(identity.get("channelId") or "").strip()
    team = team_index.get(team_id, fallback_team)
    channel = channel_index.get((team_id, channel_id), {})
    record_id = str(message.get("id") or "").strip()
    if not record_id:
        return None
    return {
        "source": "microsoft_graph",
        "recordType": "CHANNEL_MESSAGE",
        "recordId": record_id,
        "teamId": team_id,
        "teamName": str(team.get("displayName") or team_id or "Unknown Team"),
        "channelId": channel_id,
        "channelName": str(
            channel.get("displayName") or channel_id or "Unknown Channel"
        ),
        "replyToId": message.get("replyToId"),
        "sourceCreatedAtTimestamp": message.get("createdDateTime"),
        "sourceLastModifiedTimestamp": message.get("lastModifiedDateTime"),
        "from": _sender_identity(message),
        "body": message.get("body") or {},
        "attachments": message.get("attachments") or [],
        "mentions": message.get("mentions") or [],
        "importance": message.get("importance"),
        "webUrl": message.get("webUrl"),
        "raw": message,
    }


def _chat_message_record(
    message: dict[str, Any],
    *,
    source_user: dict[str, Any],
) -> dict[str, Any] | None:
    if str(message.get("messageType") or "message") != "message":
        return None
    record_id = str(message.get("id") or "").strip()
    chat_id = str(message.get("chatId") or "").strip()
    if not record_id or not chat_id:
        return None
    return {
        "source": "microsoft_graph",
        "recordType": "CHAT_MESSAGE",
        "recordId": record_id,
        "chatId": chat_id,
        "sourceUserId": source_user.get("id"),
        "sourceUserPrincipalName": source_user.get("userPrincipalName"),
        "replyToId": message.get("replyToId"),
        "sourceCreatedAtTimestamp": message.get("createdDateTime"),
        "sourceLastModifiedTimestamp": message.get("lastModifiedDateTime"),
        "from": _sender_identity(message),
        "body": message.get("body") or {},
        "attachments": message.get("attachments") or [],
        "mentions": message.get("mentions") or [],
        "importance": message.get("importance"),
        "webUrl": message.get("webUrl"),
        "raw": message,
    }


def _records_to_source(records: list[dict[str, Any]]) -> ContextSourceResult:
    buckets: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(records):
        record_type = str(record.get("recordType") or "").upper()
        if record_type == "CHANNEL_MESSAGE":
            team_name = str(record.get("teamName") or record.get("teamId") or "Team")
            channel_name = str(
                record.get("channelName") or record.get("channelId") or "Channel"
            )
            bucket_key = f"channel:{record.get('teamId')}:{record.get('channelId')}"
            channel_label = f"#{team_name}/{channel_name}"
            fallback_thread = str(
                record.get("replyToId")
                or record.get("channelId")
                or record.get("recordId")
                or f"channel:{index}"
            )
        else:
            chat_id = str(record.get("chatId") or "chat").strip()
            bucket_key = f"chat:{chat_id}"
            channel_label = f"chat/{chat_id[:24] or index + 1}"
            fallback_thread = chat_id or str(record.get("recordId") or index)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "channel": channel_label,
                "channel_id": record.get("channelId") or record.get("chatId") or "",
                "team_id": record.get("teamId", ""),
                "team_name": record.get("teamName", ""),
                "source_kind": "chat" if record_type == "CHAT_MESSAGE" else "channel",
                "unread": 0,
                "messages": [],
            },
        )
        bucket["messages"].append(
            _message_payload(record, fallback_thread=fallback_thread)
        )

    channels = sorted(buckets.values(), key=lambda item: str(item.get("channel", "")))
    message_count = sum(len(channel.get("messages", [])) for channel in channels)
    return ContextSourceResult(
        provider="teams",
        captured_at=iso_now(),
        status="ok" if message_count else "empty",
        record_counts={
            "channels": sum(1 for c in channels if c.get("source_kind") == "channel"),
            "chats": sum(1 for c in channels if c.get("source_kind") == "chat"),
            "messages": message_count,
        },
        data={
            "channels": channels,
            "profile": {"source_gateway": "microsoft_graph"},
        },
    )


def _message_payload(
    source: dict[str, Any],
    *,
    fallback_thread: str,
) -> dict[str, Any]:
    created = str(
        source.get("sourceCreatedAtTimestamp")
        or source.get("createdDateTime")
        or source.get("ts")
        or ""
    )
    text = _extract_body(source)
    reply_to = source.get("replyToId") or source.get("thread_ts")
    return {
        "id": str(source.get("recordId") or source.get("id") or ""),
        "ts": created,
        "timestamp": created,
        "user": _extract_sender(source),
        "text": text,
        "body": text,
        "thread_ts": reply_to,
        "thread_id": str(reply_to or fallback_thread),
        "conversation_id": str(
            source.get("chatId") or source.get("channelId") or fallback_thread
        ),
        "web_url": source.get("webUrl"),
        "attachments": source.get("attachments") or [],
        "mentions": source.get("mentions") or [],
    }


def _sender_identity(message: dict[str, Any]) -> dict[str, str]:
    from_obj = message.get("from") or {}
    if not isinstance(from_obj, dict):
        return {}
    user = from_obj.get("user") or {}
    application = from_obj.get("application") or {}
    sender = user if isinstance(user, dict) and user else application
    if not isinstance(sender, dict):
        return {}
    return {
        "id": str(sender.get("id") or ""),
        "displayName": str(sender.get("displayName") or sender.get("id") or "unknown"),
        "userIdentityType": str(sender.get("userIdentityType") or ""),
    }


def _extract_sender(msg: dict[str, Any]) -> str:
    from_obj = msg.get("from") or {}
    if isinstance(from_obj, dict):
        if "displayName" in from_obj:
            return str(from_obj.get("displayName") or from_obj.get("id") or "unknown")
        user = from_obj.get("user") or {}
        if isinstance(user, dict):
            return str(user.get("displayName") or user.get("id") or "unknown")
    return "unknown"


def _extract_body(msg: dict[str, Any]) -> str:
    body = msg.get("body") or {}
    if isinstance(body, dict):
        content = str(body.get("content", ""))
        content_type = str(body.get("contentType", "")).lower()
    else:
        content = str(msg.get("content") or msg.get("text") or "")
        content_type = ""
    if content_type == "html" or "<" in content:
        content = re.sub(r"<br\s*/?>", "\n", content, flags=re.IGNORECASE)
        content = re.sub(r"</p\s*>", "\n", content, flags=re.IGNORECASE)
        content = re.sub(r"<[^>]+>", "", content)
    return html.unescape(content).strip()[:4000]


def _message_query_params(
    *,
    since: str,
    until: str,
    page_size: int,
) -> dict[str, str]:
    params = {"$top": str(max(1, min(page_size, DEFAULT_PAGE_SIZE)))}
    filters: list[str] = []
    if since:
        filters.append(f"lastModifiedDateTime gt {since}")
    if until:
        filters.append(f"lastModifiedDateTime lt {until}")
    if filters:
        params["$filter"] = " and ".join(filters)
    return params


def _graph_datetime(value: str, *, end: bool) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        parsed = date.fromisoformat(raw)
        if end:
            parsed = parsed + timedelta(days=1)
        return f"{parsed.isoformat()}T00:00:00Z"
    if raw.endswith("Z") or re.search(r"[+-]\d{2}:\d{2}$", raw):
        return raw
    return raw + "Z"


def _record_key(record: dict[str, Any]) -> str:
    record_type = str(record.get("recordType") or "").upper()
    if record_type == "CHAT_MESSAGE":
        return f"chat:{record.get('chatId')}:{record.get('recordId')}"
    return (
        f"channel:{record.get('teamId')}:{record.get('channelId')}:"
        f"{record.get('recordId')}"
    )


def _load_existing_records(
    raw_records_path: Path | None,
) -> tuple[list[dict[str, Any]], set[str]]:
    if raw_records_path is None or not raw_records_path.exists():
        return [], set()
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in raw_records_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            records.append(payload)
            seen.add(_record_key(payload))
    return records, seen


def _load_or_new_manifest(
    manifest_path: Path | None,
    *,
    run_id: str,
    tenant_id: str,
    since: str,
    until: str,
    include_channels: bool,
    include_chats: bool,
    resume: bool,
) -> TeamsGraphCaptureManifest:
    if resume and manifest_path is not None and manifest_path.exists():
        return TeamsGraphCaptureManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
    return TeamsGraphCaptureManifest(
        run_id=run_id,
        tenant_id=tenant_id,
        captured_at=iso_now(),
        since=since,
        until=until,
        include_channels=include_channels,
        include_chats=include_chats,
    )


def _write_manifest(
    manifest_path: Path | None,
    manifest: TeamsGraphCaptureManifest,
    *,
    completed_scopes: set[str],
    raw_record_count: int,
    duplicates: int,
    complete: bool,
    warnings: list[str],
) -> None:
    if manifest_path is None:
        return
    updated = manifest.model_copy(
        update={
            "completed_scopes": sorted(completed_scopes),
            "raw_record_count": raw_record_count,
            "duplicate_record_count": duplicates,
            "complete": complete,
            "warnings": warnings,
        }
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(updated.model_dump_json(indent=2), encoding="utf-8")


def _append_jsonl(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _request_json(request: Request, *, timeout_s: int) -> Any:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=timeout_s) as response:  # nosec B310
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_exc = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"Microsoft Graph HTTP {exc.code}: {detail[:500]}"
                ) from exc
            retry_after = exc.headers.get("Retry-After")
            _sleep_before_retry(retry_after, attempt=attempt)
        except URLError as exc:
            last_exc = exc
            if attempt == 2:
                raise RuntimeError(f"Microsoft Graph request failed: {exc}") from exc
            _sleep_before_retry(None, attempt=attempt)
    raise RuntimeError(f"Microsoft Graph request failed: {last_exc}")


def _sleep_before_retry(retry_after: str | None, *, attempt: int) -> None:
    if retry_after and retry_after.isdigit():
        delay = min(int(retry_after), 10)
    else:
        delay = min(2**attempt, 5)
    time.sleep(delay)


def _load_dotenv_if_present() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv(override=False)


def _has_graph_app_env() -> bool:
    _load_dotenv_if_present()
    return all(
        os.environ.get(name, "").strip()
        for name in (
            DEFAULT_TENANT_ENV,
            DEFAULT_CLIENT_ID_ENV,
            DEFAULT_CLIENT_SECRET_ENV,
        )
    )


def _coerce_filter_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _matches_any(filters_lc: set[str], values: Iterable[Any]) -> bool:
    candidates = [str(value or "").strip().lower() for value in values]
    return any(candidate in filters_lc for candidate in candidates if candidate)


def _team_summary(team: dict[str, Any]) -> dict[str, str]:
    return {
        "id": str(team.get("id") or ""),
        "display_name": str(team.get("displayName") or ""),
        "description": str(team.get("description") or ""),
    }


def _user_summary(user: dict[str, Any]) -> dict[str, str]:
    return {
        "id": str(user.get("id") or ""),
        "display_name": str(user.get("displayName") or ""),
        "user_principal_name": str(user.get("userPrincipalName") or ""),
        "mail": str(user.get("mail") or ""),
    }


def _update_source_registry(
    workspace: Path,
    report: TeamsGraphCaptureReport,
    *,
    sync_root: Path,
) -> None:
    imports_dir = workspace / "imports"
    imports_dir.mkdir(parents=True, exist_ok=True)
    registry_path = imports_dir / "source_registry.json"
    history_path = imports_dir / "source_sync_history.json"
    registry = _read_json_list(registry_path)
    now = iso_now()
    entry = {
        "source_id": "microsoft_teams_graph",
        "connector": "microsoft_teams",
        "display_name": "Microsoft Teams Graph capture",
        "last_synced_at": now,
        "status": "ok" if report.complete else "partial",
        "record_counts": dict(report.source_counts),
        "metadata": {
            "run_id": report.run_id,
            "sync_root": str(sync_root.relative_to(workspace)),
        },
    }
    registry = [
        item
        for item in registry
        if not (isinstance(item, dict) and item.get("source_id") == entry["source_id"])
    ]
    registry.append(entry)
    registry_path.write_text(
        json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8"
    )
    history = _read_json_list(history_path)
    history.append(
        {
            "source_id": "microsoft_teams_graph",
            "connector": "microsoft_teams",
            "synced_at": now,
            "status": "ok" if report.complete else "partial",
            "record_counts": dict(report.source_counts),
            "metadata": {
                "run_id": report.run_id,
                "sync_root": str(sync_root.relative_to(workspace)),
            },
        }
    )
    history_path.write_text(
        json.dumps(history, indent=2, sort_keys=True), encoding="utf-8"
    )


def _read_json_list(path: Path) -> list[Any]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []
