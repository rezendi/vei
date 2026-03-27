from __future__ import annotations

from typing import Any, Dict, List

from vei.context.models import ContextProviderConfig, ContextSourceResult

from .base import api_get_json, iso_now, join_url, resolve_token


class JiraContextProvider:
    name = "jira"

    def capture(self, config: ContextProviderConfig) -> ContextSourceResult:
        token = resolve_token(config)
        base_url = config.base_url
        if not base_url:
            raise ValueError("jira provider requires base_url")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        timeout = config.timeout_s
        limit = min(config.limit, 100)

        project_key = config.filters.get("project")
        jql = config.filters.get("jql", "")
        if not jql:
            jql = (
                f"project = {project_key} ORDER BY updated DESC"
                if project_key
                else "ORDER BY updated DESC"
            )

        issues = _fetch_issues(base_url, headers, timeout, jql=jql, limit=limit)
        projects = _fetch_projects(base_url, headers, timeout)

        for issue in issues[:10]:
            issue["transitions"] = _fetch_transitions(
                base_url, headers, timeout, issue["ticket_id"]
            )

        return ContextSourceResult(
            provider="jira",
            captured_at=iso_now(),
            status="ok",
            record_counts={
                "issues": len(issues),
                "projects": len(projects),
            },
            data={
                "issues": issues,
                "projects": projects,
            },
        )


def _fetch_issues(
    base_url: str,
    headers: Dict[str, str],
    timeout: int,
    *,
    jql: str,
    limit: int,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    start_at = 0
    while len(issues) < limit:
        page_size = min(50, limit - len(issues))
        url = join_url(
            base_url,
            f"/rest/api/3/search?jql={jql}&maxResults={page_size}&startAt={start_at}"
            "&fields=summary,status,assignee,description,issuetype,priority,updated,comment",
        )
        result = api_get_json(url, headers=headers, timeout_s=timeout)
        raw_issues = result.get("issues", []) if isinstance(result, dict) else []
        if not raw_issues:
            break
        for issue in raw_issues:
            if not isinstance(issue, dict):
                continue
            fields = issue.get("fields") or {}
            comments_data = fields.get("comment", {})
            comments = _extract_comments(comments_data)
            issues.append(
                {
                    "ticket_id": str(issue.get("key", "")),
                    "title": str(fields.get("summary", "")),
                    "status": _nested_name(fields.get("status")),
                    "assignee": _nested_name(fields.get("assignee"), key="displayName"),
                    "description": _adf_to_text(fields.get("description")),
                    "issue_type": _nested_name(fields.get("issuetype")),
                    "priority": _nested_name(fields.get("priority")),
                    "updated": str(fields.get("updated", "")),
                    "comments": comments,
                }
            )
        total = result.get("total", 0) if isinstance(result, dict) else 0
        start_at += len(raw_issues)
        if start_at >= total:
            break
    return issues[:limit]


def _fetch_transitions(
    base_url: str,
    headers: Dict[str, str],
    timeout: int,
    issue_key: str,
) -> List[Dict[str, Any]]:
    url = join_url(base_url, f"/rest/api/3/issue/{issue_key}/transitions")
    try:
        result = api_get_json(url, headers=headers, timeout_s=timeout)
    except Exception:
        return []
    raw = result.get("transitions", []) if isinstance(result, dict) else []
    return [
        {"id": str(t.get("id", "")), "name": str(t.get("name", ""))}
        for t in raw
        if isinstance(t, dict)
    ]


def _extract_comments(comment_data: Any) -> List[Dict[str, Any]]:
    if not isinstance(comment_data, dict):
        return []
    raw = comment_data.get("comments", [])
    return [
        {
            "id": str(c.get("id", "")),
            "author": _nested_name(c.get("author"), key="displayName"),
            "body": _adf_to_text(c.get("body")),
            "created": str(c.get("created", "")),
        }
        for c in raw
        if isinstance(c, dict)
    ]


def _fetch_projects(
    base_url: str,
    headers: Dict[str, str],
    timeout: int,
) -> List[Dict[str, Any]]:
    url = join_url(base_url, "/rest/api/3/project")
    result = api_get_json(url, headers=headers, timeout_s=timeout)
    if not isinstance(result, list):
        return []
    return [
        {
            "key": str(p.get("key", "")),
            "name": str(p.get("name", "")),
            "style": str(p.get("style", "")),
        }
        for p in result
        if isinstance(p, dict)
    ]


def _nested_name(value: Any, *, key: str = "name") -> str:
    if isinstance(value, dict):
        return str(value.get(key, ""))
    return ""


def _adf_to_text(value: Any) -> str:
    """Extract plain text from Atlassian Document Format (ADF) or return as-is."""
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return ""
    parts: list[str] = []
    _walk_adf(value, parts)
    return " ".join(parts).strip()


def _walk_adf(node: Any, parts: list[str]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "text":
            text = node.get("text")
            if isinstance(text, str):
                parts.append(text)
        for child in node.get("content", []):
            _walk_adf(child, parts)
    elif isinstance(node, list):
        for child in node:
            _walk_adf(child, parts)
