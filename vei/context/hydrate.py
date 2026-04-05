from __future__ import annotations

from typing import Any, Optional

from vei.blueprint.models import (
    BlueprintAsset,
    BlueprintCapabilityGraphsAsset,
    BlueprintCommGraphAsset,
    BlueprintDocGraphAsset,
    BlueprintDocumentAsset,
    BlueprintIdentityApplicationAsset,
    BlueprintIdentityGraphAsset,
    BlueprintIdentityGroupAsset,
    BlueprintIdentityUserAsset,
    BlueprintMailMessageAsset,
    BlueprintMailThreadAsset,
    BlueprintSlackChannelAsset,
    BlueprintSlackMessageAsset,
    BlueprintTicketAsset,
    BlueprintWorkGraphAsset,
)

from .models import ContextSnapshot, ContextSourceResult


def hydrate_snapshot_to_blueprint(
    snapshot: ContextSnapshot,
    *,
    scenario_name: str = "captured_context",
    workflow_name: str = "captured_context",
) -> BlueprintAsset:
    slack_source = snapshot.source_for("slack")
    jira_source = snapshot.source_for("jira")
    google_source = snapshot.source_for("google")
    okta_source = snapshot.source_for("okta")
    gmail_source = snapshot.source_for("gmail")
    mail_archive_source = snapshot.source_for("mail_archive")
    teams_source = snapshot.source_for("teams")

    comm_graph = _build_comm_graph(slack_source, teams_source)
    mail_threads = _build_mail_threads(gmail_source, mail_archive_source)
    if comm_graph and mail_threads:
        comm_graph.mail_threads = mail_threads
    elif mail_threads:
        comm_graph = BlueprintCommGraphAsset(mail_threads=mail_threads)

    doc_graph = _build_doc_graph(google_source)
    work_graph = _build_work_graph(jira_source)
    identity_graph = _build_identity_graph(
        okta_source,
        google_source,
        mail_archive_source,
    )

    facades = _infer_facades(
        slack_source,
        jira_source,
        google_source,
        okta_source,
        gmail_source,
        mail_archive_source,
        teams_source,
    )

    return BlueprintAsset(
        name=f"{snapshot.organization_name.lower().replace(' ', '_')}.blueprint",
        title=snapshot.organization_name,
        description=f"Context capture for {snapshot.organization_name}",
        scenario_name=scenario_name,
        family_name="captured_context",
        workflow_name=workflow_name,
        workflow_variant="captured_context",
        requested_facades=facades,
        capability_graphs=BlueprintCapabilityGraphsAsset(
            organization_name=snapshot.organization_name,
            organization_domain=snapshot.organization_domain,
            scenario_brief=f"Captured operational state of {snapshot.organization_name}",
            comm_graph=comm_graph,
            doc_graph=doc_graph,
            work_graph=work_graph,
            identity_graph=identity_graph,
            metadata={
                "source": "context_capture",
                "captured_at": snapshot.captured_at,
                "providers": [s.provider for s in snapshot.sources],
            },
        ),
        metadata={
            "source": "context_capture",
            "captured_at": snapshot.captured_at,
        },
    )


def _build_comm_graph(
    slack_source: Optional[ContextSourceResult],
    teams_source: Optional[ContextSourceResult] = None,
) -> Optional[BlueprintCommGraphAsset]:
    channels: list[BlueprintSlackChannelAsset] = []

    if slack_source and slack_source.status != "error":
        for ch in slack_source.data.get("channels", []):
            if not isinstance(ch, dict):
                continue
            messages = [
                BlueprintSlackMessageAsset(
                    ts=str(m.get("ts", "")),
                    user=str(m.get("user", "unknown")),
                    text=str(m.get("text", "")),
                    thread_ts=m.get("thread_ts"),
                )
                for m in ch.get("messages", [])
                if isinstance(m, dict)
            ]
            channels.append(
                BlueprintSlackChannelAsset(
                    channel=str(ch.get("channel", "")),
                    messages=messages,
                    unread=int(ch.get("unread", 0) or 0),
                )
            )

    if teams_source and teams_source.status != "error":
        for ch in teams_source.data.get("channels", []):
            if not isinstance(ch, dict):
                continue
            messages = [
                BlueprintSlackMessageAsset(
                    ts=str(m.get("ts", "")),
                    user=str(m.get("user", "unknown")),
                    text=str(m.get("text", "")),
                    thread_ts=m.get("thread_ts"),
                )
                for m in ch.get("messages", [])
                if isinstance(m, dict)
            ]
            channels.append(
                BlueprintSlackChannelAsset(
                    channel=str(ch.get("channel", "")),
                    messages=messages,
                    unread=int(ch.get("unread", 0) or 0),
                )
            )

    if not channels:
        return None

    return BlueprintCommGraphAsset(
        slack_initial_message="Context captured from live workspace.",
        slack_channels=channels,
    )


def _build_mail_from_gmail(
    gmail_source: Optional[ContextSourceResult],
) -> list[BlueprintMailThreadAsset]:
    if not gmail_source or gmail_source.status == "error":
        return []

    threads_data = gmail_source.data.get("threads", [])
    result: list[BlueprintMailThreadAsset] = []

    for thread in threads_data:
        if not isinstance(thread, dict):
            continue
        messages = thread.get("messages", [])
        mail_messages = [
            BlueprintMailMessageAsset(
                from_address=str(m.get("from", "")),
                to_address=str(m.get("to", "")),
                subject=str(m.get("subject", "")),
                body_text=str(m.get("snippet", "")),
                unread=bool(m.get("unread", False)),
            )
            for m in messages
            if isinstance(m, dict)
        ]
        if not mail_messages:
            continue
        labels = []
        if messages and isinstance(messages[0], dict):
            labels = messages[0].get("labels", [])
        category = "internal"
        if any(lb in labels for lb in ["CATEGORY_PROMOTIONS", "CATEGORY_SOCIAL"]):
            category = "external"
        elif any(lb in labels for lb in ["IMPORTANT", "STARRED"]):
            category = "important"

        result.append(
            BlueprintMailThreadAsset(
                thread_id=str(thread.get("thread_id", "")),
                title=str(thread.get("subject", "")),
                category=category,
                messages=mail_messages,
            )
        )

    return result


def _build_mail_from_archive(
    mail_archive_source: Optional[ContextSourceResult],
) -> list[BlueprintMailThreadAsset]:
    if not mail_archive_source or mail_archive_source.status == "error":
        return []

    result: list[BlueprintMailThreadAsset] = []
    for thread in mail_archive_source.data.get("threads", []):
        if not isinstance(thread, dict):
            continue
        mail_messages: list[BlueprintMailMessageAsset] = []
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            raw_time_ms = message.get("time_ms")
            mail_messages.append(
                BlueprintMailMessageAsset(
                    from_address=str(message.get("from", "")),
                    to_address=str(message.get("to", "")),
                    subject=str(message.get("subj", message.get("subject", ""))),
                    body_text=str(
                        message.get(
                            "body_text",
                            message.get("snippet", message.get("content", "")),
                        )
                    ),
                    unread=bool(message.get("unread", False)),
                    time_ms=(
                        int(raw_time_ms)
                        if isinstance(raw_time_ms, (int, float, str))
                        else None
                    ),
                )
            )
        if not mail_messages:
            continue
        result.append(
            BlueprintMailThreadAsset(
                thread_id=str(thread.get("thread_id", "")),
                title=str(thread.get("title", thread.get("subject", ""))),
                category=str(thread.get("category", "archive")),
                messages=mail_messages,
            )
        )
    return result


def _build_mail_threads(
    gmail_source: Optional[ContextSourceResult],
    mail_archive_source: Optional[ContextSourceResult],
) -> list[BlueprintMailThreadAsset]:
    archive_threads = _build_mail_from_archive(mail_archive_source)
    if archive_threads:
        return archive_threads
    return _build_mail_from_gmail(gmail_source)


def _build_doc_graph(
    google_source: Optional[ContextSourceResult],
) -> Optional[BlueprintDocGraphAsset]:
    if not google_source or google_source.status == "error":
        return None

    docs_data = google_source.data.get("documents", [])
    if not docs_data:
        return None

    documents = [
        BlueprintDocumentAsset(
            doc_id=str(d.get("doc_id", f"doc-{i}")),
            title=str(d.get("title", "")),
            body=str(d.get("body", "")),
            tags=[t for t in [d.get("mime_type", "")] if t],
        )
        for i, d in enumerate(docs_data)
        if isinstance(d, dict)
    ]

    return BlueprintDocGraphAsset(documents=documents)


def _build_work_graph(
    jira_source: Optional[ContextSourceResult],
) -> Optional[BlueprintWorkGraphAsset]:
    if not jira_source or jira_source.status == "error":
        return None

    issues_data = jira_source.data.get("issues", [])
    if not issues_data:
        return None

    tickets = [
        BlueprintTicketAsset(
            ticket_id=str(issue.get("ticket_id", f"ISSUE-{i}")),
            title=str(issue.get("title", "")),
            status=str(issue.get("status", "open")),
            assignee=str(issue.get("assignee", "unassigned")),
            description=str(issue.get("description", "")),
        )
        for i, issue in enumerate(issues_data)
        if isinstance(issue, dict)
    ]

    return BlueprintWorkGraphAsset(tickets=tickets)


def _build_identity_graph(
    okta_source: Optional[ContextSourceResult],
    google_source: Optional[ContextSourceResult],
    mail_archive_source: Optional[ContextSourceResult] = None,
) -> Optional[BlueprintIdentityGraphAsset]:
    users: list[BlueprintIdentityUserAsset] = []
    groups: list[BlueprintIdentityGroupAsset] = []
    apps: list[BlueprintIdentityApplicationAsset] = []

    if okta_source and okta_source.status != "error":
        for u in okta_source.data.get("users", []):
            if not isinstance(u, dict):
                continue
            profile = u.get("profile") or {}
            users.append(
                BlueprintIdentityUserAsset(
                    user_id=str(u.get("id", "")),
                    email=str(profile.get("login", profile.get("email", ""))),
                    first_name=str(profile.get("firstName", "")),
                    last_name=str(profile.get("lastName", "")),
                    display_name=str(
                        profile.get("displayName", profile.get("firstName", ""))
                    ),
                    department=str(profile.get("department", "")),
                    title=str(profile.get("title", "")),
                    status=str(u.get("status", "active")),
                    groups=u.get("group_ids", []),
                )
            )
        for g in okta_source.data.get("groups", []):
            if not isinstance(g, dict):
                continue
            profile = g.get("profile") or {}
            groups.append(
                BlueprintIdentityGroupAsset(
                    group_id=str(g.get("id", "")),
                    name=str(profile.get("name", g.get("id", ""))),
                    members=g.get("members", []),
                )
            )
        for a in okta_source.data.get("applications", []):
            if not isinstance(a, dict):
                continue
            apps.append(
                BlueprintIdentityApplicationAsset(
                    app_id=str(a.get("id", "")),
                    label=str(a.get("label", a.get("name", ""))),
                    status=str(a.get("status", "active")),
                    assignments=a.get("assignments", []),
                )
            )

    if google_source and google_source.status != "error":
        for u in google_source.data.get("users", []):
            if not isinstance(u, dict):
                continue
            if any(existing.email == u.get("email") for existing in users):
                continue
            full_name = str(u.get("name", ""))
            name_parts = full_name.split(" ", 1)
            users.append(
                BlueprintIdentityUserAsset(
                    user_id=str(u.get("id", "")),
                    email=str(u.get("email", "")),
                    first_name=name_parts[0],
                    last_name=name_parts[1] if len(name_parts) > 1 else "",
                    display_name=full_name,
                    department=str(u.get("org_unit", "")),
                    status="suspended" if u.get("suspended") else "active",
                )
            )

    if mail_archive_source and mail_archive_source.status != "error":
        _append_mail_archive_users(users, mail_archive_source.data)

    if not users and not groups and not apps:
        return None

    return BlueprintIdentityGraphAsset(
        users=users,
        groups=groups,
        applications=apps,
    )


def _infer_facades(
    slack_source: Optional[ContextSourceResult],
    jira_source: Optional[ContextSourceResult],
    google_source: Optional[ContextSourceResult],
    okta_source: Optional[ContextSourceResult],
    gmail_source: Optional[ContextSourceResult] = None,
    mail_archive_source: Optional[ContextSourceResult] = None,
    teams_source: Optional[ContextSourceResult] = None,
) -> list[str]:
    facades: list[str] = []
    if slack_source and slack_source.status != "error":
        facades.append("slack")
    if teams_source and teams_source.status != "error":
        if "slack" not in facades:
            facades.append("slack")
    if jira_source and jira_source.status != "error":
        facades.extend(["jira", "servicedesk"])
    if google_source and google_source.status != "error":
        facades.append("docs")
    if (gmail_source and gmail_source.status != "error") or (
        mail_archive_source and mail_archive_source.status != "error"
    ):
        if "mail" not in facades:
            facades.append("mail")
    if (
        mail_archive_source
        and mail_archive_source.status != "error"
        and "identity" not in facades
    ):
        facades.append("identity")
    if okta_source and okta_source.status != "error":
        facades.append("identity")
    return facades


def _append_mail_archive_users(
    users: list[BlueprintIdentityUserAsset],
    payload: dict[str, Any],
) -> None:
    seen = {item.email.lower() for item in users if item.email}
    actors = payload.get("actors", [])
    if isinstance(actors, list):
        for actor in actors:
            if not isinstance(actor, dict):
                continue
            email = str(actor.get("email", "")).strip()
            if not email or email.lower() in seen:
                continue
            first_name, last_name = _split_actor_name(
                str(actor.get("display_name", "")) or email
            )
            users.append(
                BlueprintIdentityUserAsset(
                    user_id=str(actor.get("actor_id", email)),
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    login=email,
                    display_name=(
                        str(actor.get("display_name", "")).strip()
                        or f"{first_name} {last_name}".strip()
                    ),
                    department=str(actor.get("department", "")) or None,
                    title=str(actor.get("title", "")) or None,
                    status="ACTIVE",
                )
            )
            seen.add(email.lower())

    if actors:
        return

    derived: dict[str, BlueprintIdentityUserAsset] = {}
    for thread in payload.get("threads", []):
        if not isinstance(thread, dict):
            continue
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            for key in ("from", "to"):
                email = str(message.get(key, "")).strip()
                lowered = email.lower()
                if not email or lowered in seen or lowered in derived:
                    continue
                first_name, last_name = _split_actor_name(email)
                derived[lowered] = BlueprintIdentityUserAsset(
                    user_id=email,
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    login=email,
                    display_name=f"{first_name} {last_name}".strip(),
                    status="ACTIVE",
                )
    users.extend(derived.values())


def _split_actor_name(value: str) -> tuple[str, str]:
    cleaned = value.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
    parts = [item for item in cleaned.split() if item]
    if not parts:
        return ("Unknown", "Actor")
    if len(parts) == 1:
        token = parts[0].capitalize()
        return (token, "Actor")
    return (parts[0].capitalize(), parts[-1].capitalize())
