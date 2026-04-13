from __future__ import annotations


def intervention_tags(prompt: str) -> set[str]:
    lowered = prompt.strip().lower()
    tags: set[str] = set()
    if any(token in lowered for token in ("legal", "compliance")):
        tags.update({"legal", "compliance"})
    if any(token in lowered for token in ("hold", "pause", "stop forward", "freeze")):
        tags.update({"hold", "pause_forward"})
    if any(
        token in lowered
        for token in (
            "status note",
            "short update",
            "clean update soon",
            "no-attachment",
            "no attachment",
            "without attachment",
        )
    ):
        tags.update({"status_only", "attachment_removed"})
    if any(
        token in lowered
        for token in (
            "reply immediately",
            "respond immediately",
            "same day",
            "right away",
        )
    ):
        tags.add("reply_immediately")
    if any(token in lowered for token in ("owner", "ownership", "clarify owner")):
        tags.add("clarify_owner")
    if any(
        token in lowered
        for token in ("executive gate", "route through", "sign-off", "approval")
    ):
        tags.add("executive_gate")
    if any(
        token in lowered
        for token in (
            "remove attachment",
            "remove the attachment",
            "strip attachment",
            "strip the attachment",
            "keep the attachment inside",
            "keep the original attachment internal",
        )
    ):
        tags.add("attachment_removed")
    if any(
        token in lowered
        for token in (
            "remove external",
            "remove outside recipient",
            "remove the outside recipient",
            "pull the outside recipient",
            "internal only",
            "keep this internal",
            "keep it internal",
            "keep the issue internal",
            "hold the outside send",
        )
    ):
        tags.add("external_removed")
    if any(
        token in lowered
        for token in (
            "send now",
            "send immediately",
            "outside loop active",
            "widen circulation",
            "broader loop",
            "rapid comments",
            "parallel follow-up",
            "fast turnaround",
        )
    ):
        tags.update({"send_now", "widen_loop"})
    return tags
