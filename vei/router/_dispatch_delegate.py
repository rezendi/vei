from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from vei.connectors import ConnectorInvocationError
from vei.blueprint import resolve_tool_operation_class
from vei.connectors.api import TOOL_ROUTES
from vei.events.api import (
    ActorRef,
    ExecutionPrincipal,
    ToolPolicyMetadata,
    build_tool_call_event,
    classify_tool_call_failure,
    extract_object_refs,
    stable_event_id,
)

from ._dispatch import GUARDED_PREFIXES, build_dispatch_table
from .errors import MCPError

if TYPE_CHECKING:
    from .core import Router


def _resolve_execution_principal(
    principal: ExecutionPrincipal | dict[str, Any] | None,
    *,
    request_metadata: dict[str, Any] | None = None,
) -> ExecutionPrincipal:
    if isinstance(principal, ExecutionPrincipal):
        return principal
    if isinstance(principal, dict):
        return ExecutionPrincipal.from_mapping(
            {**(request_metadata or {}), **principal},
            source=str(principal.get("source") or "mcp"),
        )
    if request_metadata:
        return ExecutionPrincipal.from_mapping(request_metadata, source="mcp")
    return ExecutionPrincipal.from_env(source="mcp")


def _principal_from_payload(payload: Dict[str, Any]) -> dict[str, Any] | None:
    for key in ("principal", "execution_principal", "identity", "actor"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return None


def _request_metadata_from_payload(payload: Dict[str, Any]) -> dict[str, Any] | None:
    metadata: dict[str, Any] = {}
    for key in ("request_metadata", "metadata", "headers"):
        value = payload.get(key)
        if isinstance(value, dict):
            if key == "headers":
                metadata.setdefault("headers", value)
            else:
                metadata.update(value)
    return metadata or None


def _principal_actor_ref(principal: ExecutionPrincipal) -> ActorRef | None:
    actor_id = principal.agent_id or principal.human_user_id or principal.auth_subject
    if not actor_id:
        return None
    return ActorRef(
        actor_id=actor_id,
        display_name=principal.agent_id or principal.human_user_id or actor_id,
        role=principal.source,
        tenant_id=principal.tenant_id,
    )


def _tool_policy_metadata(
    *,
    router: Router,
    tool: str,
    args: Dict[str, Any],
    principal: ExecutionPrincipal,
    request_metadata: dict[str, Any] | None = None,
) -> ToolPolicyMetadata:
    operation_class = _resolve_operation_class(router, tool)
    metadata = request_metadata or {}
    supplied_metadata = metadata.get("policy_metadata", {})
    if not isinstance(supplied_metadata, dict):
        supplied_metadata = {}
    return ToolPolicyMetadata(
        operation_class=operation_class,
        access_mode=_access_mode(tool=tool, operation_class=operation_class),
        destination_class=_destination_class(tool=tool, args=args),
        sensitivity_tags=_sensitivity_tags(args, supplied_metadata),
        object_classification=_object_classification(args, supplied_metadata),
        externality=str(
            supplied_metadata.get("externality") or _externality(tool=tool, args=args)
        ),
        destructive_write=_destructive_write(
            tool=tool, operation_class=operation_class
        ),
        policy_profile_id=str(
            principal.extra.get("policy_profile_id") or principal.policy_profile_id
        ),
        approval_required=_approval_required(
            tool=tool, operation_class=operation_class
        ),
        tenant_boundary=str(
            supplied_metadata.get("tenant_boundary")
            or _tenant_boundary(args=args, principal=principal)
        ),
    )


def _resolve_operation_class(router: Router, tool: str) -> str:
    normalized = router.alias_map.get(tool, tool)
    route = TOOL_ROUTES.get(normalized)
    if route is not None:
        return route.operation_class.value
    resolved = resolve_tool_operation_class(normalized)
    if resolved:
        return str(resolved)
    lower = normalized.lower()
    if any(
        token in lower
        for token in (
            ".list",
            ".read",
            ".open",
            ".get",
            ".search",
            ".describe",
            ".state",
            ".observe",
        )
    ):
        return "read"
    if any(
        token in lower
        for token in (
            ".delete",
            ".remove",
            ".suspend",
            ".reset",
            ".cancel",
            ".deactivate",
        )
    ):
        return "write_risky"
    if any(
        token in lower
        for token in (
            ".create",
            ".update",
            ".send",
            ".reply",
            ".post",
            ".assign",
            ".upsert",
            ".write",
        )
    ):
        return "write_safe"
    return "unknown"


def _access_mode(*, tool: str, operation_class: str) -> str:
    lower = tool.lower()
    if operation_class == "read":
        return "read"
    if any(
        token in lower
        for token in ("delete", "remove", "suspend", "reset", "cancel", "deactivate")
    ):
        return "delete"
    if operation_class in {"write_safe", "write_risky"}:
        return "write"
    if "execute" in lower or "run" in lower:
        return "execute"
    return "unknown"


def _destination_class(*, tool: str, args: Dict[str, Any]) -> str:
    if str(args.get("visibility") or "").lower() in {"public", "external"}:
        return "public"
    if tool.startswith(("mail.", "gmail.")) and any(
        key in args for key in ("to", "cc", "bcc", "recipient", "recipients")
    ):
        recipients = _flatten_strings(
            args.get("to"),
            args.get("cc"),
            args.get("bcc"),
            args.get("recipient"),
            args.get("recipients"),
        )
        if any("@" in item and not item.endswith(".example") for item in recipients):
            return "external"
        return "customer" if recipients else "internal"
    if tool.startswith(("slack.", "teams.")) and args.get("channel"):
        return "internal"
    if tool.startswith(("browser.", "http.")):
        return "external"
    return "internal"


def _externality(*, tool: str, args: Dict[str, Any]) -> str:
    destination = _destination_class(tool=tool, args=args)
    if destination in {"external", "public", "customer"}:
        return destination
    return ""


def _sensitivity_tags(
    args: Dict[str, Any],
    policy_metadata: dict[str, Any] | None = None,
) -> list[str]:
    metadata = policy_metadata or {}
    values = _flatten_strings(
        args.get("sensitivity_tags"),
        args.get("policy_tags"),
        args.get("tags"),
        args.get("classification"),
        args.get("data_classification"),
        args.get("visibility"),
        metadata.get("sensitivity_tags"),
        metadata.get("policy_tags"),
        metadata.get("tags"),
        metadata.get("classification"),
        metadata.get("object_classification"),
    )
    seen: set[str] = set()
    tags: list[str] = []
    for value in values:
        tag = value.strip().lower().replace(" ", "_")
        if tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return tags[:12]


def _object_classification(
    args: Dict[str, Any],
    policy_metadata: dict[str, Any] | None = None,
) -> str:
    metadata = policy_metadata or {}
    for key in (
        "object_classification",
        "classification",
        "data_classification",
        "sensitivity",
    ):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    for key in (
        "object_classification",
        "classification",
        "data_classification",
        "sensitivity",
    ):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return ""


def _destructive_write(*, tool: str, operation_class: str) -> bool:
    if operation_class == "write_risky":
        return True
    lower = tool.lower()
    return any(
        token in lower
        for token in ("delete", "remove", "suspend", "reset", "cancel", "deactivate")
    )


def _approval_required(*, tool: str, operation_class: str) -> bool:
    return operation_class == "write_risky" or _destructive_write(
        tool=tool,
        operation_class=operation_class,
    )


def _tenant_boundary(*, args: Dict[str, Any], principal: ExecutionPrincipal) -> str:
    target_tenant = str(args.get("tenant_id") or args.get("target_tenant_id") or "")
    if not target_tenant or not principal.tenant_id:
        return "unknown"
    return "same_tenant" if target_tenant == principal.tenant_id else "cross_tenant"


def _flatten_strings(*values: Any) -> list[str]:
    flattened: list[str] = []
    for value in values:
        if value is None or value == "":
            continue
        if isinstance(value, str):
            flattened.extend(item.strip() for item in value.split(",") if item.strip())
        elif isinstance(value, (list, tuple, set)):
            flattened.extend(str(item).strip() for item in value if str(item).strip())
    return flattened


class RouterDispatch:
    GUARDED_PREFIXES = GUARDED_PREFIXES

    @staticmethod
    def build_dispatch_table(router: Router) -> Dict[str, Any]:
        return build_dispatch_table(router)

    @staticmethod
    def deliver_plugin_event(
        router: Router,
        target: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        for entry in router.facade_plugins.values():
            plugin = entry.plugin
            if target not in plugin.event_targets:
                continue
            component = entry.component
            if plugin.event_handler is not None:
                return plugin.event_handler(router, component, payload)
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload args must be an object",
                )
            result = RouterDispatch.execute(
                router,
                tool,
                args,
                principal=_principal_from_payload(payload),
                request_metadata=_request_metadata_from_payload(payload),
            )
            return {"tool": tool, "result": router._jsonable(result)}
        return None

    @staticmethod
    def deliver_event(
        router: Router, target: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        if target == "slack":
            return router.slack.deliver(payload)
        if target == "mail":
            return router.mail.deliver(payload)
        if target == "docs":
            return router.docs.deliver(payload)
        if target == "calendar":
            return router.calendar.deliver(payload)
        if target == "tickets":
            return router.tickets.deliver(payload)
        if target in {"db", "database"}:
            return router.database.deliver(payload)
        if target in {
            "erp",
            "crm",
            "servicedesk",
            "okta",
            "google_admin",
            "siem",
            "datadog",
            "pagerduty",
            "feature_flags",
            "hris",
            "jira",
            "tool",
        }:
            tool = payload.get("tool")
            args = payload.get("args", {})
            if not isinstance(tool, str):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload must include string 'tool'",
                )
            if not isinstance(args, dict):
                raise MCPError(
                    "invalid_event",
                    f"{target} event payload args must be an object",
                )
            result = RouterDispatch.execute(
                router,
                tool,
                args,
                principal=_principal_from_payload(payload),
                request_metadata=_request_metadata_from_payload(payload),
            )
            return {"tool": tool, "result": router._jsonable(result)}
        plugin_delivery = RouterDispatch.deliver_plugin_event(router, target, payload)
        if plugin_delivery is not None:
            return plugin_delivery
        return {"ignored": True, "reason": f"unsupported target '{target}'"}

    @staticmethod
    def execute(
        router: Router,
        tool: str,
        args: Dict[str, Any],
        *,
        principal: ExecutionPrincipal | dict[str, Any] | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> Any:
        ts_ms = int(getattr(router.bus, "clock_ms", 0))
        source_id = (
            f"router:{getattr(router.state_store, 'branch', 'main')}:{ts_ms}:{tool}"
        )
        resolved_principal = _resolve_execution_principal(
            principal,
            request_metadata=request_metadata,
        )
        actor_ref = _principal_actor_ref(resolved_principal)
        policy_metadata = _tool_policy_metadata(
            router=router,
            tool=tool,
            args=args,
            principal=resolved_principal,
            request_metadata=request_metadata,
        )
        context = resolved_principal.to_event_context(
            workspace_id=str(
                getattr(getattr(router, "event_sink", None), "workspace", "")
            )
            or resolved_principal.workspace_id,
            run_id=str(getattr(router.state_store, "branch", "")),
            trace_id=source_id,
            span_id=stable_event_id(source_id, "requested"),
            source_id=source_id,
            source_granularity="per_call",
        )
        requested_event = router._emit_canonical_event(
            build_tool_call_event(
                kind="tool.call.requested",
                event_id=stable_event_id(source_id, "requested"),
                ts_ms=ts_ms,
                tool_name=tool,
                actor_ref=actor_ref,
                object_refs=extract_object_refs(tool_name=tool, args=args),
                args=args,
                status="requested",
                source_id=source_id,
                context=context,
                policy_metadata=policy_metadata,
            )
        )
        if tool == "vei.observe":
            focus = args.get("focus") if isinstance(args, dict) else None
            result = router.observe(focus_hint=focus).model_dump()
            router._emit_canonical_event(
                build_tool_call_event(
                    kind="tool.call.completed",
                    event_id=stable_event_id(source_id, "completed"),
                    ts_ms=int(getattr(router.bus, "clock_ms", ts_ms)),
                    tool_name=tool,
                    actor_ref=actor_ref,
                    object_refs=extract_object_refs(
                        tool_name=tool, args=args, response=result
                    ),
                    args=args,
                    response=result,
                    status="completed",
                    source_id=source_id,
                    policy_metadata=policy_metadata,
                    link_refs=[requested_event.event_id],
                    links=[
                        {
                            "kind": "completed_by",
                            "event_id": requested_event.event_id,
                        }
                    ],
                    context=context.model_copy(
                        update={
                            "span_id": stable_event_id(source_id, "completed"),
                            "parent_event_id": requested_event.event_id,
                        }
                    ),
                )
            )
            return result
        try:
            result = RouterDispatch._execute_inner(router, tool, args)
        except Exception as exc:
            router._emit_canonical_event(
                build_tool_call_event(
                    kind="tool.call.failed",
                    event_id=stable_event_id(source_id, "failed", type(exc).__name__),
                    ts_ms=int(getattr(router.bus, "clock_ms", ts_ms)),
                    tool_name=tool,
                    actor_ref=actor_ref,
                    object_refs=extract_object_refs(tool_name=tool, args=args),
                    args=args,
                    status="failed",
                    error=str(exc) or type(exc).__name__,
                    source_id=source_id,
                    error_class=classify_tool_call_failure(exc),
                    policy_metadata=policy_metadata,
                    link_refs=[requested_event.event_id],
                    links=[{"kind": "failed_by", "event_id": requested_event.event_id}],
                    context=context.model_copy(
                        update={
                            "span_id": stable_event_id(
                                source_id, "failed", type(exc).__name__
                            ),
                            "parent_event_id": requested_event.event_id,
                        }
                    ),
                )
            )
            raise
        router._emit_canonical_event(
            build_tool_call_event(
                kind="tool.call.completed",
                event_id=stable_event_id(source_id, "completed"),
                ts_ms=int(getattr(router.bus, "clock_ms", ts_ms)),
                tool_name=tool,
                actor_ref=actor_ref,
                object_refs=extract_object_refs(
                    tool_name=tool, args=args, response=result
                ),
                args=args,
                response=result,
                status="completed",
                source_id=source_id,
                policy_metadata=policy_metadata,
                link_refs=[requested_event.event_id],
                links=[{"kind": "completed_by", "event_id": requested_event.event_id}],
                context=context.model_copy(
                    update={
                        "span_id": stable_event_id(source_id, "completed"),
                        "parent_event_id": requested_event.event_id,
                    }
                ),
            )
        )
        return result

    @staticmethod
    def _execute_inner(router: Router, tool: str, args: Dict[str, Any]) -> Any:
        if tool == "vei.tick":
            return router.tick(**args)
        if tool == "vei.state":
            return router.state_snapshot(**args)
        if tool == "vei.act_and_observe":
            target_tool = args.get("tool")
            target_args = args.get("args", {})
            if not target_tool:
                raise MCPError("invalid_args", "act_and_observe requires tool")
            return router.act_and_observe(target_tool, target_args)
        if tool == "vei.inject":
            return router.inject(**args)

        if not tool.startswith("vei."):
            router._maybe_fault(tool)
        tool = router.alias_map.get(tool, tool)
        intercepted = router._maybe_fidelity_intercept(tool, args)
        if intercepted is not None:
            return intercepted
        if router.connector_runtime.managed_tool(tool):
            try:
                return router.connector_runtime.invoke_tool(
                    tool,
                    args,
                    time_ms=router.bus.clock_ms,
                    metadata={"router_branch": router.state_store.branch},
                )
            except ConnectorInvocationError as exc:
                raise MCPError(exc.code, exc.message) from exc

        handler = router._dispatch.get(tool)
        if handler is not None:
            return handler(args)

        for prefix, label in RouterDispatch.GUARDED_PREFIXES.items():
            if tool.startswith(prefix):
                if not getattr(router, prefix.rstrip("."), None):
                    raise MCPError("unsupported_tool", f"{label} twin not available")
                raise MCPError("unknown_tool", f"No such tool: {tool}")

        for provider in router.tool_providers:
            if provider.handles(tool):
                return provider.call(tool, args)

        raise MCPError("unknown_tool", f"No such tool: {tool}")
