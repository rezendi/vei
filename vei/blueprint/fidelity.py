"""Surface fidelity resolution for progressive disclosure.

Given a tool call and the blueprint's surface_fidelity config, determines
whether to:
- L1: return a static/canned response
- L2: use a simple key-value store (CRUD without cross-system causality)
- L3: delegate to the full router (default, no interception)
"""

from __future__ import annotations

from typing import Any

from vei.blueprint.models import BlueprintAsset, SurfaceFidelitySpec

_SURFACE_FROM_TOOL_PREFIX = {
    "slack": "slack",
    "mail": "mail",
    "jira": "tickets",
    "tickets": "tickets",
    "browser": "docs",
    "docs": "docs",
    "okta": "identity",
    "crm": "crm",
    "salesforce": "crm",
    "erp": "erp",
    "calendar": "calendar",
    "spreadsheet": "spreadsheet",
    "siem": "siem",
    "pagerduty": "pagerduty",
    "hris": "hris",
    "servicedesk": "servicedesk",
    "feature_flags": "feature_flags",
    "datadog": "datadog",
}


def resolve_surface(tool_name: str) -> str:
    """Map a tool name like 'slack.send_message' to a surface key."""
    prefix = tool_name.split(".")[0].lower()
    return _SURFACE_FROM_TOOL_PREFIX.get(prefix, prefix)


def get_fidelity(
    asset: BlueprintAsset,
    tool_name: str,
) -> SurfaceFidelitySpec:
    """Get the fidelity spec for a tool's surface."""
    surface = resolve_surface(tool_name)
    return asset.surface_fidelity.get(
        surface,
        SurfaceFidelitySpec(level="L3"),
    )


def should_intercept(asset: BlueprintAsset, tool_name: str) -> bool:
    """Return True if the tool call should be intercepted (L1 or L2)."""
    spec = get_fidelity(asset, tool_name)
    return spec.level in ("L1", "L2")


def l1_response(spec: SurfaceFidelitySpec, tool_name: str) -> dict[str, Any]:
    """Generate a static L1 response for a tool call."""
    if tool_name in spec.static_responses:
        return dict(spec.static_responses[tool_name])
    action = tool_name.split(".")[-1] if "." in tool_name else tool_name
    return {
        "status": "ok",
        "tool": tool_name,
        "fidelity": "L1",
        "message": f"Static response for {action}",
    }


class L2Store:
    """Simple key-value store for L2 (stateful but no cross-system causality).

    Each surface gets its own namespace. Supports basic CRUD patterns.
    """

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}

    def get(self, surface: str, key: str) -> Any | None:
        return self._data.get(surface, {}).get(key)

    def put(self, surface: str, key: str, value: Any) -> None:
        if surface not in self._data:
            self._data[surface] = {}
        self._data[surface][key] = value

    def delete(self, surface: str, key: str) -> bool:
        if surface in self._data and key in self._data[surface]:
            del self._data[surface][key]
            return True
        return False

    def list_keys(self, surface: str) -> list[str]:
        return list(self._data.get(surface, {}).keys())

    def handle(
        self,
        surface: str,
        tool_name: str,
        args: dict[str, Any],
    ) -> dict[str, Any]:
        """Route a tool call through the L2 KV store."""
        action = tool_name.split(".")[-1] if "." in tool_name else tool_name
        key = args.get("id") or args.get("key") or args.get("name") or action

        if "list" in action or "search" in action:
            items = [
                {"key": k, **v} if isinstance(v, dict) else {"key": k, "value": v}
                for k, v in self._data.get(surface, {}).items()
            ]
            return {"status": "ok", "fidelity": "L2", "items": items}

        if "get" in action or "read" in action or "open" in action:
            value = self.get(surface, str(key))
            if value is not None:
                return (
                    {"status": "ok", "fidelity": "L2", **value}
                    if isinstance(value, dict)
                    else {"status": "ok", "fidelity": "L2", "value": value}
                )
            return {"status": "not_found", "fidelity": "L2", "key": str(key)}

        if "delete" in action or "remove" in action:
            deleted = self.delete(surface, str(key))
            return {"status": "ok" if deleted else "not_found", "fidelity": "L2"}

        self.put(surface, str(key), args)
        return {"status": "ok", "fidelity": "L2", "key": str(key)}
