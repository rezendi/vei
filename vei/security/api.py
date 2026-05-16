from __future__ import annotations

from typing import Any
from urllib.parse import urlparse
import urllib.request

ALLOWED_URL_SCHEMES = frozenset({"http", "https"})


def safe_urlopen(
    target: str | urllib.request.Request,
    *,
    timeout: float | None = None,
) -> Any:
    _validate_http_target(target)
    return urllib.request.urlopen(  # nosec B310  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        target,
        timeout=timeout,
    )


def _validate_http_target(target: str | urllib.request.Request) -> None:
    url = target.full_url if isinstance(target, urllib.request.Request) else target
    parsed = urlparse(str(url))
    scheme = parsed.scheme.lower()
    if scheme not in ALLOWED_URL_SCHEMES:
        raise ValueError(f"URL scheme must be http or https, got {scheme or '<empty>'}")
    if not parsed.netloc:
        raise ValueError("URL must include a network location")
