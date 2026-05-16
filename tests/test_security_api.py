from __future__ import annotations

import urllib.request

import pytest

from vei.security.api import safe_urlopen


def test_safe_urlopen_rejects_file_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError, match="http or https"):
        safe_urlopen("file:///tmp/source.json")

    assert calls == []


def test_safe_urlopen_requires_network_location() -> None:
    with pytest.raises(ValueError, match="network location"):
        safe_urlopen("https:///missing-host")


def test_safe_urlopen_allows_https_request(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}

    def fake_urlopen(
        target: urllib.request.Request,
        *,
        timeout: float | None = None,
    ) -> str:
        recorded["target"] = target
        recorded["timeout"] = timeout
        return "ok"

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    request = urllib.request.Request("https://example.com/path")
    assert safe_urlopen(request, timeout=2.5) == "ok"
    assert recorded == {"target": request, "timeout": 2.5}
