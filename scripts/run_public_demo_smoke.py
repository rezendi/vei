from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


@dataclass(frozen=True)
class DemoSpec:
    path: str
    title: str
    bundle_key: str


DEMOS = (
    DemoSpec("/enron/", "Enron World Model", "actors"),
    DemoSpec("/public-history/", "VEI Public History", "source"),
)


def _read_url(url: str, *, max_bytes: int | None = None) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"public demo smoke only fetches https URLs: {url}")
    if not parsed.hostname:
        raise ValueError(f"public demo smoke URL is missing a host: {url}")
    response = requests.get(
        url,
        headers={"User-Agent": "VEI public demo smoke"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.content
    return payload if max_bytes is None else payload[:max_bytes]


def _extract_assets(html: str) -> list[str]:
    assets: list[str] = []
    for match in re.finditer(r"""(?:src|href)=["']([^"']+)["']""", html):
        asset = match.group(1)
        if asset.startswith(("data:", "http://", "https://")):
            continue
        assets.append(asset)
    return assets


def _json_payload(url: str) -> dict[str, Any]:
    payload = json.loads(_read_url(url).decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{url} did not return a JSON object")
    return payload


def _check_demo(base_url: str, spec: DemoSpec) -> None:
    page_url = urljoin(base_url, spec.path)
    html = _read_url(page_url).decode("utf-8", errors="replace")
    if spec.title not in html:
        raise RuntimeError(f"{page_url} is missing title {spec.title!r}")

    assets = _extract_assets(html)
    app_assets = [asset for asset in assets if asset.endswith(".js") or ".js?" in asset]
    if not app_assets:
        raise RuntimeError(f"{page_url} did not reference an app.js asset")
    for asset in app_assets:
        _read_url(urljoin(base_url, asset), max_bytes=256)

    bundle_url = urljoin(base_url, f"{spec.path}bundle.json")
    bundle = _json_payload(bundle_url)
    if spec.bundle_key not in bundle:
        raise RuntimeError(f"{bundle_url} missing expected key {spec.bundle_key!r}")

    model_url = urljoin(base_url, f"{spec.path}jepa_model.onnx")
    model_prefix = _read_url(model_url, max_bytes=128)
    if len(model_prefix) < 32:
        raise RuntimeError(f"{model_url} did not return a non-empty ONNX payload")
    print(f"ok {spec.path} bundle_keys={len(bundle)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke-check strangelab.ai Enron and Public History static demos."
    )
    parser.add_argument("--base-url", default="https://strangelab.ai")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/") + "/"
    for spec in DEMOS:
        _check_demo(base_url, spec)


if __name__ == "__main__":
    main()
